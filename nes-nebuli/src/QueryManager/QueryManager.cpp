/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <QueryManager/QueryManager.hpp>

#include <chrono>
#include <cmath>
#include <exception>
#include <optional>
#include <ranges>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Util/Logger/Logger.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>

#include <fmt/chrono.h>

#include <QueryManager/FileQueryPlanStore.h>

namespace NES
{
namespace
{
DistributedQueryId uniqueDistributedQueryId(const QueryManagerState& state)
{
    auto uniqueId = getNextDistributedQueryId();
    size_t counter = 0;
    while (state.queries.contains(uniqueId))
    {
        uniqueId = DistributedQueryId(getNextDistributedQueryId().getRawValue()
                                      + std::to_string(counter++));
    }
    return uniqueId;
}
} // namespace

/* ============================
 * QueryManagerBackends
 * ============================ */

std::unordered_map<GrpcAddr, UniquePtr<QuerySubmissionBackend>>
QueryManager::QueryManagerBackends::createBackends(
    const std::vector<WorkerConfig>& workers,
    BackendProvider& provider)
{
    std::unordered_map<GrpcAddr, UniquePtr<QuerySubmissionBackend>> result;
    for (const auto& workerConfig : workers)
    {
        result.emplace(workerConfig.grpc, provider(workerConfig));
    }
    return result;
}

QueryManager::QueryManagerBackends::QueryManagerBackends(
    SharedPtr<WorkerCatalog> workerCatalog,
    BackendProvider provider)
    : workerCatalog(std::move(workerCatalog)),
      backendProvider(std::move(provider))
{
    rebuildBackendsIfNeeded();
}

void QueryManager::QueryManagerBackends::rebuildBackendsIfNeeded() const
{
    const auto currentVersion = workerCatalog->getVersion();
    if (currentVersion != cachedWorkerCatalogVersion)
    {
        NES_DEBUG(
            "WorkerCatalog version changed from {} to {}, rebuilding backends",
            cachedWorkerCatalogVersion,
            currentVersion);
        backends = createBackends(workerCatalog->getAllWorkers(), backendProvider);
        cachedWorkerCatalogVersion = currentVersion;
    }
}

/* ============================
 * QueryManager
 * ============================ */

QueryManager::QueryManager(
    SharedPtr<WorkerCatalog> workerCatalog,
    BackendProvider provider,
    QueryManagerState state)
    : state(std::move(state)),
      backends(std::move(workerCatalog), std::move(provider)),
      planStore(std::make_unique<FileQueryPlanStore>("/tmp/nes-worker-store"))
{
}

QueryManager::QueryManager(
    SharedPtr<WorkerCatalog> workerCatalog,
    BackendProvider provider)
    : backends(std::move(workerCatalog), std::move(provider)),
      planStore(std::make_unique<FileQueryPlanStore>("/tmp/nes-worker-store"))
{
    // === Recovery path (Option B) ===
    // Load persisted LogicalPlans and rebuild DistributedLogicalPlans.
    // Without a planner/distributor, we conservatively assign the plan
    // to all available workers.

    auto storedPlans = planStore->loadAll();
    for (const auto& [id, logicalPlan] : storedPlans)
    {
        std::unordered_map<GrpcAddr, std::vector<LogicalPlan>> localPlans;

        for (const auto& [grpcAddr, _backend] : backends)
        {
            localPlans[grpcAddr].push_back(logicalPlan);
        }

        DecomposedLogicalPlan<GrpcAddr> decomposed{std::move(localPlans)};
        DistributedLogicalPlan dplan{std::move(decomposed), logicalPlan};
        dplan.setQueryId(id);

        (void)registerQuery(dplan);
    }
}

std::expected<DistributedQueryId, Exception>
QueryManager::registerQuery(const DistributedLogicalPlan& plan)
{
    std::unordered_map<GrpcAddr, std::vector<LocalQueryId>> localQueries;

    auto id = plan.getQueryId();
    if (id == DistributedQueryId(DistributedQueryId::INVALID))
    {
        id = uniqueDistributedQueryId(state);
    }
    else if (state.queries.contains(id))
    {
        throw QueryAlreadyRegistered("{}", id);
    }

    // Persist intent (LogicalPlan), NOT derived distributed state
    if (planStore)
    {
        planStore->persist(id, plan.getGlobalPlan());
    }

    for (const auto& [grpcAddr, localPlans] : plan)
    {
        INVARIANT(
            backends.contains(grpcAddr),
            "Plan assigned to unknown worker {}",
            grpcAddr);

        for (const auto& localPlan : localPlans)
        {
            const auto result = backends.at(grpcAddr).registerQuery(localPlan);
            if (!result)
            {
                return std::unexpected{result.error()};
            }
            localQueries[grpcAddr].push_back(*result);
        }
    }

    state.queries.emplace(id, DistributedQuery{std::move(localQueries)});
    return id;
}

std::expected<void, std::vector<Exception>>
QueryManager::start(DistributedQueryId queryId)
{
    auto queryResult = getQuery(queryId);
    if (!queryResult)
    {
        return std::unexpected(std::vector{queryResult.error()});
    }

    std::vector<Exception> errors;
    for (const auto& [grpcAddr, localQueryId] : queryResult->iterate())
    {
        const auto result = backends.at(grpcAddr).start(localQueryId);
        if (!result)
        {
            errors.push_back(result.error());
        }
    }

    if (!errors.empty())
    {
        return std::unexpected(errors);
    }
    return {};
}

std::expected<DistributedQueryStatus, std::vector<Exception>>
QueryManager::status(const DistributedQueryId& queryId) const
{
    auto queryResult = getQuery(queryId);
    if (!queryResult)
    {
        return std::unexpected(std::vector{queryResult.error()});
    }

    std::unordered_map<
        GrpcAddr,
        std::unordered_map<LocalQueryId, std::expected<LocalQueryStatus, Exception>>>
        localStatusResults;

    for (const auto& [grpcAddr, localQueryId] : queryResult->iterate())
    {
        localStatusResults[grpcAddr][localQueryId] =
            backends.at(grpcAddr).status(localQueryId);
    }

    return DistributedQueryStatus{
        .localStatusSnapshots = std::move(localStatusResults),
        .queryId = queryId};
}

std::vector<DistributedQueryId> QueryManager::queries() const
{
    return state.queries | std::views::keys | std::ranges::to<std::vector>();
}

std::expected<DistributedWorkerStatus, Exception>
QueryManager::workerStatus(std::chrono::system_clock::time_point after) const
{
    DistributedWorkerStatus status;
    for (const auto& [grpcAddr, backend] : backends)
    {
        status.workerStatus.emplace(grpcAddr, backend->workerStatus(after));
    }
    return status;
}

std::expected<void, std::vector<Exception>>
QueryManager::stop(DistributedQueryId queryId)
{
    auto queryResult = getQuery(queryId);
    if (!queryResult)
    {
        return std::unexpected(std::vector{queryResult.error()});
    }

    std::vector<Exception> errors;
    for (const auto& [grpcAddr, localQueryId] : queryResult->iterate())
    {
        const auto result = backends.at(grpcAddr).stop(localQueryId);
        if (!result)
        {
            errors.push_back(result.error());
        }
    }

    if (!errors.empty())
    {
        return std::unexpected(errors);
    }
    return {};
}

std::expected<void, std::vector<Exception>>
QueryManager::unregister(DistributedQueryId queryId)
{
    auto queryResult = getQuery(queryId);
    if (!queryResult)
    {
        return std::unexpected(std::vector{queryResult.error()});
    }

    std::vector<Exception> errors;
    for (const auto& [grpcAddr, localQueryId] : queryResult->iterate())
    {
        const auto result = backends.at(grpcAddr).unregister(localQueryId);
        if (!result)
        {
            errors.push_back(result.error());
        }
    }

    if (!errors.empty())
    {
        return std::unexpected(errors);
    }

    state.queries.erase(queryId);

    if (planStore)
    {
        planStore->erase(queryId);
    }

    return {};
}

std::expected<DistributedQuery, Exception>
QueryManager::getQuery(DistributedQueryId query) const
{
    auto it = state.queries.find(query);
    if (it == state.queries.end())
    {
        return std::unexpected(Exception("Query not found", 0));
    }
    return it->second;
}

std::vector<DistributedQueryId>
QueryManager::getRunningQueries() const
{
    return state.queries | std::views::keys | std::ranges::to<std::vector>();
}

} // namespace NES
