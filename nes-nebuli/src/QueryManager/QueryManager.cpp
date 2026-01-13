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
#include <QueryManager/EtcdQueryStore.hpp>

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

std::vector<WorkerConfig>
QueryManager::QueryManagerBackends::getAllWorkers() const
{
    rebuildBackendsIfNeeded();
    return workerCatalog->getAllWorkers();
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
 * QueryManager - Constructors
 * ============================ */

QueryManager::QueryManager(
    SharedPtr<WorkerCatalog> workerCatalog,
    BackendProvider provider,
    QueryManagerState state)
    : state(std::move(state)),
      backends(std::move(workerCatalog), std::move(provider)),
      config{.useEtcd = false}
{
    NES_INFO("QueryManager initialized with GRPC backends (push mode)");
}

QueryManager::QueryManager(
    SharedPtr<WorkerCatalog> workerCatalog,
    BackendProvider provider)
    : backends(std::move(workerCatalog), std::move(provider)),
      planStore(std::make_unique<FileQueryPlanStore>("/tmp/nes-worker-store")),
      config{.useEtcd = false}
{
    NES_INFO("QueryManager initialized with GRPC backends and file-based plan store");
    
    // Recovery from file store (legacy behavior)
    auto storedPlans = planStore->loadAll();
    for (const auto& [id, logicalPlan] : storedPlans)
    {
        std::unordered_map<GrpcAddr, std::vector<LogicalPlan>> localPlans;
        const auto workers = backends.getAllWorkers();

        for (const auto& w : workers)
        {
            localPlans[w.grpc].push_back(logicalPlan);
        }

        DecomposedLogicalPlan<GrpcAddr> decomposed{std::move(localPlans)};
        DistributedLogicalPlan dplan{std::move(decomposed), logicalPlan};
        dplan.setQueryId(id);

        (void)registerQuery(dplan);
    }
}

QueryManager::QueryManager(
    SharedPtr<WorkerCatalog> workerCatalog,
    BackendProvider provider,
    QueryManagerConfiguration config)
    : backends(std::move(workerCatalog), std::move(provider)),
      config(std::move(config))
{
    if (this->config.useEtcd)
    {
        NES_INFO("QueryManager initialized with etcd store (pull mode) at {}",
                 this->config.etcdConfig.endpoints);
        
        etcdStore = std::make_unique<EtcdQueryStore>(this->config.etcdConfig);
        
        if (!etcdStore->isConnected())
        {
            NES_WARNING("QueryManager: etcd connection check failed, queries may not be persisted");
        }
        
        // In pull mode, workers fetch their own state from etcd.
        // We only need to load query IDs for local state tracking.
        auto queryIdsResult = etcdStore->getAllQueryIds();
        if (queryIdsResult)
        {
            NES_INFO("QueryManager: found {} existing queries in etcd", queryIdsResult->size());
            for (const auto& queryId : *queryIdsResult)
            {
                // We don't have the full DistributedQuery info without querying each worker,
                // but we track the query ID for status queries
                state.queries.emplace(queryId, DistributedQuery{});
            }
        }
        else
        {
            NES_WARNING("QueryManager: failed to load queries from etcd: {}",
                        queryIdsResult.error().what());
        }
    }
    else
    {
        NES_INFO("QueryManager initialized with GRPC backends (push mode)");
    }
}

/* ============================
 * QueryManager - Registration
 * ============================ */

std::expected<DistributedQueryId, Exception>
QueryManager::registerQuery(const DistributedLogicalPlan& plan)
{
    if (config.useEtcd)
    {
        return registerQueryViaEtcd(plan);
    }
    return registerQueryViaGrpc(plan);
}

std::expected<DistributedQueryId, Exception>
QueryManager::registerQueryViaEtcd(const DistributedLogicalPlan& plan)
{
    auto id = plan.getQueryId();
    if (id == DistributedQueryId(DistributedQueryId::INVALID))
    {
        id = uniqueDistributedQueryId(state);
    }
    else if (state.queries.contains(id))
    {
        return std::unexpected(QueryAlreadyRegistered("{}", id));
    }

    NES_INFO("QueryManager: registering query {} via etcd", id);

    // Create a mutable copy to set the query ID
    DistributedLogicalPlan mutablePlan = plan;
    mutablePlan.setQueryId(id);

    // Persist to etcd - workers will poll and pick up the plan
    auto persistResult = etcdStore->persistQuery(id, mutablePlan);
    if (!persistResult)
    {
        return std::unexpected(persistResult.error());
    }

    // Track locally (for status queries)
    // Note: In pull mode, we don't have LocalQueryIds until workers report back
    state.queries.emplace(id, DistributedQuery{});

    NES_INFO("QueryManager: query {} persisted to etcd, workers will poll for it", id);
    return id;
}

std::expected<DistributedQueryId, Exception>
QueryManager::registerQueryViaGrpc(const DistributedLogicalPlan& plan)
{
    std::unordered_map<GrpcAddr, std::vector<LocalQueryId>> localQueries;

    auto id = plan.getQueryId();
    if (id == DistributedQueryId(DistributedQueryId::INVALID))
    {
        id = uniqueDistributedQueryId(state);
    }
    else if (state.queries.contains(id))
    {
        return std::unexpected(QueryAlreadyRegistered("{}", id));
    }

    // Persist intent (LogicalPlan) if planStore is configured
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

/* ============================
 * QueryManager - Start/Stop
 * ============================ */

std::expected<void, std::vector<Exception>>
QueryManager::start(DistributedQueryId queryId)
{
    if (config.useEtcd)
    {
        // In pull mode, workers automatically start queries when they poll etcd
        // This is a no-op, but we validate the query exists
        if (!state.queries.contains(queryId))
        {
            return std::unexpected(std::vector{QueryNotFound("{}", queryId)});
        }
        NES_INFO("QueryManager: start() called for query {} (pull mode - workers auto-start)", queryId);
        return {};
    }

    // GRPC mode - actively push start command
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

std::expected<void, std::vector<Exception>>
QueryManager::stop(DistributedQueryId queryId)
{
    if (config.useEtcd)
    {
        // In pull mode, stopping = removing from etcd
        // Workers will stop the query on their next poll
        NES_INFO("QueryManager: stop() for query {} - will remove from etcd", queryId);
        // Don't actually remove yet - that's done in unregister
        // For now, we'd need a "stopped" state in etcd, but for minimal impl,
        // stop and unregister are combined
        return {};
    }

    // GRPC mode
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

/* ============================
 * QueryManager - Unregister
 * ============================ */

std::expected<void, std::vector<Exception>>
QueryManager::unregister(DistributedQueryId queryId)
{
    if (config.useEtcd)
    {
        return unregisterViaEtcd(queryId);
    }
    return unregisterViaGrpc(queryId);
}

std::expected<void, std::vector<Exception>>
QueryManager::unregisterViaEtcd(DistributedQueryId queryId)
{
    if (!state.queries.contains(queryId))
    {
        return std::unexpected(std::vector{QueryNotFound("{}", queryId)});
    }

    NES_INFO("QueryManager: unregistering query {} from etcd", queryId);

    auto eraseResult = etcdStore->eraseQuery(queryId);
    if (!eraseResult)
    {
        return std::unexpected(std::vector{eraseResult.error()});
    }

    state.queries.erase(queryId);
    NES_INFO("QueryManager: query {} removed from etcd, workers will stop on next poll", queryId);
    return {};
}

std::expected<void, std::vector<Exception>>
QueryManager::unregisterViaGrpc(DistributedQueryId queryId)
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

/* ============================
 * QueryManager - Status
 * ============================ */

std::expected<DistributedQueryStatus, std::vector<Exception>>
QueryManager::status(const DistributedQueryId& queryId) const
{
    if (config.useEtcd)
    {
        // In pull mode, we don't have direct access to worker status
        // We'd need workers to report status back or query them separately
        // For minimal implementation, return a basic status
        if (!state.queries.contains(queryId))
        {
            return std::unexpected(std::vector{QueryNotFound("{}", queryId)});
        }
        
        // Return empty status - full status requires worker integration
        return DistributedQueryStatus{
            .localStatusSnapshots = {},
            .queryId = queryId
        };
    }

    // GRPC mode - query each worker
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
    if (config.useEtcd && etcdStore)
    {
        // Get live list from etcd
        auto result = etcdStore->getAllQueryIds();
        if (result)
        {
            return *result;
        }
        NES_WARNING("QueryManager: failed to fetch query list from etcd, using cached state");
    }
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

std::expected<DistributedQuery, Exception>
QueryManager::getQuery(DistributedQueryId query) const
{
    auto it = state.queries.find(query);
    if (it == state.queries.end())
    {
        return std::unexpected(QueryNotFound("{}", query));
    }
    return it->second;
}

std::vector<DistributedQueryId>
QueryManager::getRunningQueries() const
{
    return state.queries | std::views::keys | std::ranges::to<std::vector>();
}

} // namespace NES