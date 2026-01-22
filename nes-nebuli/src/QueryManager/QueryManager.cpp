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
 * QueryManager - Constructor
 * ============================ */

QueryManager::QueryManager(
    SharedPtr<WorkerCatalog> workerCatalog,
    BackendProvider provider,
    QueryManagerConfiguration config)
    : backends(std::move(workerCatalog), std::move(provider)),
      config(std::move(config))
{
    NES_INFO("QueryManager: initializing with etcd at {}",
             this->config.etcdConfig.endpoints);
    
    etcdStore = std::make_unique<EtcdQueryStore>(this->config.etcdConfig);
    
    if (!etcdStore->isConnected())
    {
        NES_WARNING("QueryManager: etcd connection check failed");
    }
    
    // Load existing query IDs from etcd
    auto queryIdsResult = etcdStore->getAllQueryIds();
    if (queryIdsResult)
    {
        NES_INFO("QueryManager: found {} existing queries in etcd", queryIdsResult->size());
        for (const auto& queryId : *queryIdsResult)
        {
            state.queries.emplace(queryId, DistributedQuery{});
        }
    }
    else
    {
        NES_WARNING("QueryManager: failed to load queries from etcd: {}",
                    queryIdsResult.error().what());
    }
}

QueryManager::QueryManager(
    SharedPtr<WorkerCatalog> workerCatalog,
    BackendProvider provider,
    QueryManagerState initialState)
    : state(std::move(initialState)),
      backends(std::move(workerCatalog), std::move(provider)),
      config{}
{
    NES_INFO("QueryManager: initializing with etcd at {} (with initial state)",
             this->config.etcdConfig.endpoints);
    
    etcdStore = std::make_unique<EtcdQueryStore>(this->config.etcdConfig);
    
    if (!etcdStore->isConnected())
    {
        NES_WARNING("QueryManager: etcd connection check failed");
    }
}

/* ============================
 * QueryManager - Registration
 * ============================ */

std::expected<DistributedQueryId, Exception>
QueryManager::registerQuery(const DistributedLogicalPlan& plan)
{
    return persistQueryToEtcd(plan);
}

std::expected<DistributedQueryId, Exception>
QueryManager::persistQueryToEtcd(const DistributedLogicalPlan& plan)
{
    if (!etcdStore)
    {
        return std::unexpected(Exception("etcdStore not initialized", ErrorCode::UnknownException));
    }

    auto id = plan.getQueryId();
    if (id == DistributedQueryId(DistributedQueryId::INVALID))
    {
        id = uniqueDistributedQueryId(state);
    }
    else if (state.queries.contains(id))
    {
        return std::unexpected(QueryAlreadyRegistered("{}", id));
    }

    NES_INFO("QueryManager: registering query {} to etcd", id);

    // Create a mutable copy to set the query ID
    DistributedLogicalPlan mutablePlan = plan;
    mutablePlan.setQueryId(id);

    // Persist to etcd - workers will poll and pick up the plan
    auto persistResult = etcdStore->persistQuery(id, mutablePlan);
    if (!persistResult)
    {
        return std::unexpected(persistResult.error());
    }

    // Track locally
    state.queries.emplace(id, DistributedQuery{});

    NES_INFO("QueryManager: query {} persisted to etcd", id);
    return id;
}

/* ============================
 * QueryManager - Start/Stop
 * ============================ */

std::expected<void, std::vector<Exception>>
QueryManager::start(DistributedQueryId queryId)
{
    // In pull mode, workers automatically start queries when they poll etcd
    if (!state.queries.contains(queryId))
    {
        return std::unexpected(std::vector{QueryNotFound("{}", queryId)});
    }
    NES_INFO("QueryManager: start() for query {} (workers poll etcd)", queryId);
    return {};
}

std::expected<void, std::vector<Exception>>
QueryManager::stop(DistributedQueryId queryId)
{
    // In pull mode, stop = remove from etcd (done in unregister)
    if (!state.queries.contains(queryId))
    {
        return std::unexpected(std::vector{QueryNotFound("{}", queryId)});
    }
    NES_INFO("QueryManager: stop() for query {} (will remove on unregister)", queryId);
    return {};
}

/* ============================
 * QueryManager - Unregister
 * ============================ */

std::expected<void, std::vector<Exception>>
QueryManager::unregister(DistributedQueryId queryId)
{
    if (!state.queries.contains(queryId))
    {
        return std::unexpected(std::vector{QueryNotFound("{}", queryId)});
    }

    NES_INFO("QueryManager: unregistering query {} from etcd", queryId);

    if (etcdStore)
    {
        auto eraseResult = etcdStore->eraseQuery(queryId);
        if (!eraseResult)
        {
            return std::unexpected(std::vector{eraseResult.error()});
        }
    }

    state.queries.erase(queryId);
    NES_INFO("QueryManager: query {} removed from etcd", queryId);
    return {};
}

/* ============================
 * QueryManager - Status
 * ============================ */

std::expected<DistributedQueryStatus, std::vector<Exception>>
QueryManager::status(const DistributedQueryId& queryId) const
{
    if (!state.queries.contains(queryId))
    {
        return std::unexpected(std::vector{QueryNotFound("{}", queryId)});
    }
    
    // In pull mode, status requires worker integration (future work)
    return DistributedQueryStatus{
        .localStatusSnapshots = {},
        .queryId = queryId
    };
}

std::vector<DistributedQueryId> QueryManager::queries() const
{
    if (etcdStore)
    {
        auto result = etcdStore->getAllQueryIds();
        if (result)
        {
            return *result;
        }
        NES_WARNING("QueryManager: failed to fetch from etcd, using cache");
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