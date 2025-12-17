#pragma once

#include <chrono>
#include <cstdint>
#include <expected>
#include <unordered_map>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Pointers.hpp>
#include <absl/functional/any_invocable.h>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>
#include <WorkerStatus.hpp>

#include <QueryManager/QueryPlanStore.h> // interface ONLY

namespace NES {

/* ============================
 * QuerySubmissionBackend
 * ============================ */
class QuerySubmissionBackend
{
public:
    virtual ~QuerySubmissionBackend() = default;

    [[nodiscard]] virtual std::expected<LocalQueryId, Exception>
    registerQuery(LogicalPlan) = 0;

    virtual std::expected<void, Exception> start(LocalQueryId) = 0;
    virtual std::expected<void, Exception> stop(LocalQueryId) = 0;
    virtual std::expected<void, Exception> unregister(LocalQueryId) = 0;

    [[nodiscard]] virtual std::expected<LocalQueryStatus, Exception>
    status(LocalQueryId) const = 0;

    [[nodiscard]] virtual std::expected<WorkerStatus, Exception>
    workerStatus(std::chrono::system_clock::time_point after) const = 0;
};

using BackendProvider =
    absl::AnyInvocable<UniquePtr<QuerySubmissionBackend>(const WorkerConfig&)>;

/* ============================
 * QueryManagerState
 * ============================ */
struct QueryManagerState
{
    std::unordered_map<DistributedQueryId, DistributedQuery> queries;
};

/* ============================
 * QueryManager
 * ============================ */
class QueryManager
{
    /* ============================
     * QueryManagerBackends
     * ============================ */
    class QueryManagerBackends
    {
        SharedPtr<WorkerCatalog> workerCatalog;
        mutable BackendProvider backendProvider;

        mutable std::unordered_map<GrpcAddr, UniquePtr<QuerySubmissionBackend>> backends;
        mutable uint64_t cachedWorkerCatalogVersion = 0;

        static std::unordered_map<GrpcAddr, UniquePtr<QuerySubmissionBackend>>
        createBackends(const std::vector<WorkerConfig>& workers,
                       BackendProvider& provider);

        void rebuildBackendsIfNeeded() const;

    public:
        QueryManagerBackends(SharedPtr<WorkerCatalog> workerCatalog,
                             BackendProvider provider);

        auto begin() const
        {
            rebuildBackendsIfNeeded();
            return backends.begin();
        }

        auto end() const
        {
            return backends.end();
        }

        bool contains(const GrpcAddr& grpc) const
        {
            rebuildBackendsIfNeeded();
            return backends.contains(grpc);
        }

        const QuerySubmissionBackend& at(const GrpcAddr& grpc) const
        {
            rebuildBackendsIfNeeded();
            return *backends.at(grpc);
        }

        QuerySubmissionBackend& at(const GrpcAddr& grpc)
        {
            rebuildBackendsIfNeeded();
            return *backends.at(grpc);
        }
    };

private:
    QueryManagerState state;
    QueryManagerBackends backends;
    std::unique_ptr<QueryPlanStore> planStore;

public:
    QueryManager(SharedPtr<WorkerCatalog> workerCatalog,
                 BackendProvider provider,
                 QueryManagerState state);

    QueryManager(SharedPtr<WorkerCatalog> workerCatalog,
                 BackendProvider provider);

    [[nodiscard]] std::expected<DistributedQueryId, Exception>
    registerQuery(const DistributedLogicalPlan& plan);

    std::expected<void, std::vector<Exception>>
    start(DistributedQueryId query);

    std::expected<void, std::vector<Exception>>
    stop(DistributedQueryId query);

    std::expected<void, std::vector<Exception>>
    unregister(DistributedQueryId query);

    [[nodiscard]] std::expected<DistributedQueryStatus, std::vector<Exception>>
    status(const DistributedQueryId& query) const;

    [[nodiscard]] std::vector<DistributedQueryId>
    queries() const;

    [[nodiscard]] std::expected<DistributedWorkerStatus, Exception>
    workerStatus(std::chrono::system_clock::time_point after) const;

    [[nodiscard]] std::expected<DistributedQuery, Exception>
    getQuery(DistributedQueryId query) const;

    [[nodiscard]] std::vector<DistributedQueryId>
    getRunningQueries() const;
};

} // namespace NES
