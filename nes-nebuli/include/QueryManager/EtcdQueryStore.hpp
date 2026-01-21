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

#pragma once

#include <expected>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <Plans/LogicalPlan.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <WorkerConfig.hpp>

namespace etcd {
class SyncClient;
}

namespace NES {

/// Configuration for connecting to etcd
struct EtcdConfiguration {
    std::string endpoints = "http://etcd:2379";
    std::string keyPrefix = "/nes/queries/";
};

/// Represents a query assignment for a specific worker
struct QueryAssignment {
    DistributedQueryId queryId;
    LogicalPlan plan;
};

/// EtcdQueryStore provides persistent storage for decomposed query plans.
/// 
/// Key structure in etcd:
///   /nes/queries/{queryId}/assignments/{workerAddr} -> Serialized LogicalPlan
///
/// This enables:
/// - Workers to poll for their assignments
/// - Fault tolerance through persistent state
/// - Decoupled submission and execution
class EtcdQueryStore {
public:
    explicit EtcdQueryStore(EtcdConfiguration config);
    ~EtcdQueryStore();

    // Non-copyable, movable
    EtcdQueryStore(const EtcdQueryStore&) = delete;
    EtcdQueryStore& operator=(const EtcdQueryStore&) = delete;
    EtcdQueryStore(EtcdQueryStore&&) noexcept;
    EtcdQueryStore& operator=(EtcdQueryStore&&) noexcept;

    /// Persist all worker assignments for a distributed query.
    /// Writes one key per worker with the serialized plan fragment.
    [[nodiscard]] std::expected<void, Exception> 
    persistQuery(const DistributedQueryId& queryId, 
                 const DistributedLogicalPlan& plan);

    /// Remove all assignments for a query (all workers).
    [[nodiscard]] std::expected<void, Exception> 
    eraseQuery(const DistributedQueryId& queryId);

    /// Get all query assignments for a specific worker.
    /// Workers call this during reconciliation to discover their work.
    [[nodiscard]] std::expected<std::vector<QueryAssignment>, Exception>
    getAssignmentsForWorker(const GrpcAddr& workerAddr);

    /// Get all currently stored query IDs.
    [[nodiscard]] std::expected<std::vector<DistributedQueryId>, Exception>
    getAllQueryIds();

    /// Check if etcd connection is healthy.
    [[nodiscard]] bool isConnected() const;

private:
    /// Build the etcd key for a worker assignment
    [[nodiscard]] std::string buildAssignmentKey(
        const DistributedQueryId& queryId, 
        const GrpcAddr& workerAddr) const;

    /// Build the prefix for all assignments of a query
    [[nodiscard]] std::string buildQueryPrefix(
        const DistributedQueryId& queryId) const;

    /// Extract query ID from an etcd key
    [[nodiscard]] std::expected<DistributedQueryId, Exception>
    extractQueryIdFromKey(const std::string& key) const;

    /// Extract worker address from an etcd key
    [[nodiscard]] std::expected<GrpcAddr, Exception>
    extractWorkerAddrFromKey(const std::string& key) const;

    EtcdConfiguration config;
    std::unique_ptr<etcd::SyncClient> client;
};

} // namespace NES