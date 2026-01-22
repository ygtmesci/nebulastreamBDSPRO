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

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <WorkerConfig.hpp>

namespace etcd {
class SyncClient;
}

namespace NES {

/// Configuration for the Reconciler
struct ReconcilerConfiguration {
    /// etcd endpoint(s), comma-separated if multiple
    std::string etcdEndpoints = "http://etcd:2379";
    
    /// Key prefix for queries in etcd
    std::string keyPrefix = "/nes/queries/";
    
    /// How often to poll etcd for changes (milliseconds)
    std::chrono::milliseconds pollInterval{1000};
    
    /// This worker's address (used to find assignments)
    GrpcAddr workerAddress;
};

/// Interface for the worker's query execution engine
/// The Reconciler uses this to start/stop queries
class ReconcilerWorkerInterface {
public:
    virtual ~ReconcilerWorkerInterface() = default;
    
    /// Get IDs of all currently running queries
    [[nodiscard]] virtual std::unordered_set<DistributedQueryId> getRunningQueryIds() const = 0;
    
    /// Start a query from a LogicalPlan
    /// Returns the LocalQueryId if successful
    [[nodiscard]] virtual std::expected<LocalQueryId, Exception> 
    startQuery(const DistributedQueryId& queryId, const LogicalPlan& plan) = 0;
    
    /// Stop and remove a query
    [[nodiscard]] virtual std::expected<void, Exception>
    stopQuery(const DistributedQueryId& queryId) = 0;
};

/// Reconciler synchronizes worker's running queries with desired state in etcd.
///
/// The reconciliation loop:
/// 1. Polls etcd for assignments for this worker
/// 2. Compares with currently running queries
/// 3. Starts queries that should run but aren't
/// 4. Stops queries that are running but shouldn't be
///
/// This enables fault tolerance: if a worker crashes and restarts,
/// it will automatically recover all queries it should be running.
class Reconciler {
public:
    /// Create a Reconciler with the given configuration and worker interface
    Reconciler(ReconcilerConfiguration config, 
               std::shared_ptr<ReconcilerWorkerInterface> worker);
    
    ~Reconciler();
    
    // Non-copyable
    Reconciler(const Reconciler&) = delete;
    Reconciler& operator=(const Reconciler&) = delete;
    
    /// Start the background reconciliation loop
    void start();
    
    /// Stop the reconciliation loop (blocks until stopped)
    void stop();
    
    /// Check if the reconciler is running
    [[nodiscard]] bool isRunning() const;
    
    /// Force an immediate reconciliation (for testing)
    void reconcileNow();
    
    /// Get statistics about reconciliation
    struct Stats {
        uint64_t reconciliationCount = 0;
        uint64_t queriesStarted = 0;
        uint64_t queriesStopped = 0;
        uint64_t errors = 0;
        std::chrono::system_clock::time_point lastReconciliation;
    };
    [[nodiscard]] Stats getStats() const;

private:
    /// Main reconciliation loop (runs in background thread)
    void reconciliationLoop();
    
    /// Perform one reconciliation cycle
    void reconcile();
    
    /// Fetch assignments for this worker from etcd
    [[nodiscard]] std::unordered_map<DistributedQueryId, LogicalPlan> 
    fetchDesiredState();
    
    /// Build etcd key for looking up assignments
    [[nodiscard]] std::string buildAssignmentKeyPattern() const;
    
    /// Extract query ID from an etcd key
    [[nodiscard]] std::expected<DistributedQueryId, Exception>
    extractQueryIdFromKey(const std::string& key) const;
    
    /// Encode worker address for etcd key matching
    [[nodiscard]] std::string encodeWorkerAddr(const GrpcAddr& addr) const;

    ReconcilerConfiguration config;
    std::shared_ptr<ReconcilerWorkerInterface> worker;
    std::unique_ptr<etcd::SyncClient> etcdClient;
    
    std::atomic<bool> running{false};
    std::thread reconciliationThread;
    
    mutable std::mutex statsMutex;
    Stats stats;
};

} // namespace NES