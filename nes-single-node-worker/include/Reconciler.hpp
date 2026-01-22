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
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>

namespace etcd {
class SyncClient;
}

namespace NES {

/// Configuration for the Reconciler
struct ReconcilerConfiguration {
    std::string etcdEndpoints = "http://etcd:2379";
    std::string etcdKeyPrefix = "/nes/queries/";
    std::string workerAddress;  // e.g., "localhost:8080" - used to find assignments
    std::chrono::milliseconds pollInterval{1000};
};

/// Query assignment fetched from etcd
struct QueryAssignment {
    std::string distributedQueryId;
    LogicalPlan plan;
};

/// Interface for worker operations - allows decoupling from SingleNodeWorker
class ReconcilerWorkerInterface {
public:
    virtual ~ReconcilerWorkerInterface() = default;
    
    /// Get currently running query IDs (distributed IDs we're tracking)
    virtual std::unordered_set<std::string> getRunningDistributedQueryIds() const = 0;
    
    /// Start a query from a LogicalPlan, returns local query ID
    virtual std::expected<LocalQueryId, Exception> startQuery(
        const std::string& distributedQueryId, 
        LogicalPlan plan) = 0;
    
    /// Stop a query by distributed ID
    virtual std::expected<void, Exception> stopQuery(const std::string& distributedQueryId) = 0;
};

/// Reconciler polls etcd and reconciles desired state with running state
class Reconciler {
public:
    explicit Reconciler(ReconcilerConfiguration config, 
                        std::shared_ptr<ReconcilerWorkerInterface> worker);
    ~Reconciler();

    // Non-copyable, non-movable (owns thread)
    Reconciler(const Reconciler&) = delete;
    Reconciler& operator=(const Reconciler&) = delete;
    Reconciler(Reconciler&&) = delete;
    Reconciler& operator=(Reconciler&&) = delete;

    /// Start the reconciliation loop
    void start();
    
    /// Stop the reconciliation loop
    void stop();
    
    /// Check if running
    bool isRunning() const { return running.load(); }

    /// Statistics
    struct Stats {
        uint64_t reconcileCount = 0;
        uint64_t queriesStarted = 0;
        uint64_t queriesStopped = 0;
        uint64_t errors = 0;
    };
    Stats getStats() const { return stats; }

private:
    void reconciliationLoop();
    
    /// Fetch desired state from etcd
    std::vector<QueryAssignment> fetchDesiredState();
    
    /// Perform one reconciliation cycle
    void reconcile();

    ReconcilerConfiguration config;
    std::shared_ptr<ReconcilerWorkerInterface> worker;
    std::unique_ptr<etcd::SyncClient> etcdClient;
    
    std::atomic<bool> running{false};
    std::thread reconcileThread;
    
    Stats stats;
};

} // namespace NES