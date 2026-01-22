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

#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <Reconciler.hpp>

#include <memory>
#include <unordered_map>
#include <unordered_set>

#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES {

/// Implementation of ReconcilerWorkerInterface that bridges to SingleNodeWorker
class SingleNodeWorkerReconcilerBridge : public ReconcilerWorkerInterface {
public:
    explicit SingleNodeWorkerReconcilerBridge(SingleNodeWorker* worker)
        : worker(worker) {}
    
    std::unordered_set<DistributedQueryId> getRunningQueryIds() const override {
        return worker->getRunningDistributedQueryIds();
    }
    
    std::expected<LocalQueryId, Exception> 
    startQuery(const DistributedQueryId& queryId, const LogicalPlan& plan) override {
        return worker->registerAndStartQuery(queryId, plan);
    }
    
    std::expected<void, Exception>
    stopQuery(const DistributedQueryId& queryId) override {
        return worker->stopAndUnregisterQuery(queryId);
    }
    
private:
    SingleNodeWorker* worker;
};

/// SingleNodeWorker implementation with Reconciler support
class SingleNodeWorker {
public:
    explicit SingleNodeWorker(SingleNodeWorkerConfiguration config)
        : config(std::move(config))
    {
        NES_INFO("SingleNodeWorker: initializing at {}", this->config.grpcAddressUri);
        
        // Initialize node engine, compiler, etc.
        initializeEngine();
        
        // If reconciler is enabled, set it up
        if (this->config.enableReconciler) {
            initializeReconciler();
        }
    }
    
    ~SingleNodeWorker() {
        shutdown();
    }
    
    /// Start the worker (including reconciler if enabled)
    void start() {
        NES_INFO("SingleNodeWorker: starting");
        
        // Start GRPC server (for status queries, etc.)
        startGrpcServer();
        
        // Start reconciler if enabled
        if (reconciler) {
            reconciler->start();
        }
        
        NES_INFO("SingleNodeWorker: started successfully");
    }
    
    /// Shutdown the worker
    void shutdown() {
        NES_INFO("SingleNodeWorker: shutting down");
        
        // Stop reconciler first
        if (reconciler) {
            reconciler->stop();
        }
        
        // Stop all running queries
        stopAllQueries();
        
        // Stop GRPC server
        stopGrpcServer();
        
        NES_INFO("SingleNodeWorker: shutdown complete");
    }
    
    // ========================================
    // Methods called by Reconciler
    // ========================================
    
    /// Get all running distributed query IDs
    std::unordered_set<DistributedQueryId> getRunningDistributedQueryIds() const {
        std::lock_guard<std::mutex> lock(queryMapMutex);
        
        std::unordered_set<DistributedQueryId> result;
        for (const auto& [distId, localId] : distributedToLocalQueryMap) {
            result.insert(distId);
        }
        return result;
    }
    
    /// Register and start a query from a LogicalPlan
    std::expected<LocalQueryId, Exception> 
    registerAndStartQuery(const DistributedQueryId& distributedQueryId, 
                          const LogicalPlan& plan) {
        NES_INFO("SingleNodeWorker: registering query {} from reconciler", 
                 distributedQueryId);
        
        // Check if already running
        {
            std::lock_guard<std::mutex> lock(queryMapMutex);
            if (distributedToLocalQueryMap.contains(distributedQueryId)) {
                NES_WARNING("SingleNodeWorker: query {} already running", distributedQueryId);
                return distributedToLocalQueryMap.at(distributedQueryId);
            }
        }
        
        // Compile the plan
        // auto physicalPlan = compiler->compile(localOptimizer->optimize(plan));
        
        // Register with node engine
        // auto localQueryId = nodeEngine->registerQuery(physicalPlan);
        
        // For now, generate a placeholder LocalQueryId
        auto localQueryId = LocalQueryId(generateUniqueId());
        
        // Start the query
        // nodeEngine->startQuery(localQueryId);
        
        // Track the mapping
        {
            std::lock_guard<std::mutex> lock(queryMapMutex);
            distributedToLocalQueryMap[distributedQueryId] = localQueryId;
            localToDistributedQueryMap[localQueryId] = distributedQueryId;
        }
        
        NES_INFO("SingleNodeWorker: started query {} (local ID: {})", 
                 distributedQueryId, localQueryId);
        
        return localQueryId;
    }
    
    /// Stop and unregister a query
    std::expected<void, Exception>
    stopAndUnregisterQuery(const DistributedQueryId& distributedQueryId) {
        NES_INFO("SingleNodeWorker: stopping query {}", distributedQueryId);
        
        LocalQueryId localQueryId;
        
        // Find the local query ID
        {
            std::lock_guard<std::mutex> lock(queryMapMutex);
            auto it = distributedToLocalQueryMap.find(distributedQueryId);
            if (it == distributedToLocalQueryMap.end()) {
                return std::unexpected(QueryNotFound(
                    "Query {} not found", distributedQueryId));
            }
            localQueryId = it->second;
        }
        
        // Stop the query
        // nodeEngine->stopQuery(localQueryId);
        // nodeEngine->unregisterQuery(localQueryId);
        
        // Remove from tracking
        {
            std::lock_guard<std::mutex> lock(queryMapMutex);
            distributedToLocalQueryMap.erase(distributedQueryId);
            localToDistributedQueryMap.erase(localQueryId);
        }
        
        NES_INFO("SingleNodeWorker: stopped query {}", distributedQueryId);
        
        return {};
    }

private:
    void initializeEngine() {
        // Initialize buffer manager, node engine, compiler, etc.
        // This is placeholder - actual implementation depends on existing code
        NES_DEBUG("SingleNodeWorker: initializing engine components");
    }
    
    void initializeReconciler() {
        NES_INFO("SingleNodeWorker: initializing reconciler");
        
        ReconcilerConfiguration reconcilerConfig;
        reconcilerConfig.etcdEndpoints = config.etcdEndpoints;
        reconcilerConfig.keyPrefix = config.etcdKeyPrefix;
        reconcilerConfig.pollInterval = config.reconcilerPollInterval;
        reconcilerConfig.workerAddress = GrpcAddr(config.grpcAddressUri);
        
        auto bridge = std::make_shared<SingleNodeWorkerReconcilerBridge>(this);
        reconciler = std::make_unique<Reconciler>(reconcilerConfig, bridge);
        
        NES_INFO("SingleNodeWorker: reconciler initialized (poll interval: {}ms)",
                 config.reconcilerPollInterval.count());
    }
    
    void startGrpcServer() {
        // Start GRPC server for status queries
        NES_DEBUG("SingleNodeWorker: starting GRPC server");
    }
    
    void stopGrpcServer() {
        // Stop GRPC server
        NES_DEBUG("SingleNodeWorker: stopping GRPC server");
    }
    
    void stopAllQueries() {
        std::lock_guard<std::mutex> lock(queryMapMutex);
        
        for (const auto& [distId, localId] : distributedToLocalQueryMap) {
            NES_DEBUG("SingleNodeWorker: stopping query {} during shutdown", distId);
            // nodeEngine->stopQuery(localId);
        }
        
        distributedToLocalQueryMap.clear();
        localToDistributedQueryMap.clear();
    }
    
    static std::string generateUniqueId() {
        static std::atomic<uint64_t> counter{0};
        return "local-" + std::to_string(counter++);
    }
    
    SingleNodeWorkerConfiguration config;
    
    // Reconciler (only if enabled)
    std::unique_ptr<Reconciler> reconciler;
    
    // Query tracking: DistributedQueryId <-> LocalQueryId
    mutable std::mutex queryMapMutex;
    std::unordered_map<DistributedQueryId, LocalQueryId> distributedToLocalQueryMap;
    std::unordered_map<LocalQueryId, DistributedQueryId> localToDistributedQueryMap;
    
    // These would be the actual engine components
    // std::unique_ptr<NodeEngine> nodeEngine;
    // std::unique_ptr<QueryCompiler> compiler;
    // std::unique_ptr<LocalOptimizer> localOptimizer;
};

} // namespace NES