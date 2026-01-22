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

#include <Reconciler.hpp>

#include <algorithm>
#include <chrono>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <SerializableQueryPlan.pb.h>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

#include <etcd/SyncClient.hpp>
#include <etcd/Response.hpp>

namespace NES {

namespace {
constexpr const char* ASSIGNMENTS_SEGMENT = "/assignments/";
} // anonymous namespace

Reconciler::Reconciler(ReconcilerConfiguration config,
                       std::shared_ptr<ReconcilerWorkerInterface> worker)
    : config(std::move(config))
    , worker(std::move(worker))
    , etcdClient(std::make_unique<etcd::SyncClient>(this->config.etcdEndpoints))
{
    NES_INFO("Reconciler: initialized for worker {} with etcd at {}",
             this->config.workerAddress, this->config.etcdEndpoints);
}

Reconciler::~Reconciler() {
    stop();
}

void Reconciler::start() {
    if (running.exchange(true)) {
        NES_WARNING("Reconciler: already running");
        return;
    }
    
    NES_INFO("Reconciler: starting reconciliation loop (poll interval: {}ms)",
             config.pollInterval.count());
    
    reconciliationThread = std::thread([this]() {
        reconciliationLoop();
    });
}

void Reconciler::stop() {
    if (!running.exchange(false)) {
        return; // Already stopped
    }
    
    NES_INFO("Reconciler: stopping...");
    
    if (reconciliationThread.joinable()) {
        reconciliationThread.join();
    }
    
    NES_INFO("Reconciler: stopped");
}

bool Reconciler::isRunning() const {
    return running.load();
}

void Reconciler::reconcileNow() {
    reconcile();
}

Reconciler::Stats Reconciler::getStats() const {
    std::lock_guard<std::mutex> lock(statsMutex);
    return stats;
}

void Reconciler::reconciliationLoop() {
    NES_INFO("Reconciler: loop started for worker {}", config.workerAddress);
    
    // Perform initial reconciliation immediately
    reconcile();
    
    while (running.load()) {
        // Sleep for poll interval
        auto sleepUntil = std::chrono::steady_clock::now() + config.pollInterval;
        
        while (running.load() && std::chrono::steady_clock::now() < sleepUntil) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        
        if (running.load()) {
            reconcile();
        }
    }
    
    NES_INFO("Reconciler: loop ended");
}

void Reconciler::reconcile() {
    NES_DEBUG("Reconciler: starting reconciliation cycle");
    
    try {
        // 1. Fetch desired state from etcd
        auto desired = fetchDesiredState();
        
        // 2. Get current running queries
        auto running = worker->getRunningQueryIds();
        
        NES_DEBUG("Reconciler: desired={} queries, running={} queries",
                  desired.size(), running.size());
        
        // 3. Start queries that should run but aren't
        for (const auto& [queryId, plan] : desired) {
            if (!running.contains(queryId)) {
                NES_INFO("Reconciler: starting query {} (not currently running)", queryId);
                
                auto result = worker->startQuery(queryId, plan);
                if (result) {
                    NES_INFO("Reconciler: successfully started query {} (local ID: {})",
                             queryId, *result);
                    std::lock_guard<std::mutex> lock(statsMutex);
                    stats.queriesStarted++;
                } else {
                    NES_ERROR("Reconciler: failed to start query {}: {}",
                              queryId, result.error().what());
                    std::lock_guard<std::mutex> lock(statsMutex);
                    stats.errors++;
                }
            }
        }
        
        // 4. Stop queries that are running but shouldn't be
        for (const auto& queryId : running) {
            if (!desired.contains(queryId)) {
                NES_INFO("Reconciler: stopping query {} (no longer in etcd)", queryId);
                
                auto result = worker->stopQuery(queryId);
                if (result) {
                    NES_INFO("Reconciler: successfully stopped query {}", queryId);
                    std::lock_guard<std::mutex> lock(statsMutex);
                    stats.queriesStopped++;
                } else {
                    NES_ERROR("Reconciler: failed to stop query {}: {}",
                              queryId, result.error().what());
                    std::lock_guard<std::mutex> lock(statsMutex);
                    stats.errors++;
                }
            }
        }
        
        // Update stats
        {
            std::lock_guard<std::mutex> lock(statsMutex);
            stats.reconciliationCount++;
            stats.lastReconciliation = std::chrono::system_clock::now();
        }
        
        NES_DEBUG("Reconciler: reconciliation cycle complete");
        
    } catch (const std::exception& e) {
        NES_ERROR("Reconciler: exception during reconciliation: {}", e.what());
        std::lock_guard<std::mutex> lock(statsMutex);
        stats.errors++;
    }
}

std::unordered_map<DistributedQueryId, LogicalPlan> 
Reconciler::fetchDesiredState() {
    std::unordered_map<DistributedQueryId, LogicalPlan> result;
    
    // Fetch all keys under the query prefix
    auto response = etcdClient->range(config.keyPrefix, 
                                       config.keyPrefix + "\xFF");
    
    if (!response.is_ok()) {
        NES_ERROR("Reconciler: etcd range query failed: {}", response.error_message());
        return result;
    }
    
    std::string encodedWorkerAddr = encodeWorkerAddr(config.workerAddress);
    std::string assignmentPattern = ASSIGNMENTS_SEGMENT + encodedWorkerAddr;
    
    for (const auto& kv : response.kvs()) {
        const std::string& key = kv.key();
        
        // Check if this key is an assignment for our worker
        // Key format: /nes/queries/{queryId}/assignments/{workerAddr}
        if (key.find(assignmentPattern) == std::string::npos) {
            continue;
        }
        
        // Extract query ID
        auto queryIdResult = extractQueryIdFromKey(key);
        if (!queryIdResult) {
            NES_WARNING("Reconciler: skipping malformed key '{}': {}", 
                        key, queryIdResult.error().what());
            continue;
        }
        
        // Deserialize plan
        SerializableQueryPlan proto;
        if (!proto.ParseFromString(kv.as_string())) {
            NES_WARNING("Reconciler: failed to parse plan from key '{}'", key);
            continue;
        }
        
        LogicalPlan plan = QueryPlanSerializationUtil::deserializeQueryPlan(proto);
        
        // If we already have a plan for this query (multiple fragments),
        // we'd need to handle that - for now, we assume one fragment per worker
        if (result.contains(*queryIdResult)) {
            NES_WARNING("Reconciler: multiple fragments for query {} on this worker, using first",
                        *queryIdResult);
            continue;
        }
        
        result.emplace(*queryIdResult, std::move(plan));
        NES_DEBUG("Reconciler: found assignment for query {}", *queryIdResult);
    }
    
    return result;
}

std::string Reconciler::buildAssignmentKeyPattern() const {
    return config.keyPrefix + "*" + ASSIGNMENTS_SEGMENT + 
           encodeWorkerAddr(config.workerAddress);
}

std::expected<DistributedQueryId, Exception>
Reconciler::extractQueryIdFromKey(const std::string& key) const {
    // Key format: {prefix}{queryId}/assignments/{workerAddr}
    // Example: /nes/queries/swift_arabian/assignments/localhost_8080
    
    if (!key.starts_with(config.keyPrefix)) {
        return std::unexpected(InvalidArgument(
            "Key '{}' does not start with prefix '{}'", key, config.keyPrefix));
    }
    
    std::string remainder = key.substr(config.keyPrefix.size());
    auto slashPos = remainder.find('/');
    
    if (slashPos == std::string::npos) {
        return std::unexpected(InvalidArgument(
            "Cannot extract query ID from key '{}'", key));
    }
    
    return DistributedQueryId(remainder.substr(0, slashPos));
}

std::string Reconciler::encodeWorkerAddr(const GrpcAddr& addr) const {
    std::string encoded = addr.getRawValue();
    // Replace colons with underscores for safe key usage
    std::replace(encoded.begin(), encoded.end(), ':', '_');
    return encoded;
}

} // namespace NES