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
#include <string>

#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <SerializableQueryPlan.pb.h>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

#include <etcd/SyncClient.hpp>

namespace NES {

namespace {

/// Encode worker address for etcd key lookup (replace : with _)
std::string encodeWorkerAddr(const std::string& addr) {
    std::string encoded = addr;
    std::replace(encoded.begin(), encoded.end(), ':', '_');
    return encoded;
}

/// Extract distributed query ID from etcd key
/// Key format: {prefix}{queryId}/assignments/{workerAddr}
std::string extractQueryIdFromKey(const std::string& key, const std::string& prefix) {
    if (key.find(prefix) != 0) {
        return "";
    }
    std::string remainder = key.substr(prefix.size());
    auto slashPos = remainder.find('/');
    if (slashPos == std::string::npos) {
        return "";
    }
    return remainder.substr(0, slashPos);
}

} // anonymous namespace

Reconciler::Reconciler(ReconcilerConfiguration config, 
                       std::shared_ptr<ReconcilerWorkerInterface> worker)
    : config(std::move(config))
    , worker(std::move(worker))
    , etcdClient(std::make_unique<etcd::SyncClient>(this->config.etcdEndpoints))
{
    NES_INFO("Reconciler: initialized for worker {} polling {} every {}ms",
             this->config.workerAddress,
             this->config.etcdEndpoints,
             this->config.pollInterval.count());
}

Reconciler::~Reconciler() {
    stop();
}

void Reconciler::start() {
    if (running.exchange(true)) {
        NES_WARNING("Reconciler: already running");
        return;
    }
    
    NES_INFO("Reconciler: starting reconciliation loop");
    reconcileThread = std::thread(&Reconciler::reconciliationLoop, this);
}

void Reconciler::stop() {
    if (!running.exchange(false)) {
        return;
    }
    
    NES_INFO("Reconciler: stopping");
    if (reconcileThread.joinable()) {
        reconcileThread.join();
    }
    NES_INFO("Reconciler: stopped");
}

void Reconciler::reconciliationLoop() {
    NES_INFO("Reconciler: loop started");
    
    while (running.load()) {
        try {
            reconcile();
        } catch (const std::exception& e) {
            NES_ERROR("Reconciler: exception during reconciliation: {}", e.what());
            stats.errors++;
        }
        
        std::this_thread::sleep_for(config.pollInterval);
    }
    
    NES_INFO("Reconciler: loop exited");
}

std::vector<QueryAssignment> Reconciler::fetchDesiredState() {
    std::vector<QueryAssignment> assignments;
    
    std::string encodedWorkerAddr = encodeWorkerAddr(config.workerAddress);
    std::string assignmentSegment = "/assignments/" + encodedWorkerAddr;
    
    // List all keys under prefix
    etcd::Response response = etcdClient->ls(config.etcdKeyPrefix);
    
    if (!response.is_ok()) {
        if (response.error_code() == 100) {
            // Key not found - no queries exist
            return assignments;
        }
        NES_WARNING("Reconciler: etcd ls failed: {}", response.error_message());
        return assignments;
    }
    
    for (const auto& key : response.keys()) {
        // Check if this key is for our worker
        if (key.find(assignmentSegment) == std::string::npos) {
            continue;
        }
        
        // Extract query ID
        std::string queryId = extractQueryIdFromKey(key, config.etcdKeyPrefix);
        if (queryId.empty()) {
            NES_WARNING("Reconciler: malformed key '{}'", key);
            continue;
        }
        
        // Fetch the value
        etcd::Response getResponse = etcdClient->get(key);
        if (!getResponse.is_ok()) {
            NES_WARNING("Reconciler: failed to get key '{}'", key);
            continue;
        }
        
        // Deserialize the plan
        SerializableQueryPlan proto;
        if (!proto.ParseFromString(getResponse.value().as_string())) {
            NES_WARNING("Reconciler: failed to parse plan for key '{}'", key);
            continue;
        }
        
        LogicalPlan plan = QueryPlanSerializationUtil::deserializeQueryPlan(proto);
        
        assignments.push_back(QueryAssignment{
            .distributedQueryId = queryId,
            .plan = std::move(plan)
        });
        
        NES_DEBUG("Reconciler: found assignment for query {}", queryId);
    }
    
    return assignments;
}

void Reconciler::reconcile() {
    stats.reconcileCount++;
    
    // Get desired state from etcd
    auto desired = fetchDesiredState();
    
    // Get current running state from worker
    auto running = worker->getRunningDistributedQueryIds();
    
    // Build set of desired query IDs
    std::unordered_set<std::string> desiredIds;
    for (const auto& assignment : desired) {
        desiredIds.insert(assignment.distributedQueryId);
    }
    
    // Start queries that should be running but aren't
    for (const auto& assignment : desired) {
        if (!running.contains(assignment.distributedQueryId)) {
            NES_INFO("Reconciler: starting query {}", assignment.distributedQueryId);
            auto result = worker->startQuery(assignment.distributedQueryId, assignment.plan);
            if (result) {
                stats.queriesStarted++;
                NES_INFO("Reconciler: started query {} as local {}", 
                         assignment.distributedQueryId, *result);
            } else {
                stats.errors++;
                NES_ERROR("Reconciler: failed to start query {}: {}", 
                          assignment.distributedQueryId, result.error().what());
            }
        }
    }
    
    // Stop queries that are running but shouldn't be
    for (const auto& queryId : running) {
        if (!desiredIds.contains(queryId)) {
            NES_INFO("Reconciler: stopping query {}", queryId);
            auto result = worker->stopQuery(queryId);
            if (result) {
                stats.queriesStopped++;
            } else {
                stats.errors++;
                NES_ERROR("Reconciler: failed to stop query {}: {}", 
                          queryId, result.error().what());
            }
        }
    }
    
    NES_DEBUG("Reconciler: cycle complete - desired={}, running={}", 
              desired.size(), running.size());
}

} // namespace NES