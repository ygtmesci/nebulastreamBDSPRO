/*
    Licensed under the Apache License, Version 2.0 (the "License");
*/

#include <Reconciler.hpp>
#include <SingleNodeWorker.hpp>

#include <algorithm>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <SerializableQueryPlan.pb.h>
#include <Util/Logger/Logger.hpp>
#include <Runtime/QueryTerminationType.hpp>

#include <etcd/SyncClient.hpp>

namespace NES {

namespace {
std::string encodeWorkerAddr(const std::string& addr) {
    std::string encoded = addr;
    std::replace(encoded.begin(), encoded.end(), ':', '_');
    return encoded;
}

std::string extractQueryIdFromKey(const std::string& key, const std::string& prefix) {
    if (key.find(prefix) != 0) return "";
    std::string remainder = key.substr(prefix.size());
    auto slashPos = remainder.find('/');
    if (slashPos == std::string::npos) return "";
    return remainder.substr(0, slashPos);
}
}

Reconciler::Reconciler(SingleNodeWorker& worker,
                       const std::string& workerAddress,
                       const std::string& etcdEndpoints,
                       const std::string& keyPrefix,
                       std::chrono::milliseconds pollInterval)
    : worker(worker)
    , workerAddress(workerAddress)
    , etcdEndpoints(etcdEndpoints)
    , keyPrefix(keyPrefix)
    , pollInterval(pollInterval)
    , etcdClient(std::make_unique<etcd::SyncClient>(etcdEndpoints))
{
    NES_INFO("Reconciler: initialized for worker {} at {}", workerAddress, etcdEndpoints);
}

Reconciler::~Reconciler() {
    stop();
}

void Reconciler::start() {
    if (running.exchange(true)) return;
    NES_INFO("Reconciler: starting");
    reconcileThread = std::thread(&Reconciler::reconciliationLoop, this);
}

void Reconciler::stop() {
    if (!running.exchange(false)) return;
    NES_INFO("Reconciler: stopping");
    if (reconcileThread.joinable()) {
        reconcileThread.join();
    }
}

void Reconciler::reconciliationLoop() {
    while (running.load()) {
        try {
            reconcile();
        } catch (const std::exception& e) {
            NES_ERROR("Reconciler: error: {}", e.what());
        }
        std::this_thread::sleep_for(pollInterval);
    }
}

void Reconciler::reconcile() {
    std::string encodedAddr = encodeWorkerAddr(workerAddress);
    std::string assignmentSegment = "/assignments/" + encodedAddr;

    // Fetch desired state from etcd
    etcd::Response response = etcdClient->ls(keyPrefix);
    
    std::unordered_set<std::string> desiredQueries;
    std::unordered_map<std::string, LogicalPlan> newPlans;

    if (response.is_ok()) {
        for (const auto& key : response.keys()) {
            if (key.find(assignmentSegment) == std::string::npos) continue;
            
            std::string queryId = extractQueryIdFromKey(key, keyPrefix);
            if (queryId.empty()) continue;
            
            desiredQueries.insert(queryId);
            
            // Check if already running
            {
                std::lock_guard<std::mutex> lock(mapMutex);
                if (runningQueries.contains(queryId)) continue;
            }
            
            // Fetch and deserialize plan
            etcd::Response getResp = etcdClient->get(key);
            if (!getResp.is_ok()) continue;
            
            SerializableQueryPlan proto;
            if (!proto.ParseFromString(getResp.value().as_string())) continue;
            
            newPlans.emplace(queryId, QueryPlanSerializationUtil::deserializeQueryPlan(proto));
        }
    }

    // Start new queries
    for (auto& [queryId, plan] : newPlans) {
        NES_INFO("Reconciler: starting query {}", queryId);
        auto regResult = worker.registerQuery(std::move(plan));
        if (!regResult) {
            NES_ERROR("Reconciler: failed to register {}: {}", queryId, regResult.error().what());
            continue;
        }
        
        auto startResult = worker.startQuery(*regResult);
        if (!startResult) {
            NES_ERROR("Reconciler: failed to start {}: {}", queryId, startResult.error().what());
            worker.unregisterQuery(*regResult);
            continue;
        }
        
        std::lock_guard<std::mutex> lock(mapMutex);
        runningQueries.insert_or_assign(queryId, *regResult);
        NES_INFO("Reconciler: started query {} as {}", queryId, *regResult);
    }

    // Stop removed queries
    std::vector<std::string> toRemove;
    {
        std::lock_guard<std::mutex> lock(mapMutex);
        for (const auto& [queryId, localId] : runningQueries) {
            if (!desiredQueries.contains(queryId)) {
                toRemove.push_back(queryId);
            }
        }
    }
    
    for (const auto& queryId : toRemove) {
        LocalQueryId localId;
        {
            std::lock_guard<std::mutex> lock(mapMutex);
            localId = runningQueries.at(queryId);
        }
        
        NES_INFO("Reconciler: stopping query {}", queryId);
        worker.stopQuery(localId, QueryTerminationType::Graceful);
        worker.unregisterQuery(localId);
        
        std::lock_guard<std::mutex> lock(mapMutex);
        runningQueries.erase(queryId);
    }
}

} // namespace NES