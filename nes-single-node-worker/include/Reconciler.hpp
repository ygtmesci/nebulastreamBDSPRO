/*
    Licensed under the Apache License, Version 2.0 (the "License");
*/

#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>

namespace etcd {
class SyncClient;
}

namespace NES {

class SingleNodeWorker;

/// Reconciler polls etcd and starts/stops queries to match desired state
class Reconciler {
public:
    Reconciler(SingleNodeWorker& worker, 
               const std::string& workerAddress,
               const std::string& etcdEndpoints = "http://etcd:2379",
               const std::string& keyPrefix = "/nes/queries/",
               std::chrono::milliseconds pollInterval = std::chrono::milliseconds(1000));
    ~Reconciler();

    Reconciler(const Reconciler&) = delete;
    Reconciler& operator=(const Reconciler&) = delete;

    void start();
    void stop();
    bool isRunning() const { return running.load(); }

private:
    void reconciliationLoop();
    void reconcile();

    SingleNodeWorker& worker;
    std::string workerAddress;
    std::string etcdEndpoints;
    std::string keyPrefix;
    std::chrono::milliseconds pollInterval;
    
    std::unique_ptr<etcd::SyncClient> etcdClient;
    std::atomic<bool> running{false};
    std::thread reconcileThread;
    
    // Track distributed ID -> local ID mapping
    std::unordered_map<std::string, LocalQueryId> runningQueries;
    std::mutex mapMutex;
};

} // namespace NES