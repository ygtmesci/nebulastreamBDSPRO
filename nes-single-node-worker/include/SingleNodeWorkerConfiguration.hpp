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

#include <chrono>
#include <cstdint>
#include <string>
#include <optional>

namespace NES {

/// Configuration for the SingleNodeWorker
struct SingleNodeWorkerConfiguration {
    /// GRPC address this worker listens on (also used as worker identity)
    std::string grpcAddressUri = "localhost:8080";
    
    /// Number of worker threads for query execution
    uint32_t numWorkerThreads = 4;
    
    /// Buffer size for data processing
    uint32_t bufferSize = 4096;
    
    /// Number of buffers in the pool
    uint32_t numBuffers = 128;
    
    // ============================================
    // Reconciler / etcd configuration
    // ============================================
    
    /// Enable reconciler (pull-based mode with etcd)
    /// If false, worker operates in traditional GRPC push mode
    bool enableReconciler = false;
    
    /// etcd endpoint(s) for the reconciler
    /// Can be comma-separated for multiple endpoints
    std::string etcdEndpoints = "http://etcd:2379";
    
    /// Key prefix for queries in etcd
    std::string etcdKeyPrefix = "/nes/queries/";
    
    /// How often the reconciler polls etcd (milliseconds)
    std::chrono::milliseconds reconcilerPollInterval{1000};
    
    // ============================================
    // Helper methods
    // ============================================
    
    /// Parse configuration from command line arguments
    static SingleNodeWorkerConfiguration fromCommandLine(int argc, char** argv);
    
    /// Parse configuration from environment variables
    static SingleNodeWorkerConfiguration fromEnvironment();
    
    /// Validate configuration, throws if invalid
    void validate() const;
};

} // namespace NES