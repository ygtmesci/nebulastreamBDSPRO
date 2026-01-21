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

#include <QueryManager/EtcdQueryStore.hpp>

#include <algorithm>
#include <regex>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <SerializableQueryPlan.pb.h>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

// etcd-cpp-apiv3 headers
#include <etcd/SyncClient.hpp>

namespace NES {

namespace {

constexpr const char* ASSIGNMENTS_SEGMENT = "/assignments/";

/// Encode worker address for use in etcd keys (replace special chars)
std::string encodeWorkerAddr(const GrpcAddr& addr) {
    std::string encoded = addr.getRawValue();
    // Replace colons with underscores for safe key usage
    std::replace(encoded.begin(), encoded.end(), ':', '_');
    return encoded;
}

/// Decode worker address from etcd key format
GrpcAddr decodeWorkerAddr(const std::string& encoded) {
    std::string decoded = encoded;
    std::replace(decoded.begin(), decoded.end(), '_', ':');
    return GrpcAddr(decoded);
}

} // anonymous namespace

EtcdQueryStore::EtcdQueryStore(EtcdConfiguration config)
    : config(std::move(config))
    , client(std::make_unique<etcd::SyncClient>(this->config.endpoints))
{
    NES_INFO("EtcdQueryStore: connecting to {} with prefix '{}'", 
             this->config.endpoints, this->config.keyPrefix);
}

EtcdQueryStore::~EtcdQueryStore() = default;

EtcdQueryStore::EtcdQueryStore(EtcdQueryStore&&) noexcept = default;
EtcdQueryStore& EtcdQueryStore::operator=(EtcdQueryStore&&) noexcept = default;

std::string EtcdQueryStore::buildAssignmentKey(
    const DistributedQueryId& queryId, 
    const GrpcAddr& workerAddr) const 
{
    return config.keyPrefix + queryId.getRawValue() + 
           ASSIGNMENTS_SEGMENT + encodeWorkerAddr(workerAddr);
}

std::string EtcdQueryStore::buildQueryPrefix(
    const DistributedQueryId& queryId) const 
{
    return config.keyPrefix + queryId.getRawValue() + "/";
}

std::expected<DistributedQueryId, Exception>
EtcdQueryStore::extractQueryIdFromKey(const std::string& key) const 
{
    // Key format: {prefix}{queryId}/assignments/{workerAddr}
    // Example: /nes/queries/swift_arabian/assignments/localhost_8080
    
    if (key.rfind(config.keyPrefix, 0) != 0) {
        return std::unexpected(Exception(
            "Key does not start with expected prefix", ErrorCode::UnknownException));
    }
    
    std::string remainder = key.substr(config.keyPrefix.size());
    auto slashPos = remainder.find('/');
    
    if (slashPos == std::string::npos) {
        return std::unexpected(Exception(
            "Cannot extract query ID from key", ErrorCode::UnknownException));
    }
    
    return DistributedQueryId(remainder.substr(0, slashPos));
}

std::expected<GrpcAddr, Exception>
EtcdQueryStore::extractWorkerAddrFromKey(const std::string& key) const 
{
    // Key format: {prefix}{queryId}/assignments/{workerAddr}
    auto assignmentsPos = key.find(ASSIGNMENTS_SEGMENT);
    
    if (assignmentsPos == std::string::npos) {
        return std::unexpected(Exception(
            "Cannot extract worker address from key", ErrorCode::UnknownException));
    }
    
    std::string encodedAddr = key.substr(
        assignmentsPos + std::string(ASSIGNMENTS_SEGMENT).size());
    
    // Handle case where there might be additional path segments (e.g., fragment index)
    auto nextSlash = encodedAddr.find('/');
    if (nextSlash != std::string::npos) {
        encodedAddr = encodedAddr.substr(0, nextSlash);
    }
    
    return decodeWorkerAddr(encodedAddr);
}

std::expected<void, Exception> 
EtcdQueryStore::persistQuery(
    const DistributedQueryId& queryId, 
    const DistributedLogicalPlan& plan) 
{
    NES_DEBUG("EtcdQueryStore: persisting query {} with {} worker assignments",
              queryId, plan.size());
    
    for (const auto& [workerAddr, fragments] : plan) {
        // For now, we assume one fragment per worker
        // If multiple fragments exist, we store them separately
        for (size_t i = 0; i < fragments.size(); ++i) {
            const auto& fragment = fragments[i];
            
            // Build key - if multiple fragments, append index
            std::string key = buildAssignmentKey(queryId, workerAddr);
            if (fragments.size() > 1) {
                key += "/" + std::to_string(i);
            }
            
            // Serialize the plan fragment
            SerializableQueryPlan proto = 
                QueryPlanSerializationUtil::serializeQueryPlan(fragment);
            
            std::string serialized;
            if (!proto.SerializeToString(&serialized)) {
                return std::unexpected(Exception(
                    "Failed to serialize plan fragment for query " + 
                    queryId.getRawValue(), ErrorCode::UnknownException));
            }
            
            // Write to etcd using set() - the correct method name in etcd-cpp-apiv3
            etcd::Response response = client->set(key, serialized);
            
            if (!response.is_ok()) {
                return std::unexpected(Exception(
                    "etcd set failed for key '" + key + "': " + 
                    response.error_message(), ErrorCode::UnknownException));
            }
            
            NES_DEBUG("EtcdQueryStore: stored fragment {} for worker {} (key: {})",
                      i, workerAddr, key);
        }
    }
    
    NES_INFO("EtcdQueryStore: successfully persisted query {}", queryId);
    return {};
}

std::expected<void, Exception> 
EtcdQueryStore::eraseQuery(const DistributedQueryId& queryId) 
{
    std::string prefix = buildQueryPrefix(queryId);
    
    NES_DEBUG("EtcdQueryStore: erasing query {} (prefix: {})", queryId, prefix);
    
    // Use rmdir with recursive=true to delete all keys under the prefix
    etcd::Response response = client->rmdir(prefix, true);
    
    if (!response.is_ok()) {
        // Error code 100 means "key not found" which is acceptable for delete
        if (response.error_code() == 100) {
            NES_DEBUG("EtcdQueryStore: query {} not found in etcd (already deleted?)", queryId);
            return {};
        }
        return std::unexpected(Exception(
            "etcd rmdir failed for prefix '" + prefix + "': " + 
            response.error_message(), ErrorCode::UnknownException));
    }
    
    NES_INFO("EtcdQueryStore: erased query {}", queryId);
    return {};
}

std::expected<std::vector<QueryAssignment>, Exception>
EtcdQueryStore::getAssignmentsForWorker(const GrpcAddr& workerAddr) 
{
    NES_DEBUG("EtcdQueryStore: fetching assignments for worker {}", workerAddr);
    
    std::vector<QueryAssignment> assignments;
    
    // Use ls() to get all keys under the query prefix
    etcd::Response response = client->ls(config.keyPrefix);
    
    if (!response.is_ok()) {
        // Error code 100 means "key not found" - no queries exist yet
        if (response.error_code() == 100) {
            NES_DEBUG("EtcdQueryStore: no queries found in etcd");
            return assignments;
        }
        return std::unexpected(Exception(
            "etcd ls failed: " + response.error_message(), 
            ErrorCode::UnknownException));
    }
    
    std::string encodedWorkerAddr = encodeWorkerAddr(workerAddr);
    
    // Get keys from the response
    const auto& keys = response.keys();
    
    for (size_t i = 0; i < keys.size(); ++i) {
        const std::string& key = keys[i];
        
        // Check if this key is for our worker
        std::string workerSegment = std::string(ASSIGNMENTS_SEGMENT) + encodedWorkerAddr;
        if (key.find(workerSegment) == std::string::npos) {
            continue;
        }
        
        // Extract query ID
        auto queryIdResult = extractQueryIdFromKey(key);
        if (!queryIdResult) {
            NES_WARNING("EtcdQueryStore: skipping malformed key '{}'", key);
            continue;
        }
        
        // Get the value - need to fetch it separately since ls() may not return values
        etcd::Response getResponse = client->get(key);
        if (!getResponse.is_ok()) {
            NES_WARNING("EtcdQueryStore: failed to get value for key '{}'", key);
            continue;
        }
        
        // Deserialize plan
        SerializableQueryPlan proto;
        if (!proto.ParseFromString(getResponse.value().as_string())) {
            NES_WARNING("EtcdQueryStore: failed to parse plan from key '{}'", key);
            continue;
        }
        
        LogicalPlan plan = 
            QueryPlanSerializationUtil::deserializeQueryPlan(proto);
        
        assignments.push_back(QueryAssignment{
            .queryId = *queryIdResult,
            .plan = std::move(plan)
        });
        
        NES_DEBUG("EtcdQueryStore: found assignment for query {} on worker {}",
                  *queryIdResult, workerAddr);
    }
    
    NES_INFO("EtcdQueryStore: found {} assignments for worker {}", 
             assignments.size(), workerAddr);
    
    return assignments;
}

std::expected<std::vector<DistributedQueryId>, Exception>
EtcdQueryStore::getAllQueryIds() 
{
    NES_DEBUG("EtcdQueryStore: fetching all query IDs");
    
    etcd::Response response = client->ls(config.keyPrefix);
    
    if (!response.is_ok()) {
        // Error code 100 means "key not found" - no queries exist
        if (response.error_code() == 100) {
            return std::vector<DistributedQueryId>{};
        }
        return std::unexpected(Exception(
            "etcd ls failed: " + response.error_message(),
            ErrorCode::UnknownException));
    }
    
    std::unordered_set<std::string> uniqueIds;
    
    for (const auto& key : response.keys()) {
        auto queryIdResult = extractQueryIdFromKey(key);
        if (queryIdResult) {
            uniqueIds.insert(queryIdResult->getRawValue());
        }
    }
    
    std::vector<DistributedQueryId> result;
    result.reserve(uniqueIds.size());
    for (const auto& id : uniqueIds) {
        result.emplace_back(id);
    }
    
    NES_DEBUG("EtcdQueryStore: found {} unique queries", result.size());
    return result;
}

bool EtcdQueryStore::isConnected() const 
{
    // Simple health check - try to get a non-existent key
    etcd::Response response = client->get("/__nes_health_check__");
    // Even if key doesn't exist, connection is OK if no network error
    // Error code 100 = key not found (which is fine)
    // Error code 0 = success
    return response.error_code() == 0 || response.error_code() == 100;
}

} // namespace NES