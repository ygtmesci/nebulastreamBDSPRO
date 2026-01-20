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

#include <regex>
#include <string>
#include <utility>
#include <vector>

#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <SerializableQueryPlan.pb.h>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

#include <etcd/SyncClient.hpp>
#include <etcd/Response.hpp>

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
    , client(std::make_unique<etcd::Client>(this->config.endpoints))
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

std::expected<GrpcAddr, Exception>
EtcdQueryStore::extractWorkerAddrFromKey(const std::string& key) const 
{
    // Key format: {prefix}{queryId}/assignments/{workerAddr}
    auto assignmentsPos = key.find(ASSIGNMENTS_SEGMENT);
    
    if (assignmentsPos == std::string::npos) {
        return std::unexpected(InvalidArgument(
            "Cannot extract worker address from key '{}'", key));
    }
    
    std::string encodedAddr = key.substr(
        assignmentsPos + std::string(ASSIGNMENTS_SEGMENT).size());
    
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
        // If multiple fragments exist, we concatenate them or store separately
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
            
            // Write to etcd
            auto response = client->put(key, serialized).get();
            
            if (!response.is_ok()) {
                return std::unexpected(Exception(
                    "etcd put failed for key '" + key + "': " + 
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
    
    auto response = client->rm_range(prefix).get();
    
    if (!response.is_ok()) {
        return std::unexpected(Exception(
            "etcd delete failed for prefix '" + prefix + "': " + 
            response.error_message(), ErrorCode::UnknownException));
    }
    
    NES_INFO("EtcdQueryStore: erased query {} ({} keys deleted)", 
             queryId, response.keys().size());
    return {};
}

std::expected<std::vector<QueryAssignment>, Exception>
EtcdQueryStore::getAssignmentsForWorker(const GrpcAddr& workerAddr) 
{
    NES_DEBUG("EtcdQueryStore: fetching assignments for worker {}", workerAddr);
    
    std::vector<QueryAssignment> assignments;
    
    // Get all keys under the query prefix
    auto response = client->ls(config.keyPrefix).get();
    
    if (!response.is_ok()) {
        return std::unexpected(Exception(
            "etcd ls failed: " + response.error_message(), 
            ErrorCode::UnknownException));
    }
    
    // Now fetch all keys to find those assigned to this worker
    auto rangeResponse = client->range(config.keyPrefix, 
                                        config.keyPrefix + "\xFF").get();
    
    if (!rangeResponse.is_ok()) {
        return std::unexpected(Exception(
            "etcd range failed: " + rangeResponse.error_message(),
            ErrorCode::UnknownException));
    }
    
    std::string encodedWorkerAddr = encodeWorkerAddr(workerAddr);
    
    for (const auto& kv : rangeResponse.kvs()) {
        const std::string& key = kv.key();
        
        // Check if this key is for our worker
        if (key.find(ASSIGNMENTS_SEGMENT + encodedWorkerAddr) == std::string::npos) {
            continue;
        }
        
        // Extract query ID
        auto queryIdResult = extractQueryIdFromKey(key);
        if (!queryIdResult) {
            NES_WARNING("EtcdQueryStore: skipping malformed key '{}'", key);
            continue;
        }
        
        // Deserialize plan
        SerializableQueryPlan proto;
        if (!proto.ParseFromString(kv.as_string())) {
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
    
    auto response = client->range(config.keyPrefix, 
                                   config.keyPrefix + "\xFF").get();
    
    if (!response.is_ok()) {
        return std::unexpected(Exception(
            "etcd range failed: " + response.error_message(),
            ErrorCode::UnknownException));
    }
    
    std::unordered_set<std::string> uniqueIds;
    
    for (const auto& kv : response.kvs()) {
        auto queryIdResult = extractQueryIdFromKey(kv.key());
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
    auto response = client->get("/__health_check__").get();
    // Even if key doesn't exist, connection is OK if no error
    return response.error_code() == 0 || response.error_code() == 100; // 100 = key not found
}

} // namespace NES
