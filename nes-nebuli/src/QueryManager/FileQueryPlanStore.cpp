#include <QueryManager/FileQueryPlanStore.h>

#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <SerializableQueryPlan.pb.h>
#include <Util/Logger/Logger.hpp>

#include <filesystem>
#include <fstream>

namespace NES {

FileQueryPlanStore::FileQueryPlanStore(std::filesystem::path dir)
    : baseDir(std::move(dir)) {
    std::filesystem::create_directories(baseDir);
}

void FileQueryPlanStore::persist(const DistributedQueryId& id,
                                 const LogicalPlan& plan) {
    const auto filePath =
        baseDir / (plan.getQueryId().getRawValue()); // 29/12 Janhvi

    const SerializableQueryPlan proto =
        QueryPlanSerializationUtil::serializeQueryPlan(plan);

    std::ofstream out(filePath, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
        NES_ERROR("Failed to open file {}", filePath.string());
        return;
    }

    if (!proto.SerializeToOstream(&out)) {
        NES_ERROR("Failed to serialize query plan {}", id);
    }
}

void FileQueryPlanStore::erase(const DistributedQueryId& id) {
    std::filesystem::remove(
        baseDir / (id.getRawValue() + ".pb"));
}

QueryPlanStore::StoredPlans FileQueryPlanStore::loadAll() {
    StoredPlans result;

    if (!std::filesystem::exists(baseDir)) {
        return result;
    }

    for (const auto& entry : std::filesystem::directory_iterator(baseDir)) {
        if (!entry.is_regular_file()) {
            continue;
        }

        const std::string stem = entry.path().stem().string();

        if (stem.empty() || stem == DistributedQueryId::INVALID) {
            continue;
        }

        DistributedQueryId id{stem};

        std::ifstream input(entry.path(), std::ios::binary);
        if (!input.is_open()) {
            continue;
        }

        SerializableQueryPlan proto;
        if (!proto.ParseFromIstream(&input)) {
            NES_DEBUG("Failed to parse plan {}", id);
            continue;
        }

        LogicalPlan plan =
            QueryPlanSerializationUtil::deserializeQueryPlan(proto);

        result.emplace(std::move(id), std::move(plan));
    }

    return result;
}

} // namespace NES
