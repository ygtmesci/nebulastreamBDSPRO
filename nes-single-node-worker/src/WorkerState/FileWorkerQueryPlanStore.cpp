#include <WorkerState/FileWorkerQueryPlanStore.h>

#include <fstream>
#include <filesystem>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <SerializableQueryPlan.pb.h>


namespace NES {

FileWorkerQueryPlanStore::FileWorkerQueryPlanStore(std::filesystem::path dir)
    : baseDir(std::move(dir))
{
    std::filesystem::create_directories(baseDir);
}

std::filesystem::path
FileWorkerQueryPlanStore::fileFor(const LocalQueryId& id) const
{
    return baseDir / id.getRawValue();
}


std::expected<void, Exception>
FileWorkerQueryPlanStore::persist(const LocalQueryId& id,
                                  const LogicalPlan& plan)
{
    try {
        std::ofstream out(fileFor(id), std::ios::binary | std::ios::trunc);
        if (!out.is_open()) {
            return std::unexpected(Exception("Failed to open plan store file", 1000));
        }

        SerializableQueryPlan proto =
            QueryPlanSerializationUtil::serializeQueryPlan(plan);

        if (!proto.SerializeToOstream(&out)) {
            return std::unexpected(Exception("Failed to serialize LogicalPlan", 1000));
        }

        return {};
    }
    catch (...) {
        return std::unexpected(wrapExternalException());
    }
}

std::expected<void, Exception>
FileWorkerQueryPlanStore::erase(const LocalQueryId& id)
{
    std::error_code ec;
    std::filesystem::remove(fileFor(id), ec);
    return {};
}

std::expected<std::unordered_map<LocalQueryId, LogicalPlan>, Exception>
FileWorkerQueryPlanStore::loadAll()
{
    NES_INFO("FileWorkerQueryPlanStore::loadAll scanning {}", baseDir.string());
    std::unordered_map<LocalQueryId, LogicalPlan> restored;

    for (const auto& entry : std::filesystem::directory_iterator(baseDir)) {
        if (!entry.is_regular_file()) {
            continue;
        }

        const auto stem = entry.path().stem().string(); // removes ".pb"
        const LocalQueryId id{stem};

        std::ifstream in(entry.path(), std::ios::binary);
        if (!in.is_open()) {
            continue;
        }

        SerializableQueryPlan proto;
        if (!proto.ParseFromIstream(&in)) {
            NES_ERROR("Failed to parse persisted plan {}", id);
            continue;
        }

        LogicalPlan plan =
            QueryPlanSerializationUtil::deserializeQueryPlan(proto);

        restored.emplace(id, std::move(plan));
    }

    return restored;
}


} // namespace NES
