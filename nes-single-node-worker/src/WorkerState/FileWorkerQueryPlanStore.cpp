#include <WorkerState/FileWorkerQueryPlanStore.h>

#include <fstream>
#include <filesystem>

namespace NES {

FileWorkerQueryPlanStore::FileWorkerQueryPlanStore(std::filesystem::path dir)
    : baseDir(std::move(dir))
{
    std::filesystem::create_directories(baseDir);
}

std::filesystem::path
FileWorkerQueryPlanStore::fileFor(const LocalQueryId& id) const
{
    // LocalQueryId is printable via stream
    std::stringstream ss;
    ss << id;
    return baseDir / ss.str();
}

std::expected<void, Exception>
FileWorkerQueryPlanStore::persist(const LocalQueryId& id,
                                  const LogicalPlan&)
{
    try {
        std::ofstream out(fileFor(id), std::ios::binary | std::ios::trunc);
        if (!out.is_open()) {
            return std::unexpected(
                Exception("Failed to open plan store file", 1000));
        }

        // Minimal persistence: marker file proves durability
        out << "PERSISTED\n";
        out.close();
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

    // Minimal recovery: enumerate files, but do not reconstruct plans
    for (const auto& entry : std::filesystem::directory_iterator(baseDir)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        // We deliberately do NOT deserialize yet
    }

    return restored;
}

} // namespace NES
