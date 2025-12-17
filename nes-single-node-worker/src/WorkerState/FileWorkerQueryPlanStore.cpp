//
// Created by Janhvi Goje  on 16/12/25.
//

#include <WorkerState/FileWorkerQueryPlanStore.h>

#include <filesystem>

namespace NES {

FileWorkerQueryPlanStore::FileWorkerQueryPlanStore(std::filesystem::path dir)
    : baseDir(std::move(dir))
{
    std::filesystem::create_directories(baseDir);
}

std::expected<void, Exception>
FileWorkerQueryPlanStore::persist(const LocalQueryId&, const LogicalPlan&)
{
    // Stub: persistence format intentionally deferred
    return {};
}

std::expected<void, Exception>
FileWorkerQueryPlanStore::erase(const LocalQueryId&)
{
    // Stub
    return {};
}

std::expected<
    std::unordered_map<LocalQueryId, LogicalPlan>,
    Exception>
FileWorkerQueryPlanStore::loadAll()
{
    return {};
}

std::filesystem::path
FileWorkerQueryPlanStore::fileFor(const LocalQueryId& id) const
{
    return baseDir / id.getRawValue();
}

} // namespace NES
