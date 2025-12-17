//
// Created by Janhvi Goje on 16/12/25.
//

#include <QueryManager/FileQueryPlanStore.h>
#include <filesystem>

namespace NES {

FileQueryPlanStore::FileQueryPlanStore(std::filesystem::path dir)
    : baseDir(std::move(dir))
{
    std::filesystem::create_directories(baseDir);
}

void FileQueryPlanStore::persist(const DistributedQueryId&,
                                 const DistributedLogicalPlan&)
{
    // TODO: implement proper plan serialization later
}

void FileQueryPlanStore::erase(const DistributedQueryId&)
{
    // TODO: implement deletion logic later
}

QueryPlanStore::StoredPlans FileQueryPlanStore::loadAll()
{
    return {};
}

} // namespace NES
