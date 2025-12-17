//
// Created by Janhvi Goje  on 16/12/25.
//

// nes-single-node-worker/include/WorkerState/FileWorkerQueryPlanStore.hpp

#pragma once

#include <filesystem>
#include <unordered_map>

#include <WorkerState/WorkerQueryPlanStore.h>

namespace NES {

class FileWorkerQueryPlanStore final : public WorkerQueryPlanStore {
public:
    explicit FileWorkerQueryPlanStore(std::filesystem::path dir);

    std::expected<void, Exception>
    persist(const LocalQueryId& id,
            const LogicalPlan& plan) override;

    std::expected<void, Exception>
    erase(const LocalQueryId& id) override;

    std::expected<
        std::unordered_map<LocalQueryId, LogicalPlan>,
        Exception>
    loadAll() override;

private:
    std::filesystem::path baseDir;

    std::filesystem::path fileFor(const LocalQueryId& id) const;
};

} // namespace NES

