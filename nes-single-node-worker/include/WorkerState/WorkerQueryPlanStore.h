//
// Created by Janhvi Goje  on 16/12/25.
//

#pragma once

#include <unordered_map>
#include <expected>

#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>

namespace NES {

class WorkerQueryPlanStore {
public:
    virtual ~WorkerQueryPlanStore() = default;

    virtual std::expected<void, Exception>
    persist(const LocalQueryId& id,
            const LogicalPlan& plan) = 0;

    virtual std::expected<void, Exception>
    erase(const LocalQueryId& id) = 0;

    virtual std::expected<
        std::unordered_map<LocalQueryId, LogicalPlan>,
        Exception>
    loadAll() = 0;
};

} // namespace NES



