#pragma once

#include <unordered_map>
#include <DistributedQuery.hpp>

namespace NES {

class QueryPlanStore {
public:
    using StoredPlans =
        std::unordered_map<DistributedQueryId, LogicalPlan>;

    virtual ~QueryPlanStore() = default;

    virtual void persist(
        const DistributedQueryId&,
        const LogicalPlan&) = 0;

    virtual void erase(const DistributedQueryId&) = 0;

    virtual StoredPlans loadAll() = 0;
};

} // namespace NES
