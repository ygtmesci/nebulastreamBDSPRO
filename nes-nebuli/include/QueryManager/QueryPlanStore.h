#ifndef NEBULASTREAM_QUERYPLANSTORE_H
#define NEBULASTREAM_QUERYPLANSTORE_H

#include <unordered_map>

#include <DistributedQuery.hpp>   // ✅ REQUIRED: DistributedQueryId, DistributedLogicalPlan

namespace NES {

class QueryPlanStore {
public:
    using StoredPlans =
        std::unordered_map<DistributedQueryId, DistributedLogicalPlan>;

    virtual ~QueryPlanStore() = default;

    virtual void persist(const DistributedQueryId& id,
                         const DistributedLogicalPlan& plan) = 0;

    virtual void erase(const DistributedQueryId& id) = 0;

    virtual StoredPlans loadAll() = 0;
};

} // namespace NES

#endif // NEBULASTREAM_QUERYPLANSTORE_H
