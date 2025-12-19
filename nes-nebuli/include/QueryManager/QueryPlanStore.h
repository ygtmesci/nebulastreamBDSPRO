#ifndef NEBULASTREAM_QUERYPLANSTORE_H
#define NEBULASTREAM_QUERYPLANSTORE_H

#include <unordered_map>

#include <DistributedQuery.hpp>   //  REQUIRED: DistributedQueryId, DistributedLogicalPlan

namespace NES
{
class QueryPlanStore {
public:
  using StoredPlans =
      std::unordered_map<DistributedQueryId, LogicalPlan>;

  virtual ~QueryPlanStore() = default;

  virtual void persist(const DistributedQueryId&,
                       const LogicalPlan&) = 0;

  virtual void erase(const DistributedQueryId&) = 0;

  virtual StoredPlans loadAll() = 0;
};
};

#endif // NEBULASTREAM_QUERYPLANSTORE_H
