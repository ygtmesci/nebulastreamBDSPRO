//
// Created by Janhvi Goje  on 16/12/25.
//

#include "QueryPlanStore.h"

class QueryPlanStore {
public:
    virtual ~QueryPlanStore() = default;

    virtual void persist(
        const DistributedQueryId& id,
        const std::unordered_map<GrpcAddr, std::vector<LocalQueryId>>& mapping
    ) = 0;

    virtual void erase(const DistributedQueryId& id) = 0;

    virtual std::unordered_map<
        DistributedQueryId,
        std::unordered_map<GrpcAddr, std::vector<LocalQueryId>>
    > loadAll() = 0;
};

