#ifndef NEBULASTREAM_FILEQUERYPLANSTORE_H
#define NEBULASTREAM_FILEQUERYPLANSTORE_H

#include <filesystem>
#include <QueryManager/QueryPlanStore.h>

namespace NES {

class FileQueryPlanStore final : public QueryPlanStore {
public:
    explicit FileQueryPlanStore(std::filesystem::path dir = "/tmp/nes-query-store");

    void persist(const DistributedQueryId& id,
                 const DistributedLogicalPlan& plan) override;

    void erase(const DistributedQueryId& id) override;

    QueryPlanStore::StoredPlans loadAll() override;

private:
    std::filesystem::path baseDir;

    std::filesystem::path fileFor(const DistributedQueryId& id) const;
};

} // namespace NES

#endif // NEBULASTREAM_FILEQUERYPLANSTORE_H
