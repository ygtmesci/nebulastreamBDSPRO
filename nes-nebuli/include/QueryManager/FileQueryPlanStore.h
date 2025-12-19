#ifndef NEBULASTREAM_FILEQUERYPLANSTORE_H
#define NEBULASTREAM_FILEQUERYPLANSTORE_H

#include <filesystem>
#include <QueryManager/QueryPlanStore.h>

namespace NES {

class FileQueryPlanStore : public QueryPlanStore {
public:
  explicit FileQueryPlanStore(std::filesystem::path dir);

  void persist(const DistributedQueryId&,
               const LogicalPlan&) override;

  void erase(const DistributedQueryId&) override;

  StoredPlans loadAll() override;

private:
  std::filesystem::path baseDir;
};

} // namespace NES

#endif // NEBULASTREAM_FILEQUERYPLANSTORE_H
