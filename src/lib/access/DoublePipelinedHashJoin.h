#pragma once

#include "access/PipelineObserver.h"
#include "access/PipelineEmitter.h"
#include "access/system/PlanOperation.h"
#include "tbb/concurrent_unordered_map.h"
#include "storage/storage_types.h"

namespace hyrise {
namespace access {
class DoublePipelinedHashJoin : public PlanOperation, public PipelineObserver<DoublePipelinedHashJoin>,
                                public PipelineEmitter<DoublePipelinedHashJoin> {


 public:
  void executePlanOperation() override;
  static std::shared_ptr<PlanOperation> parse(const Json::Value& data);

 private:
  // TODO maybe we should use the same hasher as in other multimap here
  typedef size_t join_key_t ;
  typedef std::pair<bool, pos_t> join_value_t;
  typedef tbb::concurrent_unordered_multimap<join_key_t, join_value_t> hashtable_t;
  typedef std::shared_ptr<hashtable_t> hashtable_ptr_t;

  hashtable_ptr_t hashtable;
};
}
}
