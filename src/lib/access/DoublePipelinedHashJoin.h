#pragma once

#include "access/PipelineObserver.h"
#include "access/PipelineEmitter.h"
#include "access/system/PlanOperation.h"

#include "tbb/concurrent_unordered_map.h"

#include "storage/storage_types.h"
#include "storage/meta_storage.h"

namespace hyrise {
namespace access {

template <typename T>
struct row_hash_functor {
  typedef T value_type;
  const storage::AbstractTable* table;
  storage::field_t f;
  size_t row;

  row_hash_functor(const storage::AbstractTable* t, const storage::field_t f, const size_t row) : table(t), f(f), row(row) {}

  template <typename R>
  inline T operator()() {
    return std::hash<R>()(table->getValue<R>(f, row));
  }

};

class DoublePipelinedHashJoin : public PlanOperation, public PipelineObserver<DoublePipelinedHashJoin>,
                                public PipelineEmitter<DoublePipelinedHashJoin> {


 public:
  void executePlanOperation() override;
  void setupPlanOperation() override;

  static std::shared_ptr<PlanOperation> parse(const Json::Value& data);
  virtual std::shared_ptr<PlanOperation> copy() override;

 private:
  // TODO maybe we should use the same hasher as in other multimap here
  typedef size_t join_key_t ;
  typedef std::pair<bool, pos_t> join_value_t;
  typedef tbb::concurrent_unordered_multimap<join_key_t, join_value_t> hashtable_t;
  typedef std::shared_ptr<hashtable_t> hashtable_ptr_t;

  // Instead of passing in the hyrise value itself,
  // all values will be hashed before. This is a hacky way of "casting" them to
  // a common datatype. (This is also in place for the storage::HashTable.)
  hashtable_ptr_t _hashtable;
};
}
}
