#pragma once

#include "access/PipelineObserver.h"
#include "access/PipelineEmitter.h"
#include "access/system/PlanOperation.h"

#include "tbb/concurrent_unordered_map.h"
#include "tbb/concurrent_vector.h"

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
    // TODO in hashtable this is only a getValueId ^^
    return std::hash<R>()(table->getValue<R>(f, row));
  }

};

class DoublePipelinedHashJoin : public PlanOperation, public PipelineObserver<DoublePipelinedHashJoin>,
                                public PipelineEmitter<DoublePipelinedHashJoin> {


 public:
  // standard constructor will create a hashtable
  DoublePipelinedHashJoin();
  // Copy constructor will reuse the hashtable of the original operation
  // in order to have a shared data structure between all copies
  // TODO If you need a copy constructor, you also need a destructor and operator=
  // source:
  // http://www.fredosaurus.com/notes-cpp/oop-condestructors/copyconstructors.html
  DoublePipelinedHashJoin(const DoublePipelinedHashJoin& original);

  void executePlanOperation() override;

  static std::shared_ptr<PlanOperation> parse(const Json::Value& data);
  virtual std::shared_ptr<PlanOperation> copy() override;

 private:
  // TODO maybe we should use the same hasher as in other multimap here
  typedef const size_t join_key_t ;
  typedef const std::tuple<const size_t, const size_t, const storage::pos_t> join_value_t;
  typedef tbb::concurrent_unordered_multimap<join_key_t, join_value_t> hashtable_t;
  typedef std::shared_ptr<hashtable_t> hashtable_ptr_t;

  // Instead of passing in the hyrise value itself,
  // all values will be hashed before. This is a hacky way of "casting" them to
  // a common datatype. (This is also in place for the storage::HashTable.)
  hashtable_ptr_t _hashtable;

  // this is just there to manage the lifetime of chunks
  // it enables us to safely store references to shared_ptr in the hashtable
  std::shared_ptr<tbb::concurrent_vector<storage::c_atable_ptr_t>> _chunk_tables;

  // pos lists for building result table
  storage::pos_list_t* _this_rows;
  std::vector<std::pair<size_t, storage::pos_t>> _other_rows;

  void emitChunk();
  storage::atable_ptr_t buildResultTable() const;
  void resetPosLists();

};
}
}
