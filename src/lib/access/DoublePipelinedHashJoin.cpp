#include "access/DoublePipelinedHashJoin.h"
#include "storage/meta_storage.h"

namespace hyrise {
namespace access {

DoublePipelinedHashJoin::DoublePipelinedHashJoin() : PlanOperation() {
  // TODO do i need to call the super constructor?
  _hashtable = std::make_shared<hashtable_t>();
}

DoublePipelinedHashJoin::DoublePipelinedHashJoin(const DoublePipelinedHashJoin& original) : PlanOperation() {
  // TODO do i need to call the super constructor?
  _hashtable = original._hashtable;
}

void DoublePipelinedHashJoin::executePlanOperation() {
  const auto& input = getInputTable();
  if (!input) {
    return;
  }

  // determine index of source op
  // TODO maybe only set the required field on the index upon copy
  field_t f = _indexed_field_definition[_source_task_index];

  for (pos_t row = 0; row < input->size(); ++row) {
    //insert into hashtable
    row_hash_functor<join_key_t> fun(input.get(), f, row);
    storage::type_switch<hyrise_basic_types> ts;
    join_key_t hash = ts(input->typeOfColumn(f), fun);
    // TODO insert the source table as well!!!
    _hashtable->insert(hashtable_t::value_type(hash, std::pair<taskscheduler::task_ptr_t, pos_t>(_source_task, row)));
  //iterator over rows.
  //get matching_rows
  //filter by table
  //construct output table
  //TODO later emit chunks of required chunk size
  }
}

std::shared_ptr<PlanOperation> DoublePipelinedHashJoin::parse(const Json::Value& data) {
  std::shared_ptr<DoublePipelinedHashJoin> instance = std::make_shared<DoublePipelinedHashJoin>();
  if (data.isMember("fields")) {
    for (unsigned i = 0; i < data["fields"].size(); ++i) {
      instance->addField(data["fields"][i]);
    }
  }
  instance->_chunkSize = data["chunkSize"].asUInt();
  return instance;
}

std::shared_ptr<PlanOperation> DoublePipelinedHashJoin::copy(){
  auto instance = std::make_shared<DoublePipelinedHashJoin>(*this);
  for (auto field : _indexed_field_definition) {
    instance->addField(field);
  }
  instance->_chunkSize = _chunkSize;
  instance->_hashtable = _hashtable;
  return instance;
}
}
}
