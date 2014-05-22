#include "access/DoublePipelinedHashJoin.h"
#include "storage/meta_storage.h"

namespace hyrise {
namespace access {

void DoublePipelinedHashJoin::setupPlanOperation() {
  // If this is a copied operation, hashtable will be provided
  if (_hashtable) {
    return;
  }

  //only the original operation needs to construct one hashtable
  _hashtable = std::make_shared<hashtable_t>();
}

void DoublePipelinedHashJoin::executePlanOperation() {
  const auto& input = getInputTable();
  if (!input) {
    return;
  }

  // // TODO
  // determine index of source op
  size_t f = 0;

  for (pos_t row = 0; row < input->size(); ++row) {
    //insert into hashtable
    row_hash_functor<join_key_t> fun(input.get(), f, row);
    storage::type_switch<hyrise_basic_types> ts;
    join_key_t hash = ts(input->typeOfColumn(f), fun);
    // TODO insert the source table as well!!!
    _hashtable->insert(hashtable_t::value_type(hash, row));
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
  auto instance = std::make_shared<DoublePipelinedHashJoin>();
  for (auto field : _indexed_field_definition) {
    instance->addField(field);
  }
  instance->_chunkSize = _chunkSize;
  instance->_hashtable = _hashtable;
  return instance;
}
}
}
