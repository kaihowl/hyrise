#include "access/DoublePipelinedHashJoin.h"

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
  // TODO implement
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
