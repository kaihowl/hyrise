#include "access/DoublePipelinedHashJoin.h"
#include "storage/meta_storage.h"
#include <algorithm>

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

  storage::pos_list_t this_rows, other_rows;

  for (pos_t row = 0; row < input->size(); ++row) {
    //insert into hashtable
    row_hash_functor<join_key_t> fun(input.get(), f, row);
    storage::type_switch<hyrise_basic_types> ts;
    join_key_t hash = ts(input->typeOfColumn(f), fun);
    join_value_t val = std::pair<taskscheduler::task_ptr_t, pos_t>(_source_task, row);
    hashtable_t::value_type insert_key(hash, val);
    _hashtable->insert(insert_key);

    // TODO maybe reserve full size before.
    hashtable_t::iterator all_matches_start, all_matches_end;
    std::tie(all_matches_start, all_matches_end) = _hashtable->equal_range(hash);
    // find our end
    auto matches_end = std::find(all_matches_end, all_matches_end, insert_key);
    // TODO maybe reserve full size before.
    // TODO and use standard iterator again
    std::vector<hashtable_t::value_type> matching_keys;
    pos_list_t matching_rows;
    // only use matching rows not in our table
    std::remove_copy_if(all_matches_start, matches_end, back_inserter(matching_keys), [this] (const hashtable_t::value_type& val) { return val.second.first == _source_task; });
    std::transform(matching_keys.begin(), matching_keys.end(), back_inserter(matching_rows), [](const hashtable_t::value_type& val) {return val.second.second;});

    if (!matching_rows.empty()) {
      this_rows.insert(this_rows.end(), matching_rows.size(), row);
      other_rows.insert(other_rows.end(), matching_rows.begin(), matching_rows.end());
    }
  }
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
