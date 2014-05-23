#include "access/DoublePipelinedHashJoin.h"

#include "storage/meta_storage.h"
#include "storage/TableRangeView.h"
#include "storage/PointerCalculator.h"

#include <algorithm>

namespace hyrise {
namespace access {

DoublePipelinedHashJoin::DoublePipelinedHashJoin() : PlanOperation() {
  _hashtable = std::make_shared<hashtable_t>();
  _chunk_tables = std::make_shared<tbb::concurrent_vector<storage::c_atable_ptr_t>>();
}

DoublePipelinedHashJoin::DoublePipelinedHashJoin(const DoublePipelinedHashJoin& original) : PlanOperation() {
  _hashtable = original._hashtable;
  _chunk_tables = original._chunk_tables;
}

void DoublePipelinedHashJoin::executePlanOperation() {
  const auto& input = getInputTable();
  if (!input) {
    return;
  }

  _chunk_tables->push_back(input);

  // determine index of source op
  // TODO maybe only set the required field on the index upon copy
  field_t f = _indexed_field_definition[_source_task_index];

  storage::pos_list_t this_rows, other_rows;

  for (pos_t row = 0; row < input->size(); ++row) {
    //insert into hashtable
    row_hash_functor<join_key_t> fun(input.get(), f, row);
    storage::type_switch<hyrise_basic_types> ts;
    join_key_t hash = ts(input->typeOfColumn(f), fun);
    // Store absolute positions
    join_value_t val = std::make_tuple(_source_task.get(), input.get(), row);
    hashtable_t::value_type insert_key(hash, val);
    _hashtable->insert(insert_key);

    // TODO optimize if the other table is not yet available.

    // TODO maybe reserve full size before.
    hashtable_t::iterator all_matches_start, all_matches_end;
    std::tie(all_matches_start, all_matches_end) = _hashtable->equal_range(hash);
    // find our end
    auto matches_end = std::find(all_matches_end, all_matches_end, insert_key);

    // TODO maybe reserve full size before.
    // TODO and use standard iterator again

    std::vector<hashtable_t::value_type> matching_keys;
    // only use matching rows not in our table
    std::remove_copy_if(all_matches_start, matches_end, back_inserter(matching_keys), [this] (const hashtable_t::value_type& val) { return std::get<0>(val.second) == _source_task.get(); });

    std::vector<std::pair<const storage::AbstractTable*, storage::pos_t>> matching_tables_rows;
    std::transform(matching_keys.begin(), matching_keys.end(), back_inserter(matching_tables_rows), [](const hashtable_t::value_type& val) {return std::pair<const storage::AbstractTable*, storage::pos_t>(std::get<1>(val.second), std::get<2>(val.second));});

    //if (!matching_rows.empty()) {
      //this_rows.insert(this_rows.end(), matching_rows.size(), row);
      //other_rows.insert(other_rows.end(), matching_rows.begin(), matching_rows.end());
    //}
  }

  //if ((*_source_tables)[0] && (*_source_tables)[1]) {
    //std::vector<storage::atable_ptr_t> parts;

    //auto this_table_pc = storage::PointerCalculator::create((*_source_tables)[_source_task_index]);
    //// TODO this can be handled nicer, ey?
    //auto other_index = _source_task_index == 0 ? 1 : 0;
    //auto other_table_pc = storage::PointerCalculator::create((*_source_tables)[other_index]);

    //parts.push_back(this_table_pc);
    //parts.push_back(other_table_pc);

    //storage::atable_ptr_t result = std::make_shared<storage::MutableVerticalTable>(parts);
    //addResult(result);
  //}

  //TODO later emit chunks of required chunk size
  //
  // emits nothing if not both source tables had been available

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
  return instance;
}
}
}
