#include "access/DoublePipelinedHashJoin.h"
#include "access/system/QueryParser.h"

#include "storage/meta_storage.h"
#include "storage/storage_types.h"
#include "storage/PointerCalculator.h"
#include "storage/HorizontalTable.h"
#include "storage/MutableVerticalTable.h"

#include <algorithm>

namespace hyrise {
namespace access {

namespace {
  auto _ = QueryParser::registerPlanOperation<DoublePipelinedHashJoin>("DoublePipelinedHashJoin");
}

DoublePipelinedHashJoin::DoublePipelinedHashJoin() : PlanOperation() {
  _hashtable = std::make_shared<hashtable_t>();
  _chunk_tables = std::make_shared<tbb::concurrent_vector<storage::c_atable_ptr_t>>();
}

DoublePipelinedHashJoin::DoublePipelinedHashJoin(const DoublePipelinedHashJoin& original) : PlanOperation() {
  _hashtable = original._hashtable;
  _chunk_tables = original._chunk_tables;
}

void DoublePipelinedHashJoin::executePlanOperation() {
  size_t num_in_tables = input.numberOfTables();

  if (num_in_tables != 1) {
    return;
  }

  const auto input_table = input.getTable(0);

  _chunk_tables->push_back(input_table);

  field_t f = _indexed_field_definition[_source_index];

  resetPosLists();

  for (pos_t row = 0; row < input_table->size(); ++row) {
    //insert into hashtable
    row_hash_functor<join_key_t> fun(input_table.get(), f, row);
    storage::type_switch<hyrise_basic_types> ts;
    join_key_t hash = ts(input_table->typeOfColumn(f), fun);
    // Store absolute positions
    join_value_t val = std::make_tuple(_source_index, input_table.get(), row);
    hashtable_t::value_type insert_key(hash, val);
    _hashtable->insert(insert_key);

    // TODO optimize if the other table is not yet available.

    // TODO maybe reserve full size before.
    hashtable_t::iterator all_matches_start, all_matches_end;
    std::tie(all_matches_start, all_matches_end) = _hashtable->equal_range(hash);
    // find our end
    auto matches_end = std::find(all_matches_start, all_matches_end, insert_key);
    assert(matches_end != all_matches_end);

    // TODO maybe reserve full size before.
    // TODO and use standard iterator again

    std::vector<hashtable_t::value_type> matching_keys;
    // only use matching rows not in our table
    std::remove_copy_if(all_matches_start, matches_end, back_inserter(matching_keys), [this] (const hashtable_t::value_type& val) { return std::get<0>(val.second) == _source_index; });

    std::vector<std::pair<const storage::AbstractTable*, storage::pos_t>> matching_tables_rows;
    std::transform(matching_keys.begin(), matching_keys.end(), back_inserter(matching_tables_rows), [](const hashtable_t::value_type& val) {return std::pair<const storage::AbstractTable*, storage::pos_t>(std::get<1>(val.second), std::get<2>(val.second));});

    if (!matching_tables_rows.empty()) {
      _this_rows->insert(_this_rows->end(), matching_tables_rows.size(), row);
      _other_rows.insert(_other_rows.end(), matching_tables_rows.begin(), matching_tables_rows.end());
    }

    if (_this_rows->size() >= _chunkSize) {
      emitChunk();
    }
  }

  if (_this_rows->size()) { // one final result to produce
    emitChunk();
  }

  //TODO later emit chunks of required chunk size
  
  //emits nothing if not both source tables had been available

}

void DoublePipelinedHashJoin::emitChunk() {
  PipelineEmitter<DoublePipelinedHashJoin>::emitChunk(buildResultTable());
  resetPosLists();
}

storage::atable_ptr_t DoublePipelinedHashJoin::buildResultTable() const {

  // TODO we are getting this twice (executePlanOperation)
  const auto input_table = input.getTable(0);

  auto this_table_pc = storage::PointerCalculator::create(input_table, std::move(_this_rows));

  std::unordered_map<const storage::AbstractTable*, pos_list_t> pos_by_tables;

  for (const auto& cur_pair : _other_rows) pos_by_tables[cur_pair.first].push_back(cur_pair.second);

  // TODO let's create this table once in the parent op and do some pos_list
  // magic for a pointer calculator on it.
  std::vector<storage::c_atable_ptr_t> horizontal_parts;

  for (const auto& table_pos_list_pair : pos_by_tables) {
    const auto& table = table_pos_list_pair.first;
    // TODO store the iterator instead of the table pointer.
    const auto& shared_table = std::find_if(_chunk_tables->begin(), _chunk_tables->end(), [&table](const storage::c_atable_ptr_t& val) { return val.get() == table; });
    const auto& pos_list = table_pos_list_pair.second;
    // TODO maybe move is wrong here since we did not create the object
    auto part = storage::PointerCalculator::create(*shared_table, std::move(pos_list));
    horizontal_parts.push_back(part);
  }

  auto other_table_ht = std::make_shared<storage::HorizontalTable>(horizontal_parts);

  std::vector<storage::atable_ptr_t> parts;

  if (_source_index == 0) {
    parts.push_back(this_table_pc);
    parts.push_back(other_table_ht);
  } else {
    parts.push_back(other_table_ht);
    parts.push_back(this_table_pc);
  }

  storage::atable_ptr_t result = std::make_shared<storage::MutableVerticalTable>(parts);
  return result;
}

void DoublePipelinedHashJoin::resetPosLists() {
  _this_rows = new pos_list_t;
  _other_rows.clear();
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
