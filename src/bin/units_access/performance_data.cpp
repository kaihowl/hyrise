// Copyright (c) 2012 Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH. All rights reserved.
#include "testing/test.h"
#include "io/shortcuts.h"

#include "io/shortcuts.h"
#include "access/ProjectionScan.h"
#include "access/Distinct.h"
#include "access/Barrier.h"
#include "optional.hpp"

namespace hyrise {
namespace access {

class PerformanceDataTests : public AccessTest {};

TEST_F(PerformanceDataTests, single_op_data) {
  storage::atable_ptr_t w = io::Loader::shortcuts::loadWithHeader("test/regression/projection_fail.data",
                                                                  "test/regression/projection_fail.tbl");

  ProjectionScan ps;
  ps.addInput(w);
  ps.addField(w->numberOfColumn("w_tax"));

  performance_attributes_t perf;
  perf.startTime = 0;
  perf.endTime = 0;
  ps.setPerformanceData(&perf);

  ps.execute();

  ASSERT_GT(perf.startTime, 0u) << "start time should be set";
  ASSERT_GT(perf.endTime, 0u) << "end time should be set";
}

TEST_F(PerformanceDataTests, rows_test) {
  auto in = io::Loader::shortcuts::load("test/tables/employees.tbl");

  Distinct d;
  d.addInput(in);
  d.addField(in->numberOfColumn("employee_company_id"));

  performance_attributes_t perf;
  d.setPerformanceData(&perf);

  d.execute();

  auto out = d.getResultTable();

  ASSERT_EQ(*(perf.in_rows), in->size());
  ASSERT_EQ(*(perf.out_rows), out->size());
}

TEST_F(PerformanceDataTests, no_rows_test) {
  Barrier b;

  performance_attributes_t perf;
  b.setPerformanceData(&perf);

  b.execute();

  ASSERT_EQ(perf.in_rows, std::nullopt);
  ASSERT_EQ(perf.out_rows, std::nullopt);
}


}
}
