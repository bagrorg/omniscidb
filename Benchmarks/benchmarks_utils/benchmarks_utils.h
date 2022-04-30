#pragma once
#include "Tests/ArrowSQLRunner/ArrowSQLRunner.h"

namespace benchmarks_utils {

using TableColumnsDescription = std::vector<ArrowStorage::ColumnDescription>;

template <class T>
T v(const TargetValue& r);

void warmup(const std::string &tableName, ExecutorDeviceType device);

void createTable(const std::string& tableName,
                             TableColumnsDescription& tableColumns,
                             size_t fragSize);

};