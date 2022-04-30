#include "benchmarks_utils.h"

namespace benchmarks_utils {

template <class T>
T v(const TargetValue& r) {
    auto scalar_r = boost::get<ScalarTargetValue>(&r);
    auto p = boost::get<T>(scalar_r);
    return *p;
}

void warmup(const std::string &tableName, ExecutorDeviceType device) {
    auto res = v<int64_t>(
      TestHelpers::ArrowSQLRunner::run_simple_agg("select count(*) from " + tableName + ";", device));
    std::cout << "Number of loaded tuples: " << res << std::endl;
}

void createTable(const std::string& tableName,
                             TableColumnsDescription& tableColumns,
                             size_t fragSize) {
    TestHelpers::ArrowSQLRunner::getStorage()->dropTable(tableName);
    ArrowStorage::TableOptions to{fragSize};
    TestHelpers::ArrowSQLRunner::createTable(tableName, tableColumns, to);
}

};