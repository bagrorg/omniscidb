/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "ArrowSQLRunner.h"

#include "Calcite/Calcite.h"
#include "DataMgr/DataMgr.h"
#include "DataMgr/DataMgrDataProvider.h"
#include "QueryEngine/CalciteAdapter.h"
#include "QueryEngine/RelAlgExecutor.h"
#include "QueryEngine/ThriftSerializers.h"

#include "gen-cpp/CalciteServer.h"

#include "SQLiteComparator.h"
#include "SchemaJson.h"

extern bool g_enable_columnar_output;
extern double g_gpu_mem_limit_percent;

namespace TestHelpers::ArrowSQLRunner {

bool g_hoist_literals = true;

namespace {

class ArrowSQLRunnerImpl {
 public:
  static void init() { instance_.reset(new ArrowSQLRunnerImpl); }

  static void reset() { instance_.reset(); }

  static ArrowSQLRunnerImpl* get() {
    CHECK(instance_) << "ArrowSQLRunner is not initialized";
    return instance_.get();
  }

  bool gpusPresent() { return data_mgr_->gpusPresent(); }

  void printStats() {
    std::cout << "Total schema to JSON time: " << (schema_to_json_time_ / 1000) << "ms."
              << std::endl;
    std::cout << "Total Calcite parsing time: " << (calcite_time_ / 1000) << "ms."
              << std::endl;
    std::cout << "Total execution time: " << (execution_time_ / 1000) << "ms."
              << std::endl;
  }

  void createTable(
      const std::string& table_name,
      const std::vector<ArrowStorage::ColumnDescription>& columns,
      const ArrowStorage::TableOptions& options = ArrowStorage::TableOptions()) {
    storage_->createTable(table_name, columns, options);
  }

  void dropTable(const std::string& table_name) { storage_->dropTable(table_name); }

  void insertCsvValues(const std::string& table_name, const std::string& values) {
    ArrowStorage::CsvParseOptions parse_options;
    parse_options.header = false;
    storage_->appendCsvData(values, table_name, parse_options);
  }

  void insertJsonValues(const std::string& table_name, const std::string& values) {
    storage_->appendJsonData(values, table_name);
  }

  ExecutionResult runSqlQuery(const std::string& sql,
                              const CompilationOptions& co,
                              const ExecutionOptions& eo) {
    std::string schema_json;
    std::string query_ra;
    ExecutionResult res;

    schema_to_json_time_ += measure<std::chrono::microseconds>::execution(
        [&]() { schema_json = schema_to_json(storage_); });

    calcite_time_ += measure<std::chrono::microseconds>::execution([&]() {
      query_ra =
          calcite_->process("admin", "test_db", pg_shim(sql), schema_json, "", {}, true)
              .plan_result;
    });

    execution_time_ += measure<std::chrono::microseconds>::execution([&]() {
      auto dag =
          std::make_unique<RelAlgDagBuilder>(query_ra, TEST_DB_ID, storage_, nullptr);
      auto ra_executor = RelAlgExecutor(executor_.get(),
                                        TEST_DB_ID,
                                        storage_,
                                        data_mgr_->getDataProvider(),
                                        std::move(dag));
      res = ra_executor.executeRelAlgQuery(co, eo, false, nullptr);
    });

    return res;
  }

  ExecutionResult runSqlQuery(const std::string& sql,
                              ExecutorDeviceType device_type,
                              const ExecutionOptions& eo) {
    return runSqlQuery(sql, getCompilationOptions(device_type), eo);
  }

  ExecutionResult runSqlQuery(const std::string& sql,
                              ExecutorDeviceType device_type,
                              bool allow_loop_joins) {
    return runSqlQuery(sql, device_type, getExecutionOptions(allow_loop_joins));
  }

  ExecutionOptions getExecutionOptions(bool allow_loop_joins, bool just_explain = false) {
    return {g_enable_columnar_output,
            true,
            just_explain,
            allow_loop_joins,
            false,
            false,
            false,
            false,
            10000,
            false,
            false,
            g_gpu_mem_limit_percent,
            false,
            1000};
  }

  CompilationOptions getCompilationOptions(ExecutorDeviceType device_type) {
    auto co = CompilationOptions::defaults(device_type);
    co.hoist_literals = g_hoist_literals;
    return co;
  }

  std::shared_ptr<ResultSet> run_multiple_agg(const std::string& query_str,
                                              const ExecutorDeviceType device_type,
                                              const bool allow_loop_joins = true) {
    return runSqlQuery(query_str, device_type, allow_loop_joins).getRows();
  }

  TargetValue run_simple_agg(const std::string& query_str,
                             const ExecutorDeviceType device_type,
                             const bool allow_loop_joins = true) {
    auto rows = run_multiple_agg(query_str, device_type, allow_loop_joins);
    auto crt_row = rows->getNextRow(true, true);
    CHECK_EQ(size_t(1), crt_row.size()) << query_str;
    return crt_row[0];
  }

  void run_sqlite_query(const std::string& query_string) {
    sqlite_comparator_.query(query_string);
  }

  void c(const std::string& query_string, const ExecutorDeviceType device_type) {
    sqlite_comparator_.compare(
        run_multiple_agg(query_string, device_type), query_string, device_type);
  }

  void c(const std::string& query_string,
         const std::string& sqlite_query_string,
         const ExecutorDeviceType device_type) {
    sqlite_comparator_.compare(
        run_multiple_agg(query_string, device_type), sqlite_query_string, device_type);
  }

  /* timestamp approximate checking for NOW() */
  void cta(const std::string& query_string, const ExecutorDeviceType device_type) {
    sqlite_comparator_.compare_timstamp_approx(
        run_multiple_agg(query_string, device_type), query_string, device_type);
  }

  void c_arrow(const std::string& query_string, const ExecutorDeviceType device_type) {
    auto results = run_multiple_agg(query_string, device_type);
    auto arrow_omnisci_results = result_set_arrow_loopback(nullptr, results, device_type);
    sqlite_comparator_.compare_arrow_output(
        arrow_omnisci_results, query_string, device_type);
  }

  void clearCpuMemory() {
    Executor::clearMemory(Data_Namespace::MemoryLevel::CPU_LEVEL, data_mgr_.get());
  }

  BufferPoolStats getBufferPoolStats(Data_Namespace::MemoryLevel mmeory_level) {
    return ::getBufferPoolStats(data_mgr_.get(), mmeory_level);
  }

  std::shared_ptr<ArrowStorage> getStorage() { return storage_; }

  DataMgr* getDataMgr() { return data_mgr_.get(); }

  ~ArrowSQLRunnerImpl() {
    storage_.reset();
    executor_.reset();
    data_mgr_.reset();
    calcite_.reset();
  }

 protected:
  ArrowSQLRunnerImpl() {
    storage_ = std::make_shared<ArrowStorage>(TEST_SCHEMA_ID, "test", TEST_DB_ID);

    std::unique_ptr<CudaMgr_Namespace::CudaMgr> cuda_mgr;
    bool uses_gpu = false;
#ifdef HAVE_CUDA
    cuda_mgr = std::make_unique<CudaMgr_Namespace::CudaMgr>(-1, 0);
    uses_gpu = true;
#endif

    SystemParameters system_parameters;
    data_mgr_ =
        std::make_shared<DataMgr>("", system_parameters, std::move(cuda_mgr), uses_gpu);
    auto* ps_mgr = data_mgr_->getPersistentStorageMgr();
    ps_mgr->registerDataProvider(TEST_SCHEMA_ID, storage_);

    executor_ = std::make_shared<Executor>(0,
                                           data_mgr_.get(),
                                           data_mgr_->getBufferProvider(),
                                           system_parameters.cuda_block_size,
                                           system_parameters.cuda_grid_size,
                                           system_parameters.max_gpu_slab_size,
                                           "",
                                           "");

    calcite_ = std::make_shared<Calcite>(-1, CALCITE_PORT, "", 1024, 5000, true, "");
    ExtensionFunctionsWhitelist::add(calcite_->getExtensionFunctionWhitelist());
    table_functions::TableFunctionsFactory::init();
    auto udtfs = ThriftSerializers::to_thrift(
        table_functions::TableFunctionsFactory::get_table_funcs(/*is_runtime=*/false));
    std::vector<TUserDefinedFunction> udfs = {};
    calcite_->setRuntimeExtensionFunctions(udfs, udtfs, /*is_runtime=*/false);
  }

  std::shared_ptr<DataMgr> data_mgr_;
  std::shared_ptr<ArrowStorage> storage_;
  std::shared_ptr<Executor> executor_;
  std::shared_ptr<Calcite> calcite_;
  SQLiteComparator sqlite_comparator_;
  int64_t schema_to_json_time_;
  int64_t calcite_time_;
  int64_t execution_time_;

  static std::unique_ptr<ArrowSQLRunnerImpl> instance_;
};

std::unique_ptr<ArrowSQLRunnerImpl> ArrowSQLRunnerImpl::instance_;

}  // namespace

void init() {
  ArrowSQLRunnerImpl::init();
}

void reset() {
  ArrowSQLRunnerImpl::reset();
}

bool gpusPresent() {
  return ArrowSQLRunnerImpl::get()->gpusPresent();
}

void printStats() {
  return ArrowSQLRunnerImpl::get()->printStats();
}

void createTable(const std::string& table_name,
                 const std::vector<ArrowStorage::ColumnDescription>& columns,
                 const ArrowStorage::TableOptions& options) {
  ArrowSQLRunnerImpl::get()->createTable(table_name, columns, options);
}

void dropTable(const std::string& table_name) {
  ArrowSQLRunnerImpl::get()->dropTable(table_name);
}

void insertCsvValues(const std::string& table_name, const std::string& values) {
  ArrowSQLRunnerImpl::get()->insertCsvValues(table_name, values);
}

void insertJsonValues(const std::string& table_name, const std::string& values) {
  ArrowSQLRunnerImpl::get()->insertJsonValues(table_name, values);
}

ExecutionResult runSqlQuery(const std::string& sql,
                            const CompilationOptions& co,
                            const ExecutionOptions& eo) {
  return ArrowSQLRunnerImpl::get()->runSqlQuery(sql, co, eo);
}

ExecutionResult runSqlQuery(const std::string& sql,
                            ExecutorDeviceType device_type,
                            const ExecutionOptions& eo) {
  return ArrowSQLRunnerImpl::get()->runSqlQuery(sql, device_type, eo);
}

ExecutionResult runSqlQuery(const std::string& sql,
                            ExecutorDeviceType device_type,
                            bool allow_loop_joins) {
  return ArrowSQLRunnerImpl::get()->runSqlQuery(sql, device_type, allow_loop_joins);
}

ExecutionOptions getExecutionOptions(bool allow_loop_joins, bool just_explain) {
  return ArrowSQLRunnerImpl::get()->getExecutionOptions(allow_loop_joins, just_explain);
}

CompilationOptions getCompilationOptions(ExecutorDeviceType device_type) {
  return ArrowSQLRunnerImpl::get()->getCompilationOptions(device_type);
}

std::shared_ptr<ResultSet> run_multiple_agg(const std::string& query_str,
                                            const ExecutorDeviceType device_type,
                                            const bool allow_loop_joins) {
  return ArrowSQLRunnerImpl::get()->run_multiple_agg(
      query_str, device_type, allow_loop_joins);
}

TargetValue run_simple_agg(const std::string& query_str,
                           const ExecutorDeviceType device_type,
                           const bool allow_loop_joins) {
  return ArrowSQLRunnerImpl::get()->run_simple_agg(
      query_str, device_type, allow_loop_joins);
}

void run_sqlite_query(const std::string& query_string) {
  ArrowSQLRunnerImpl::get()->run_sqlite_query(query_string);
}

void c(const std::string& query_string, const ExecutorDeviceType device_type) {
  ArrowSQLRunnerImpl::get()->c(query_string, device_type);
}

void c(const std::string& query_string,
       const std::string& sqlite_query_string,
       const ExecutorDeviceType device_type) {
  ArrowSQLRunnerImpl::get()->c(query_string, sqlite_query_string, device_type);
}

void cta(const std::string& query_string, const ExecutorDeviceType device_type) {
  ArrowSQLRunnerImpl::get()->cta(query_string, device_type);
}

void c_arrow(const std::string& query_string, const ExecutorDeviceType device_type) {
  ArrowSQLRunnerImpl::get()->c_arrow(query_string, device_type);
}

void clearCpuMemory() {
  ArrowSQLRunnerImpl::get()->clearCpuMemory();
}

BufferPoolStats getBufferPoolStats(const Data_Namespace::MemoryLevel memory_level) {
  return ArrowSQLRunnerImpl::get()->getBufferPoolStats(memory_level);
}

std::shared_ptr<ArrowStorage> getStorage() {
  return ArrowSQLRunnerImpl::get()->getStorage();
}

DataMgr* getDataMgr() {
  return ArrowSQLRunnerImpl::get()->getDataMgr();
}

}  // namespace TestHelpers::ArrowSQLRunner