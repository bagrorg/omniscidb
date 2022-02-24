/*
 * Copyright 2021, OmniSci, Inc.
 *
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

#include "TestHelpers.h"

#include "Logger/Logger.h"
#include "QueryEngine/CompilationOptions.h"
#include "QueryEngine/Execute.h"
#include "QueryEngine/JoinHashTable/PerfectJoinHashTable.h"
#include "QueryEngine/QueryPlanDagCache.h"
#include "QueryEngine/QueryPlanDagExtractor.h"
#include "QueryRunner/QueryRunner.h"

#include <gtest/gtest.h>
#include <boost/algorithm/string/join.hpp>

#include <exception>
#include <future>
#include <stdexcept>

extern bool g_is_test_env;
extern unsigned g_trivial_loop_join_threshold;
extern bool g_enable_overlaps_hashjoin;
extern bool g_enable_hashjoin_many_to_many;

using QR = QueryRunner::QueryRunner;
using namespace TestHelpers;

#ifndef BASE_PATH
#define BASE_PATH "./tmp"
#endif

bool g_cpu_only{false};

bool skip_tests(const ExecutorDeviceType device_type) {
#ifdef HAVE_CUDA
  return device_type == ExecutorDeviceType::GPU && !(QR::get()->gpusPresent());
#else
  return device_type == ExecutorDeviceType::GPU;
#endif
}

namespace {

inline void run_ddl_statement(const std::string& create_table_stmt) {
  QR::get()->runDDLStatement(create_table_stmt);
}

TargetValue run_simple_query(const std::string& query_str,
                             const ExecutorDeviceType device_type,
                             const bool allow_loop_joins = true) {
  auto rows = QR::get()->runSQL(query_str, device_type, allow_loop_joins);
  auto crt_row = rows->getNextRow(true, true);
  CHECK_EQ(size_t(1), crt_row.size()) << query_str;
  return crt_row[0];
}

int drop_table() {
  try {
    run_ddl_statement("DROP TABLE IF EXISTS T1;");
    run_ddl_statement("DROP TABLE IF EXISTS T2;");
    run_ddl_statement("DROP TABLE IF EXISTS T3;");
    run_ddl_statement("DROP TABLE IF EXISTS T4;");
  } catch (...) {
    LOG(ERROR) << "Failed to drop table";
    return -1;
  }
  return 0;
}

int create_and_populate_table() {
  try {
    drop_table();
    const auto table_ddl = "(x int, y int, z text encoding dict);";
    auto create_table_ddl = [&table_ddl](const std::string& tbl_name) {
      return "CREATE TABLE " + tbl_name + table_ddl;
    };
    run_ddl_statement(create_table_ddl("T1"));
    run_ddl_statement(create_table_ddl("T2"));
    run_ddl_statement(create_table_ddl("T3"));
    run_ddl_statement(create_table_ddl("T4"));
    const auto data_insertion = [](const std::string& tbl_name) {
      auto insert_dml = "INSERT INTO " + tbl_name + " VALUES(";
      std::vector<std::string> value_vec = {"1, 1, '1'", "2, 1, '2'", "3, 1, '3'"};
      for (auto& v : value_vec) {
        QR::get()->runSQL(insert_dml + v + ");", ExecutorDeviceType::CPU);
      }
    };
    data_insertion("T1");
    data_insertion("T2");
    QR::get()->runSQL("INSERT INTO T2 VALUES(4,2,'4');", ExecutorDeviceType::CPU);
    data_insertion("T3");
    QR::get()->runSQL("INSERT INTO T3 VALUES(4,2,'4');", ExecutorDeviceType::CPU);
    QR::get()->runSQL("INSERT INTO T3 VALUES(5,2,'5');", ExecutorDeviceType::CPU);
    data_insertion("T4");
    QR::get()->runSQL("INSERT INTO T4 VALUES(4,2,'4');", ExecutorDeviceType::CPU);
    QR::get()->runSQL("INSERT INTO T4 VALUES(5,2,'5');", ExecutorDeviceType::CPU);
    QR::get()->runSQL("INSERT INTO T4 VALUES(6,2,'6');", ExecutorDeviceType::CPU);
  } catch (...) {
    LOG(ERROR) << "Failed to (re-)create table";
    return -1;
  }
  return 0;
}

std::shared_ptr<CacheItemMetric> getCachedHashTableMetric(
    std::set<QueryPlanHash>& already_visited,
    CacheItemType cache_item_type) {
  auto cached_ht = QR::get()->getCachedHashtableWithoutCacheKey(
      already_visited, cache_item_type, DataRecyclerUtil::CPU_DEVICE_IDENTIFIER);
  auto cache_key = std::get<0>(cached_ht);
  already_visited.insert(cache_key);
  return QR::get()->getCacheItemMetric(
      cache_key, cache_item_type, DataRecyclerUtil::CPU_DEVICE_IDENTIFIER);
}

struct OverlapsCachedHTAndMetaInfo {
  QueryPlanHash key;
  std::shared_ptr<HashTable> cached_ht;
  std::optional<HashtableCacheMetaInfo> cached_ht_metainfo;
  std::shared_ptr<CacheItemMetric> cached_metric;
  std::optional<AutoTunerMetaInfo> cached_tuning_info;
};

}  // namespace

TEST(DataRecycler, QueryPlanDagExtractor_Simple_Project_Query) {
  auto executor = Executor::getExecutor(Executor::UNITARY_EXECUTOR_ID,
                                        &QR::get()->getCatalog()->getDataMgr())
                      .get();
  auto q1_str = "SELECT x FROM T1 ORDER BY x;";
  auto q1_query_info = QR::get()->getQueryInfoForDataRecyclerTest(q1_str);
  ASSERT_TRUE(q1_query_info.left_deep_trees_id.empty());
  auto q1_rel_alg_translator = QR::get()->getRelAlgTranslator(q1_str, executor);
  auto q1_plan_dag =
      QueryPlanDagExtractor::extractQueryPlanDag(q1_query_info.root_node.get(),
                                                 executor->getSchemaProvider(),
                                                 std::nullopt,
                                                 q1_query_info.left_deep_trees_info,
                                                 {},
                                                 executor,
                                                 *q1_rel_alg_translator);
  // 1. a sort node becomes a root (dag_rel_id = 0)
  // 2. a project node becomes a child of the sort node (dag_rel_id = 1)
  // 3. a scan node (the leaf of the query plan) becomes a child of the project node
  ASSERT_TRUE(q1_plan_dag.extracted_dag.compare("0|1|2|") == 0);

  auto q2_str = "SELECT x FROM T1;";
  auto q2_query_info = QR::get()->getQueryInfoForDataRecyclerTest(q2_str);
  ASSERT_TRUE(q2_query_info.left_deep_trees_id.empty());
  auto q2_rel_alg_translator = QR::get()->getRelAlgTranslator(q2_str, executor);
  auto q2_plan_dag =
      QueryPlanDagExtractor::extractQueryPlanDag(q2_query_info.root_node.get(),
                                                 executor->getSchemaProvider(),
                                                 std::nullopt,
                                                 q2_query_info.left_deep_trees_info,
                                                 {},
                                                 executor,
                                                 *q2_rel_alg_translator);
  // q2 is the same as q1 except sort node
  ASSERT_TRUE(q2_plan_dag.extracted_dag.compare("1|2|") == 0);

  auto q3_str = "SELECT x FROM T1 GROUP BY x;";
  auto q3_query_info = QR::get()->getQueryInfoForDataRecyclerTest(q3_str);
  ASSERT_TRUE(q3_query_info.left_deep_trees_id.empty());
  auto q3_rel_alg_translator = QR::get()->getRelAlgTranslator(q3_str, executor);
  auto q3_plan_dag =
      QueryPlanDagExtractor::extractQueryPlanDag(q3_query_info.root_node.get(),
                                                 executor->getSchemaProvider(),
                                                 std::nullopt,
                                                 q3_query_info.left_deep_trees_info,
                                                 {},
                                                 executor,
                                                 *q3_rel_alg_translator);
  // compound node becomes the root (dag_rel_id = 3), and the scan node
  // (that is the same node as both q1 and q2) is the leaf of the query plan
  ASSERT_TRUE(q3_plan_dag.extracted_dag.compare("3|2|") == 0);

  auto q4_str = "SELECT x FROM T1 GROUP BY x ORDER BY x;";
  auto q4_query_info = QR::get()->getQueryInfoForDataRecyclerTest(q4_str);
  ASSERT_TRUE(q4_query_info.left_deep_trees_id.empty());
  auto q4_rel_alg_translator = QR::get()->getRelAlgTranslator(q4_str, executor);
  auto q4_plan_dag =
      QueryPlanDagExtractor::extractQueryPlanDag(q4_query_info.root_node.get(),
                                                 executor->getSchemaProvider(),
                                                 std::nullopt,
                                                 q4_query_info.left_deep_trees_info,
                                                 {},
                                                 executor,
                                                 *q4_rel_alg_translator);
  // this sort node has different input compared with that of q1
  // so we assign the new dag_rel_id (4) to the sort node
  ASSERT_TRUE(q4_plan_dag.extracted_dag.compare("4|3|2|") == 0);

  auto q1_dup_plan_dag =
      QueryPlanDagExtractor::extractQueryPlanDag(q1_query_info.root_node.get(),
                                                 executor->getSchemaProvider(),
                                                 std::nullopt,
                                                 q1_query_info.left_deep_trees_info,
                                                 {},
                                                 executor,
                                                 *q1_rel_alg_translator);
  ASSERT_TRUE(q1_dup_plan_dag.extracted_dag.compare("0|1|2|") == 0);

  auto q4_dup_plan_dag =
      QueryPlanDagExtractor::extractQueryPlanDag(q4_query_info.root_node.get(),
                                                 executor->getSchemaProvider(),
                                                 std::nullopt,
                                                 q4_query_info.left_deep_trees_info,
                                                 {},
                                                 executor,
                                                 *q4_rel_alg_translator);
  ASSERT_TRUE(q4_dup_plan_dag.extracted_dag.compare("4|3|2|") == 0);
}

TEST(DataRecycler, QueryPlanDagExtractor_Heavy_IN_clause) {
  // we do not extract query plan dag where at least one rel node
  // containing a heavy IN-expr w.r.t its value list, i.e., |value list| > 20
  auto executor = Executor::getExecutor(Executor::UNITARY_EXECUTOR_ID,
                                        &QR::get()->getCatalog()->getDataMgr())
                      .get();

  auto create_query_having_IN_expr = [](const std::string tbl_name,
                                        const std::string agg_col_name,
                                        const int value_list_size) {
    // assume value_list_size > 0
    std::vector<std::string> value_list;
    for (int i = 0; i < value_list_size; i++) {
      value_list.push_back(std::to_string(i));
    }
    return "SELECT COUNT(" + agg_col_name + ") FROM " + tbl_name + " WHERE " +
           agg_col_name + " IN (" + boost::algorithm::join(value_list, ",") + ");";
  };

  // so we can extract q1's dag
  auto q1_str = create_query_having_IN_expr("T1", "x", 20);
  auto q1_query_info = QR::get()->getQueryInfoForDataRecyclerTest(q1_str);
  ASSERT_TRUE(q1_query_info.left_deep_trees_id.empty());
  auto rel_alg_translator_for_q1 = QR::get()->getRelAlgTranslator(q1_str, executor);
  auto q1_plan_dag =
      QueryPlanDagExtractor::extractQueryPlanDag(q1_query_info.root_node.get(),
                                                 executor->getSchemaProvider(),
                                                 std::nullopt,
                                                 q1_query_info.left_deep_trees_info,
                                                 {},
                                                 executor,
                                                 *rel_alg_translator_for_q1);
  ASSERT_EQ(q1_plan_dag.contain_not_supported_rel_node, false);
  // but we skip to extract a DAG for q2 since it contains IN-expr having 21 elems in its
  // value list

  auto q2_str = create_query_having_IN_expr("T1", "x", 21);
  auto q2_query_info = QR::get()->getQueryInfoForDataRecyclerTest(q2_str);
  ASSERT_TRUE(q2_query_info.left_deep_trees_id.empty());
  auto rel_alg_translator_for_q2 = QR::get()->getRelAlgTranslator(q2_str, executor);
  auto q2_plan_dag =
      QueryPlanDagExtractor::extractQueryPlanDag(q2_query_info.root_node.get(),
                                                 executor->getSchemaProvider(),
                                                 std::nullopt,
                                                 q2_query_info.left_deep_trees_info,
                                                 {},
                                                 executor,
                                                 *rel_alg_translator_for_q2);
  ASSERT_EQ(q2_plan_dag.contain_not_supported_rel_node, true);
}

TEST(DataRecycler, QueryPlanDagExtractor_Join_Query) {
  auto executor = Executor::getExecutor(Executor::UNITARY_EXECUTOR_ID,
                                        &QR::get()->getCatalog()->getDataMgr())
                      .get();

  auto q1_str = "SELECT T1.x FROM T1, T2 WHERE T1.x = T2.x;";
  auto q1_query_info = QR::get()->getQueryInfoForDataRecyclerTest(q1_str);
  ASSERT_TRUE(q1_query_info.left_deep_trees_id.size() == 1);
  auto q1_rel_alg_translator = QR::get()->getRelAlgTranslator(q1_str, executor);
  auto q1_plan_dag =
      QueryPlanDagExtractor::extractQueryPlanDag(q1_query_info.root_node.get(),
                                                 executor->getSchemaProvider(),
                                                 q1_query_info.left_deep_trees_id[0],
                                                 q1_query_info.left_deep_trees_info,
                                                 {},
                                                 executor,
                                                 *q1_rel_alg_translator);

  auto q2_str = "SELECT T1.x FROM T1 JOIN T2 ON T1.x = T2.x;";
  auto q2_query_info = QR::get()->getQueryInfoForDataRecyclerTest(q2_str);
  ASSERT_TRUE(q2_query_info.left_deep_trees_id.size() == 1);
  auto q2_rel_alg_translator = QR::get()->getRelAlgTranslator(q2_str, executor);
  auto q2_plan_dag =
      QueryPlanDagExtractor::extractQueryPlanDag(q2_query_info.root_node.get(),
                                                 executor->getSchemaProvider(),
                                                 q2_query_info.left_deep_trees_id[0],
                                                 q2_query_info.left_deep_trees_info,
                                                 {},
                                                 executor,
                                                 *q2_rel_alg_translator);

  ASSERT_TRUE(q1_plan_dag.extracted_dag.compare(q2_plan_dag.extracted_dag) == 0);

  auto q3_str = "SELECT T1.x FROM T1, T2 WHERE T1.x = T2.x and T2.y = T1.y;";
  auto q3_query_info = QR::get()->getQueryInfoForDataRecyclerTest(q3_str);
  ASSERT_TRUE(q3_query_info.left_deep_trees_id.size() == 1);
  auto q3_rel_alg_translator = QR::get()->getRelAlgTranslator(q3_str, executor);
  auto q3_plan_dag =
      QueryPlanDagExtractor::extractQueryPlanDag(q3_query_info.root_node.get(),
                                                 executor->getSchemaProvider(),
                                                 q3_query_info.left_deep_trees_id[0],
                                                 q3_query_info.left_deep_trees_info,
                                                 {},
                                                 executor,
                                                 *q3_rel_alg_translator);

  auto q4_str = "SELECT T1.x FROM T1 JOIN T2 ON T1.x = T2.x and T1.y = T2.y;";
  auto q4_query_info = QR::get()->getQueryInfoForDataRecyclerTest(q4_str);
  ASSERT_TRUE(q4_query_info.left_deep_trees_id.size() == 1);
  auto q4_rel_alg_translator = QR::get()->getRelAlgTranslator(q4_str, executor);
  auto q4_plan_dag =
      QueryPlanDagExtractor::extractQueryPlanDag(q4_query_info.root_node.get(),
                                                 executor->getSchemaProvider(),
                                                 q4_query_info.left_deep_trees_id[0],
                                                 q4_query_info.left_deep_trees_info,
                                                 {},
                                                 executor,
                                                 *q4_rel_alg_translator);

  ASSERT_TRUE(q3_plan_dag.extracted_dag.compare(q4_plan_dag.extracted_dag) != 0);

  auto q5_str = "SELECT T1.x FROM T1 JOIN T2 ON T1.y = T2.y and T1.x = T2.x;";
  auto q5_query_info = QR::get()->getQueryInfoForDataRecyclerTest(q5_str);
  ASSERT_TRUE(q5_query_info.left_deep_trees_id.size() == 1);
  auto q5_rel_alg_translator = QR::get()->getRelAlgTranslator(q5_str, executor);
  auto q5_plan_dag =
      QueryPlanDagExtractor::extractQueryPlanDag(q5_query_info.root_node.get(),
                                                 executor->getSchemaProvider(),
                                                 q5_query_info.left_deep_trees_id[0],
                                                 q5_query_info.left_deep_trees_info,
                                                 {},
                                                 executor,
                                                 *q5_rel_alg_translator);
  ASSERT_TRUE(q3_plan_dag.extracted_dag.compare(q5_plan_dag.extracted_dag) != 0);
}

TEST(DataRecycler, DAG_Cache_Size_Management) {
  // test if DAG cache becomes full
  auto executor = Executor::getExecutor(Executor::UNITARY_EXECUTOR_ID,
                                        &QR::get()->getCatalog()->getDataMgr())
                      .get();
  // get query info for DAG cache test in advance
  auto& DAG_CACHE = executor->getQueryPlanDagCache();

  auto original_DAG_cache_max_size = MAX_NODE_CACHE_SIZE;
  ScopeGuard reset_overlaps_state = [&original_DAG_cache_max_size, &DAG_CACHE] {
    DAG_CACHE.setNodeMapMaxSize(original_DAG_cache_max_size);
  };

  auto q1_str = "SELECT x FROM T1 ORDER BY x;";
  auto q2_str = "SELECT y FROM T1;";
  auto q3_str =
      "SELECT T2.y, COUNT(T1.x) FROM T1, T2 WHERE T1.y = T2.y and T1.x = T2.x GROUP BY "
      "T2.y;";
  auto q1_query_info = QR::get()->getQueryInfoForDataRecyclerTest(q1_str);
  auto q2_query_info = QR::get()->getQueryInfoForDataRecyclerTest(q2_str);
  auto q3_query_info = QR::get()->getQueryInfoForDataRecyclerTest(q3_str);
  DAG_CACHE.clearQueryPlanCache();

  // test: when DAG cache becomes full, it should skip the following query and clear the
  // cached plan
  DAG_CACHE.setNodeMapMaxSize(48);
  auto q1_rel_alg_translator = QR::get()->getRelAlgTranslator(q1_str, executor);
  auto q1_plan_dag =
      QueryPlanDagExtractor::extractQueryPlanDag(q1_query_info.root_node.get(),
                                                 executor->getSchemaProvider(),
                                                 std::nullopt,
                                                 q1_query_info.left_deep_trees_info,
                                                 {},
                                                 executor,
                                                 *q1_rel_alg_translator);
  // 1. a sort node becomes a root (dag_rel_id = 0)
  // 2. a project node becomes a child of the sort node (dag_rel_id = 1)
  // 3. a scan node (the leaf of the query plan) becomes a child of the project node
  ASSERT_TRUE(q1_plan_dag.extracted_dag.compare("0|1|2|") == 0);
  // 3 unique REL nodes in the cache --> 3 * 2 * 8 = 48
  ASSERT_EQ(DAG_CACHE.getCurrentNodeMapSize(), 48u);
  auto q2_rel_alg_translator = QR::get()->getRelAlgTranslator(q2_str, executor);
  auto q2_plan_dag =
      QueryPlanDagExtractor::extractQueryPlanDag(q2_query_info.root_node.get(),
                                                 executor->getSchemaProvider(),
                                                 std::nullopt,
                                                 q2_query_info.left_deep_trees_info,
                                                 {},
                                                 executor,
                                                 *q2_rel_alg_translator);
  // we set the DAG cache size be 48, so when we try to cache the q2, it becomes full
  // so it skips to extract DAG plan of this query and also clear the cache itself
  ASSERT_TRUE(q2_plan_dag.extracted_dag.compare("") == 0);
  // 2 unique REL nodes in the cache --> 2 * 2 * 8 = 32
  ASSERT_EQ(DAG_CACHE.getCurrentNodeMapSize(), 0u);
  DAG_CACHE.clearQueryPlanCache();

  // test: when a query size is too large, we skip caching the query
  auto q3_rel_alg_translator = QR::get()->getRelAlgTranslator(q3_str, executor);
  auto q3_plan_dag =
      QueryPlanDagExtractor::extractQueryPlanDag(q3_query_info.root_node.get(),
                                                 executor->getSchemaProvider(),
                                                 q3_query_info.left_deep_trees_id[0],
                                                 q3_query_info.left_deep_trees_info,
                                                 {},
                                                 executor,
                                                 *q3_rel_alg_translator);
  // q3 has more than three nodes, so its size is beyond the limit of the DAG cache (48)
  // so we cannot keep it to our DAG cache
  ASSERT_EQ(DAG_CACHE.getCurrentNodeMapSize(), 0u);

  // test: increase the cache size that is enough to hold both q1 and q2
  DAG_CACHE.setNodeMapMaxSize(80);
  auto new_q1_plan_dag =
      QueryPlanDagExtractor::extractQueryPlanDag(q1_query_info.root_node.get(),
                                                 executor->getSchemaProvider(),
                                                 std::nullopt,
                                                 q1_query_info.left_deep_trees_info,
                                                 {},
                                                 executor,
                                                 *q1_rel_alg_translator);
  ASSERT_TRUE(new_q1_plan_dag.extracted_dag.compare("0|1|2|") == 0);
  auto new_q2_plan_dag =
      QueryPlanDagExtractor::extractQueryPlanDag(q2_query_info.root_node.get(),
                                                 executor->getSchemaProvider(),
                                                 std::nullopt,
                                                 q2_query_info.left_deep_trees_info,
                                                 {},
                                                 executor,
                                                 *q2_rel_alg_translator);
  ASSERT_TRUE(new_q2_plan_dag.extracted_dag.compare("3|2|") == 0);
  ASSERT_GE(DAG_CACHE.getCurrentNodeMapSize(), 48u);
}

TEST(DataRecycler, Perfect_Hashtable_Cache_Maintanence) {
  auto data_mgr = &QR::get()->getCatalog()->getDataMgr();
  auto executor = Executor::getExecutor(Executor::UNITARY_EXECUTOR_ID, data_mgr).get();
  std::set<QueryPlanHash> visited_hashtable_key;
  auto clearCaches = [&executor, data_mgr, &visited_hashtable_key] {
    Executor::clearMemory(MemoryLevel::CPU_LEVEL, data_mgr);
    executor->getQueryPlanDagCache().clearQueryPlanCache();
    visited_hashtable_key.clear();
  };

  // hashtables for t1, t2, t3 and t4
  // column x: unique key, like t1: {1,2,3} and t4: {1,2,3,4,5,6}
  // column y: dup. key, like t1: {1,1,1} and t2: {1,1,1,2,2,2}
  // query on a single col: deal with perfect hashtable
  // query on both cols x and y: deal with baseline hashtable
  // hashtable size info
  // t1.x = 12 / t1.y = 20 / t1.x and t1.y = 72
  // t2.x = 16 / t2.y = 32 / t2.x and t2.y = 96
  // t3.x = 20 / t3.y = 36 / t3.x and t3.y = 120
  // t4.x = 24 / t4.y = 40 / t4.x and t4.y = 144

  for (auto dt : {ExecutorDeviceType::CPU}) {
    {
      // test1. cache hashtable of t1.x and then reuse it correctly?
      clearCaches();
      auto q1 = "SELECT count(*) from t1, t2 where t1.x = t2.x;";
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q1, dt)));
      ASSERT_EQ(static_cast<size_t>(1), QR::get()->getNumberOfCachedPerfectHashTables());
      auto q1_perfect_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::PERFECT_HT);
      auto ht1_ref_count_v1 = q1_perfect_ht_metrics->getRefCount();
      ASSERT_EQ(static_cast<size_t>(1), ht1_ref_count_v1);
      ASSERT_EQ(static_cast<size_t>(12), q1_perfect_ht_metrics->getMemSize());
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q1, dt)));
      ASSERT_EQ(static_cast<size_t>(1), QR::get()->getNumberOfCachedPerfectHashTables());
      auto ht1_ref_count_v2 = q1_perfect_ht_metrics->getRefCount();
      ASSERT_LT(ht1_ref_count_v1, ht1_ref_count_v2);
      auto q2 = "SELECT count(*) from t1, t3 where t1.x = t3.x;";
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q2, dt)));
      ASSERT_EQ(static_cast<size_t>(1), QR::get()->getNumberOfCachedPerfectHashTables());
      auto ht1_ref_count_v3 = q1_perfect_ht_metrics->getRefCount();
      ASSERT_LT(ht1_ref_count_v2, ht1_ref_count_v3);
    }

    {
      // test2. cache t1.x and t1.y
      clearCaches();

      auto q1 = "SELECT count(*) from t1, t2 where t1.x = t2.x;";
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q1, dt)));
      ASSERT_EQ(static_cast<size_t>(1), QR::get()->getNumberOfCachedPerfectHashTables());
      auto q1_perfect_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::PERFECT_HT);
      ASSERT_EQ(static_cast<size_t>(1), q1_perfect_ht_metrics->getRefCount());
      ASSERT_EQ(static_cast<size_t>(12), q1_perfect_ht_metrics->getMemSize());
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q1, dt)));
      ASSERT_EQ(static_cast<size_t>(1), QR::get()->getNumberOfCachedPerfectHashTables());

      auto q2 = "SELECT count(*) from t1, t2 where t1.y = t2.y;";
      ASSERT_EQ(static_cast<int64_t>(9), v<int64_t>(run_simple_query(q2, dt)));
      ASSERT_EQ(static_cast<size_t>(2), QR::get()->getNumberOfCachedPerfectHashTables());
      auto q2_perfect_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::PERFECT_HT);
      ASSERT_EQ(static_cast<size_t>(1), q2_perfect_ht_metrics->getRefCount());
      ASSERT_EQ(static_cast<size_t>(20), q2_perfect_ht_metrics->getMemSize());
    }

    {
      // test3. set hashtable cache size as 30 bytes,
      // and try to cache t1.x's hashtable (12 bytes) and then that of t4.x's (24 bytes)
      // since sizeof(t1.x) + sizeof(t4.x) > 30 we need to remove t1.x's to cache t4.x's
      const auto original_total_cache_size = g_hashtable_cache_total_bytes;
      PerfectJoinHashTable::getHashTableCache()->setTotalCacheSize(
          CacheItemType::PERFECT_HT, 30);
      ScopeGuard reset_cache_status = [&original_total_cache_size] {
        PerfectJoinHashTable::getHashTableCache()->setTotalCacheSize(
            CacheItemType::PERFECT_HT, original_total_cache_size);
      };
      clearCaches();

      auto q1 = "SELECT count(*) from t1, t2 where t1.x = t2.x;";
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q1, dt)));
      ASSERT_EQ(static_cast<size_t>(1), QR::get()->getNumberOfCachedPerfectHashTables());
      auto q1_perfect_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::PERFECT_HT);
      ASSERT_EQ(static_cast<size_t>(1), q1_perfect_ht_metrics->getRefCount());
      ASSERT_EQ(static_cast<size_t>(12), q1_perfect_ht_metrics->getMemSize());

      auto q2 = "SELECT count(*) from t4 a, t4 b where a.x = b.x;";
      ASSERT_EQ(static_cast<int64_t>(6), v<int64_t>(run_simple_query(q2, dt)));
      ASSERT_EQ(static_cast<size_t>(1), QR::get()->getNumberOfCachedPerfectHashTables());
      auto q2_perfect_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::PERFECT_HT);
      ASSERT_EQ(static_cast<size_t>(1), q2_perfect_ht_metrics->getRefCount());
      ASSERT_EQ(static_cast<size_t>(24), q2_perfect_ht_metrics->getMemSize());
      auto current_cache_size =
          PerfectJoinHashTable::getHashTableCache()->getCurrentCacheSizeForDevice(
              CacheItemType::PERFECT_HT, DataRecyclerUtil::CPU_DEVICE_IDENTIFIER);
      ASSERT_EQ(static_cast<size_t>(24), current_cache_size);
    }

    {
      // test4. set hashtable cache size as 30 bytes, and
      // cache t1.x and t2.x (so total 14 * 2 = 28 bytes) and then try to cache t4.x's
      // and check whether cache only has t4.x's (remove t1.x's and t2.x's to make a room
      // for t4.x's)
      const auto original_total_cache_size = g_hashtable_cache_total_bytes;
      PerfectJoinHashTable::getHashTableCache()->setTotalCacheSize(
          CacheItemType::PERFECT_HT, 30);
      ScopeGuard reset_cache_status = [&original_total_cache_size] {
        PerfectJoinHashTable::getHashTableCache()->setTotalCacheSize(
            CacheItemType::PERFECT_HT, original_total_cache_size);
      };
      clearCaches();

      auto q1 = "SELECT count(*) from t1, t2 where t1.x = t2.x;";
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q1, dt)));
      ASSERT_EQ(static_cast<size_t>(1), QR::get()->getNumberOfCachedPerfectHashTables());
      auto q1_perfect_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::PERFECT_HT);
      ASSERT_EQ(static_cast<size_t>(1), q1_perfect_ht_metrics->getRefCount());
      ASSERT_EQ(static_cast<size_t>(12), q1_perfect_ht_metrics->getMemSize());

      auto q2 = "SELECT count(*) from t2 a, t2 b where a.x = b.x;";
      ASSERT_EQ(static_cast<int64_t>(4), v<int64_t>(run_simple_query(q2, dt)));
      ASSERT_EQ(static_cast<size_t>(2), QR::get()->getNumberOfCachedPerfectHashTables());
      auto q2_perfect_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::PERFECT_HT);

      auto q3 = "SELECT count(*) from t4 a, t4 b where a.x = b.x;";
      ASSERT_EQ(static_cast<int64_t>(6), v<int64_t>(run_simple_query(q3, dt)));
      ASSERT_EQ(static_cast<size_t>(1), QR::get()->getNumberOfCachedPerfectHashTables());
      auto q3_perfect_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::PERFECT_HT);
      ASSERT_EQ(static_cast<size_t>(24), q3_perfect_ht_metrics->getMemSize());
      ASSERT_EQ(static_cast<size_t>(1), q3_perfect_ht_metrics->getRefCount());
      auto current_cache_size =
          PerfectJoinHashTable::getHashTableCache()->getCurrentCacheSizeForDevice(
              CacheItemType::PERFECT_HT, DataRecyclerUtil::CPU_DEVICE_IDENTIFIER);
      ASSERT_EQ(static_cast<size_t>(24), current_cache_size);
    }

    {
      // test5. set hashtable cache size as 40 bytes, and
      // we try to cache t1.x, t2.x and t3.x's hashtables
      // here we make t1.x's to be more frequently reused one than t2.x's
      // and try to cache t3.x's.
      // if our cache maintenance works correctly, we should remove t2.x's since it is
      // less frequently reused one
      const auto original_total_cache_size = g_hashtable_cache_total_bytes;
      PerfectJoinHashTable::getHashTableCache()->setTotalCacheSize(
          CacheItemType::PERFECT_HT, 40);
      ScopeGuard reset_cache_status = [&original_total_cache_size] {
        PerfectJoinHashTable::getHashTableCache()->setTotalCacheSize(
            CacheItemType::PERFECT_HT, original_total_cache_size);
      };
      clearCaches();

      auto q1 = "SELECT count(*) from t1, t2 where t1.x = t2.x;";
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q1, dt)));
      ASSERT_EQ(static_cast<size_t>(1), QR::get()->getNumberOfCachedPerfectHashTables());
      auto q1_perfect_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::PERFECT_HT);
      ASSERT_EQ(static_cast<size_t>(1), q1_perfect_ht_metrics->getRefCount());
      ASSERT_EQ(static_cast<size_t>(12), q1_perfect_ht_metrics->getMemSize());

      auto q2 = "SELECT count(*) from t2 a, t2 b where a.x = b.x;";
      ASSERT_EQ(static_cast<int64_t>(4), v<int64_t>(run_simple_query(q2, dt)));
      ASSERT_EQ(static_cast<size_t>(2), QR::get()->getNumberOfCachedPerfectHashTables());
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q1, dt)));
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q1, dt)));
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q1, dt)));

      auto q3 = "SELECT count(*) from t3 a, t3 b where a.x = b.x;";
      ASSERT_EQ(static_cast<int64_t>(5), v<int64_t>(run_simple_query(q3, dt)));
      ASSERT_EQ(static_cast<size_t>(2), QR::get()->getNumberOfCachedPerfectHashTables());
      auto q3_perfect_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::PERFECT_HT);
      ASSERT_EQ(q1_perfect_ht_metrics->getMemSize(), static_cast<size_t>(12));
      ASSERT_GT(q1_perfect_ht_metrics->getRefCount(), static_cast<size_t>(1));
      ASSERT_EQ(q3_perfect_ht_metrics->getMemSize(), static_cast<size_t>(20));
      ASSERT_EQ(q3_perfect_ht_metrics->getRefCount(), static_cast<size_t>(1));
      auto current_cache_size =
          PerfectJoinHashTable::getHashTableCache()->getCurrentCacheSizeForDevice(
              CacheItemType::PERFECT_HT, DataRecyclerUtil::CPU_DEVICE_IDENTIFIER);
      ASSERT_EQ(static_cast<size_t>(32), current_cache_size);
    }

    {
      // test 6. set per_hashtable_size_limit to be 18
      // and try to cache t1.x, t2.x and t3.x
      // due to the per item limit, we can cache t1.x's and t2.x's but not t3.x's
      const auto original_per_max_hashtable_size = g_max_cacheable_hashtable_size_bytes;
      PerfectJoinHashTable::getHashTableCache()->setMaxCacheItemSize(
          CacheItemType::PERFECT_HT, 18);
      ScopeGuard reset_cache_status = [&original_per_max_hashtable_size] {
        PerfectJoinHashTable::getHashTableCache()->setMaxCacheItemSize(
            CacheItemType::PERFECT_HT, original_per_max_hashtable_size);
      };
      clearCaches();

      auto q1 = "SELECT count(*) from t1, t2 where t1.x = t2.x;";
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q1, dt)));
      ASSERT_EQ(static_cast<size_t>(1), QR::get()->getNumberOfCachedPerfectHashTables());
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q1, dt)));
      auto q1_perfect_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::PERFECT_HT);

      auto q2 = "SELECT count(*) from t2 a, t2 b where a.x = b.x;";
      ASSERT_EQ(static_cast<int64_t>(4), v<int64_t>(run_simple_query(q2, dt)));
      ASSERT_EQ(static_cast<size_t>(2), QR::get()->getNumberOfCachedPerfectHashTables());
      ASSERT_EQ(static_cast<int64_t>(4), v<int64_t>(run_simple_query(q2, dt)));
      auto q2_perfect_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::PERFECT_HT);

      auto q3 = "SELECT count(*) from t3 a, t3 b where a.x = b.x;";
      ASSERT_EQ(static_cast<int64_t>(5), v<int64_t>(run_simple_query(q3, dt)));
      ASSERT_EQ(static_cast<size_t>(2), QR::get()->getNumberOfCachedPerfectHashTables());
      auto q3_perfect_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::PERFECT_HT);
      auto current_cache_size =
          PerfectJoinHashTable::getHashTableCache()->getCurrentCacheSizeForDevice(
              CacheItemType::PERFECT_HT, DataRecyclerUtil::CPU_DEVICE_IDENTIFIER);
      ASSERT_EQ(static_cast<size_t>(28), current_cache_size);
      if (!q1_perfect_ht_metrics || !q1_perfect_ht_metrics || q3_perfect_ht_metrics) {
        ASSERT_TRUE(false);
      }
    }
  }
}

TEST(DataRecycler, Baseline_Hashtable_Cache_Maintanence) {
  auto data_mgr = &QR::get()->getCatalog()->getDataMgr();
  auto executor = Executor::getExecutor(Executor::UNITARY_EXECUTOR_ID, data_mgr).get();
  std::set<QueryPlanHash> visited_hashtable_key;
  auto clearCaches = [&executor, data_mgr, &visited_hashtable_key] {
    Executor::clearMemory(MemoryLevel::CPU_LEVEL, data_mgr);
    executor->getQueryPlanDagCache().clearQueryPlanCache();
    visited_hashtable_key.clear();
  };

  // hashtables for t1, t2, t3 and t4
  // column x: unique key, like t1: {1,2,3} and t4: {1,2,3,4,5,6}
  // column y: dup. key, like t1: {1,1,1} and t2: {1,1,1,2,2,2}
  // query on a single col: deal with perfect hashtable
  // query on both cols x and y: deal with baseline hashtable
  // hashtable size info
  // t1.x = 12 / t1.y = 20 / t1.x and t1.y = 72
  // t2.x = 16 / t2.y = 32 / t2.x and t2.y = 96
  // t3.x = 20 / t3.y = 36 / t3.x and t3.y = 120
  // t4.x = 24 / t4.y = 40 / t4.x and t4.y = 144
  for (auto dt : {ExecutorDeviceType::CPU}) {
    // currently we do not support hashtable caching for GPU
    {
      // test1. cache hashtable of t1 and then reuse it correctly?
      clearCaches();
      auto q1 = "SELECT count(*) from t1, t2 where t1.x = t2.x and t1.y = t2.y;";
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q1, dt)));
      ASSERT_EQ(static_cast<size_t>(1),
                QR::get()->getNumberOfCachedBaselineJoinHashTables());
      auto q1_baseline_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::BASELINE_HT);
      auto ht1_ref_count_v1 = q1_baseline_ht_metrics->getRefCount();
      ASSERT_EQ(static_cast<size_t>(1), ht1_ref_count_v1);
      ASSERT_EQ(static_cast<size_t>(72), q1_baseline_ht_metrics->getMemSize());
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q1, dt)));
      ASSERT_EQ(static_cast<size_t>(1),
                QR::get()->getNumberOfCachedBaselineJoinHashTables());
      auto ht1_ref_count_v2 = q1_baseline_ht_metrics->getRefCount();
      ASSERT_LT(ht1_ref_count_v1, ht1_ref_count_v2);
      auto q2 = "SELECT count(*) from t1, t3 where t1.x = t3.x and t1.y = t3.y;";
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q2, dt)));
      ASSERT_EQ(static_cast<size_t>(1),
                QR::get()->getNumberOfCachedBaselineJoinHashTables());
      auto ht1_ref_count_v3 = q1_baseline_ht_metrics->getRefCount();
      ASSERT_LT(ht1_ref_count_v2, ht1_ref_count_v3);
    }

    {
      // test2. cache t1 and t2
      clearCaches();
      auto q1 = "SELECT count(*) from t1, t2 where t1.x = t2.x and t1.y = t2.y;";
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q1, dt)));
      ASSERT_EQ(static_cast<size_t>(1),
                QR::get()->getNumberOfCachedBaselineJoinHashTables());
      auto q1_baseline_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::BASELINE_HT);
      auto ht1_ref_count_v1 = q1_baseline_ht_metrics->getRefCount();
      ASSERT_EQ(static_cast<size_t>(1), ht1_ref_count_v1);
      ASSERT_EQ(static_cast<size_t>(72), q1_baseline_ht_metrics->getMemSize());

      auto q2 = "SELECT count(*) from t2, t3 where t2.x = t3.x and t2.y = t3.y;";
      ASSERT_EQ(static_cast<int64_t>(4), v<int64_t>(run_simple_query(q2, dt)));
      ASSERT_EQ(static_cast<size_t>(2),
                QR::get()->getNumberOfCachedBaselineJoinHashTables());
    }

    {
      // test3. set hashtable cache size as 150 bytes,
      // and try to cache t1's hashtable (72 bytes) and then that of t4's (144 bytes)
      // since sizeof(t1) + sizeof(t4) > 150 we need to remove t1's to cache t4's
      const auto original_total_cache_size = g_hashtable_cache_total_bytes;
      BaselineJoinHashTable::getHashTableCache()->setTotalCacheSize(
          CacheItemType::BASELINE_HT, 150);
      ScopeGuard reset_cache_status = [&original_total_cache_size] {
        BaselineJoinHashTable::getHashTableCache()->setTotalCacheSize(
            CacheItemType::BASELINE_HT, original_total_cache_size);
      };
      clearCaches();

      auto q1 = "SELECT count(*) from t1, t2 where t1.x = t2.x and t1.y = t2.y;";
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q1, dt)));
      ASSERT_EQ(static_cast<size_t>(1),
                QR::get()->getNumberOfCachedBaselineJoinHashTables());
      auto q1_baseline_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::BASELINE_HT);
      auto ht1_ref_count = q1_baseline_ht_metrics->getRefCount();
      ASSERT_EQ(static_cast<size_t>(1), ht1_ref_count);
      ASSERT_EQ(static_cast<size_t>(72), q1_baseline_ht_metrics->getMemSize());

      auto q2 = "SELECT count(*) from t4 a, t4 b where a.x = b.x and a.y = b.y;";
      ASSERT_EQ(static_cast<int64_t>(6), v<int64_t>(run_simple_query(q2, dt)));
      ASSERT_EQ(static_cast<size_t>(1),
                QR::get()->getNumberOfCachedBaselineJoinHashTables());
      auto q4_baseline_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::BASELINE_HT);
      auto ht4_ref_count = q4_baseline_ht_metrics->getRefCount();
      ASSERT_EQ(static_cast<size_t>(1), ht4_ref_count);
      ASSERT_EQ(static_cast<size_t>(144), q4_baseline_ht_metrics->getMemSize());
      auto current_cache_size =
          BaselineJoinHashTable::getHashTableCache()->getCurrentCacheSizeForDevice(
              CacheItemType::BASELINE_HT, DataRecyclerUtil::CPU_DEVICE_IDENTIFIER);
      ASSERT_EQ(static_cast<size_t>(144), current_cache_size);
    }

    {
      // test4. set hashtable cache size as 180 bytes, and
      // cache t1 and t2 (so total 72+96 = 168 bytes) and then try to cache t4's
      // and check whether cache only has t4's (remove t1.x's and t2.x's to make a room
      // for t4's)
      const auto original_total_cache_size = g_hashtable_cache_total_bytes;
      BaselineJoinHashTable::getHashTableCache()->setTotalCacheSize(
          CacheItemType::BASELINE_HT, 180);
      ScopeGuard reset_cache_status = [&original_total_cache_size] {
        BaselineJoinHashTable::getHashTableCache()->setTotalCacheSize(
            CacheItemType::BASELINE_HT, original_total_cache_size);
      };
      clearCaches();

      auto q1 = "SELECT count(*) from t1, t2 where t1.x = t2.x and t1.y = t2.y;";
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q1, dt)));
      ASSERT_EQ(static_cast<size_t>(1),
                QR::get()->getNumberOfCachedBaselineJoinHashTables());
      auto q1_baseline_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::BASELINE_HT);
      auto ht1_ref_count = q1_baseline_ht_metrics->getRefCount();
      ASSERT_EQ(static_cast<size_t>(1), ht1_ref_count);
      ASSERT_EQ(static_cast<size_t>(72), q1_baseline_ht_metrics->getMemSize());

      auto q2 = "SELECT count(*) from t2 a, t2 b where a.x = b.x and a.y = b.y;";
      ASSERT_EQ(static_cast<int64_t>(4), v<int64_t>(run_simple_query(q2, dt)));
      ASSERT_EQ(static_cast<size_t>(2),
                QR::get()->getNumberOfCachedBaselineJoinHashTables());

      auto q3 = "SELECT count(*) from t4 a, t4 b where a.x = b.x and a.y = b.y;";
      ASSERT_EQ(static_cast<int64_t>(6), v<int64_t>(run_simple_query(q3, dt)));
      ASSERT_EQ(static_cast<size_t>(1),
                QR::get()->getNumberOfCachedBaselineJoinHashTables());
      auto q3_baseline_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::BASELINE_HT);
      auto ht4_ref_count = q3_baseline_ht_metrics->getRefCount();
      ASSERT_EQ(static_cast<size_t>(1), ht4_ref_count);
      ASSERT_EQ(static_cast<size_t>(144), q3_baseline_ht_metrics->getMemSize());
      auto current_cache_size =
          BaselineJoinHashTable::getHashTableCache()->getCurrentCacheSizeForDevice(
              CacheItemType::BASELINE_HT, DataRecyclerUtil::CPU_DEVICE_IDENTIFIER);
      ASSERT_EQ(static_cast<size_t>(144), current_cache_size);
    }

    {
      // test5. set hashtable cache size as 200 bytes, and
      // we try to cache t1 (72 bytes), t2 (96 bytes) and t3's hashtable (120 bytes),
      // respectively here we make t1's to be more frequently reused one than t2's and try
      // to cache t3's. in this case we should remove at least one hashtable btw. t1 and
      // t2 since sizeof(t1) + sizeof(t2) + sizeof(t3) ? 72 + 96 + 120 > 200 if our cache
      // maintenance works correctly, we should remove t2's since it is less frequently
      // reused one (so 72 + 120 = 192 < 200)
      const auto original_total_cache_size = g_hashtable_cache_total_bytes;
      BaselineJoinHashTable::getHashTableCache()->setTotalCacheSize(
          CacheItemType::BASELINE_HT, 200);
      ScopeGuard reset_cache_status = [&original_total_cache_size] {
        BaselineJoinHashTable::getHashTableCache()->setTotalCacheSize(
            CacheItemType::BASELINE_HT, original_total_cache_size);
      };
      clearCaches();

      auto q1 = "SELECT count(*) from t1, t2 where t1.x = t2.x and t1.y = t2.y;";
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q1, dt)));
      ASSERT_EQ(static_cast<size_t>(1),
                QR::get()->getNumberOfCachedBaselineJoinHashTables());
      auto q1_baseline_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::BASELINE_HT);
      ASSERT_EQ(static_cast<size_t>(1), q1_baseline_ht_metrics->getRefCount());
      ASSERT_EQ(static_cast<size_t>(72), q1_baseline_ht_metrics->getMemSize());
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q1, dt)));
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q1, dt)));

      auto q2 = "SELECT count(*) from t2 a, t2 b where a.x = b.x and a.y = b.y;";
      ASSERT_EQ(static_cast<int64_t>(4), v<int64_t>(run_simple_query(q2, dt)));
      ASSERT_EQ(static_cast<size_t>(2),
                QR::get()->getNumberOfCachedBaselineJoinHashTables());

      auto q3 = "SELECT count(*) from t3 a, t3 b where a.x = b.x and a.y = b.y;";
      ASSERT_EQ(static_cast<int64_t>(5), v<int64_t>(run_simple_query(q3, dt)));
      ASSERT_EQ(static_cast<size_t>(2),
                QR::get()->getNumberOfCachedBaselineJoinHashTables());
      auto q3_baseline_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::BASELINE_HT);
      ASSERT_EQ(q3_baseline_ht_metrics->getMemSize(), static_cast<size_t>(120));
      auto current_cache_size =
          BaselineJoinHashTable::getHashTableCache()->getCurrentCacheSizeForDevice(
              CacheItemType::BASELINE_HT, DataRecyclerUtil::CPU_DEVICE_IDENTIFIER);
      ASSERT_EQ(static_cast<size_t>(192), current_cache_size);
    }

    {
      // test 6. set per_hashtable_size_limit to be 100
      // and try to cache t1 (72 bytes), t2 (96 bytes) and t3 (120 bytes)
      // due to the per item limit, we can cache t1's and t2's but not t3's
      const auto original_per_max_hashtable_size = g_max_cacheable_hashtable_size_bytes;
      BaselineJoinHashTable::getHashTableCache()->setMaxCacheItemSize(
          CacheItemType::BASELINE_HT, 100);
      ScopeGuard reset_cache_status = [&original_per_max_hashtable_size] {
        BaselineJoinHashTable::getHashTableCache()->setMaxCacheItemSize(
            CacheItemType::BASELINE_HT, original_per_max_hashtable_size);
      };
      clearCaches();

      auto q1 = "SELECT count(*) from t1, t2 where t1.x = t2.x and t1.y = t2.y;";
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q1, dt)));
      ASSERT_EQ(static_cast<size_t>(1),
                QR::get()->getNumberOfCachedBaselineJoinHashTables());

      auto q2 = "SELECT count(*) from t2 a, t2 b where a.x = b.x and a.y = b.y;";
      ASSERT_EQ(static_cast<int64_t>(4), v<int64_t>(run_simple_query(q2, dt)));
      ASSERT_EQ(static_cast<size_t>(2),
                QR::get()->getNumberOfCachedBaselineJoinHashTables());

      auto q3 = "SELECT count(*) from t3 a, t3 b where a.x = b.x and a.y = b.y;";
      ASSERT_EQ(static_cast<int64_t>(5), v<int64_t>(run_simple_query(q3, dt)));
      ASSERT_EQ(static_cast<size_t>(2),
                QR::get()->getNumberOfCachedBaselineJoinHashTables());
      auto current_cache_size =
          BaselineJoinHashTable::getHashTableCache()->getCurrentCacheSizeForDevice(
              CacheItemType::BASELINE_HT, DataRecyclerUtil::CPU_DEVICE_IDENTIFIER);
      ASSERT_EQ(static_cast<size_t>(168), current_cache_size);
    }
  }
}

TEST(DataRecycler, Hashtable_From_Subqueries) {
  // todo (yoonmin): revisit here if we support skipping hashtable building based on
  // consideration of filter quals
  auto data_mgr = &QR::get()->getCatalog()->getDataMgr();
  auto executor = Executor::getExecutor(Executor::UNITARY_EXECUTOR_ID, data_mgr).get();
  std::set<QueryPlanHash> visited_hashtable_key;
  auto clearCaches = [&executor, data_mgr, &visited_hashtable_key] {
    Executor::clearMemory(MemoryLevel::CPU_LEVEL, data_mgr);
    executor->getQueryPlanDagCache().clearQueryPlanCache();
    visited_hashtable_key.clear();
  };

  for (auto dt : {ExecutorDeviceType::CPU}) {
    // currently we do not support hashtable caching for GPU
    {
      // test1. perfect hashtable
      clearCaches();
      auto q1 =
          "SELECT count(*) from t1, (select x from t2 where x < 3) tt2 where t1.x = "
          "tt2.x;";
      ASSERT_EQ(static_cast<int64_t>(2), v<int64_t>(run_simple_query(q1, dt)));
      ASSERT_EQ(static_cast<size_t>(1), QR::get()->getNumberOfCachedPerfectHashTables());
      auto q1_perfect_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::PERFECT_HT);
      auto ht1_ref_count_v1 = q1_perfect_ht_metrics->getRefCount();
      ASSERT_EQ(static_cast<size_t>(1), ht1_ref_count_v1);
      ASSERT_EQ(static_cast<int64_t>(2), v<int64_t>(run_simple_query(q1, dt)));
      ASSERT_EQ(static_cast<size_t>(1), QR::get()->getNumberOfCachedPerfectHashTables());
      auto ht1_ref_count_v2 = q1_perfect_ht_metrics->getRefCount();
      ASSERT_LT(ht1_ref_count_v1, ht1_ref_count_v2);

      auto q2 =
          "SELECT count(*) from t1, (select x from t2 where x < 2) tt2 where t1.x = "
          "tt2.x;";
      ASSERT_EQ(static_cast<int64_t>(1), v<int64_t>(run_simple_query(q2, dt)));
      ASSERT_EQ(static_cast<size_t>(2), QR::get()->getNumberOfCachedPerfectHashTables());
      auto q2_perfect_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::PERFECT_HT);
      auto ht1_ref_count_v3 = q2_perfect_ht_metrics->getRefCount();
      ASSERT_GT(ht1_ref_count_v2, ht1_ref_count_v3);

      auto q3 =
          "SELECT count(*) from (select x from t1) tt1, (select x from t2 where x < 2) "
          "tt2 where tt1.x = tt2.x;";
      ASSERT_EQ(static_cast<int64_t>(1), v<int64_t>(run_simple_query(q3, dt)));
      ASSERT_EQ(static_cast<size_t>(2), QR::get()->getNumberOfCachedPerfectHashTables());
      auto ht1_ref_count_v4 = q2_perfect_ht_metrics->getRefCount();
      ASSERT_LT(ht1_ref_count_v3, ht1_ref_count_v4);
    }

    {
      // test2. baseline hashtable
      clearCaches();
      auto q1 =
          "SELECT count(*) from t1, (select x,y from t2 where x < 4) tt2 where t1.x = "
          "tt2.x and t1.y = tt2.y;";
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q1, dt)));
      ASSERT_EQ(static_cast<size_t>(1),
                QR::get()->getNumberOfCachedBaselineJoinHashTables());
      auto q1_baseline_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::BASELINE_HT);
      auto ht1_ref_count_v1 = q1_baseline_ht_metrics->getRefCount();
      ASSERT_EQ(static_cast<size_t>(1), ht1_ref_count_v1);
      ASSERT_EQ(static_cast<int64_t>(3), v<int64_t>(run_simple_query(q1, dt)));
      ASSERT_EQ(static_cast<size_t>(1),
                QR::get()->getNumberOfCachedBaselineJoinHashTables());
      auto ht1_ref_count_v2 = q1_baseline_ht_metrics->getRefCount();
      ASSERT_LT(ht1_ref_count_v1, ht1_ref_count_v2);

      auto q2 =
          "SELECT count(*) from t1, (select x, y from t3 where x < 3) tt3 where t1.x = "
          "tt3.x and t1.y = tt3.y;";
      ASSERT_EQ(static_cast<int64_t>(2), v<int64_t>(run_simple_query(q2, dt)));
      ASSERT_EQ(static_cast<size_t>(2),
                QR::get()->getNumberOfCachedBaselineJoinHashTables());
      auto q2_baseline_ht_metrics =
          getCachedHashTableMetric(visited_hashtable_key, CacheItemType::BASELINE_HT);
      auto ht1_ref_count_v3 = q2_baseline_ht_metrics->getRefCount();
      ASSERT_LT(ht1_ref_count_v3, ht1_ref_count_v2);

      auto q3 =
          "SELECT count(*) from (select x, y from t1 where x < 3) tt1, (select x, y from "
          "t3 where x < 3) tt3 where tt1.x = tt3.x and tt1.y = tt3.y;";
      ASSERT_EQ(static_cast<int64_t>(2), v<int64_t>(run_simple_query(q3, dt)));
      ASSERT_EQ(static_cast<size_t>(2),
                QR::get()->getNumberOfCachedBaselineJoinHashTables());
      auto ht1_ref_count_v4 = q2_baseline_ht_metrics->getRefCount();
      ASSERT_LT(ht1_ref_count_v3, ht1_ref_count_v4);
    }

    {
      // test 3. changing resultset's ordering
      clearCaches();
      auto q1 =
          "WITH tt2 AS (SELECT z, approx_count_distinct(x) * 1.0 AS xx FROM t2 GROUP BY "
          "z) SELECT tt2.z, tt2.xx FROM t4, tt2 WHERE (t4.z = tt2.z) ORDER BY tt2.z;";
      auto q2 =
          "WITH tt2 AS (SELECT z, x FROM t2 limit 2) SELECT tt2.z, tt2.x FROM t4, tt2 "
          "WHERE (t4.z = tt2.z) ORDER BY tt2.z;";
      for (int i = 0; i < 5; ++i) {
        QR::get()->runSQL(q1, dt);
        QR::get()->runSQL(q2, dt);
      }
      // we have to skip hashtable caching for the above query
      ASSERT_EQ(static_cast<size_t>(0), QR::get()->getNumberOfCachedPerfectHashTables());
    }
  }
}

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  TestHelpers::init_logger_stderr_only(argc, argv);

  QR::init(BASE_PATH);
  g_is_test_env = true;
  int err{0};
  try {
    err = create_and_populate_table();
    err = RUN_ALL_TESTS();
    err = drop_table();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
  }
  g_is_test_env = false;
  QR::reset();
  return err;
}
