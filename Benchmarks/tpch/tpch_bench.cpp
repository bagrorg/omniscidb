#include <benchmark/benchmark.h>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>

#include "Tests/ArrowSQLRunner/ArrowSQLRunner.h"

size_t g_fragment_size = 1'000'000;

extern bool g_enable_heterogeneous_execution;
extern bool g_enable_multifrag_heterogeneous_execution;

namespace fs = boost::filesystem;
namespace po = boost::program_options;
using namespace TestHelpers::ArrowSQLRunner;

using TableColumnsDescription = std::vector<ArrowStorage::ColumnDescription>;

static void createTPCHTables(const std::string& tableName,
                             TableColumnsDescription& tableColumns) {
  getStorage()->dropTable(tableName);
  ArrowStorage::TableOptions to{g_fragment_size};
  createTable(tableName, tableColumns, to);
}

static void populateTPCHTables(const fs::path& data_p,
                               const std::string& name,
                               char delim) {
  ArrowStorage::CsvParseOptions po;
  po.delimiter = delim;
  po.header = false;
  getStorage()->appendCsvFile(data_p.string(), name, po);
}

// Is it better to create util library for tpch and taxi benchmarks?
template <class T>
T v(const TargetValue& r) {
  auto scalar_r = boost::get<ScalarTargetValue>(&r);
  auto p = boost::get<T>(scalar_r);
  return *p;
}

static void warmup() {
  auto res = v<int64_t>(
      run_simple_agg("select count(*) from lineitem", ExecutorDeviceType::GPU));
  std::cout << "Number of loaded tuples: " << res << std::endl;
}

static void tpch_q1(benchmark::State& state) {
  std::string query = R"(
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
        sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from
        lineitem
    where
        l_shipdate <= date '1998-12-01' - interval '90' day (3) 
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus;
  
  )";

  for (auto _ : state) {
    run_multiple_agg(query, ExecutorDeviceType::GPU);
  }
}

static void tpch_q2(benchmark::State& state) {
  std::string query = R"(
      select
        s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment
      from
        part,
        supplier,
        partsupp,
        nation,
        region
      where
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
        and p_size = 15
        and p_type like '%BRASS'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'EUROPE'
        and ps_supplycost = (
              select
                min(ps_supplycost)
              from
                partsupp, supplier,
                nation, region
              where
                p_partkey = ps_partkey
                and s_suppkey = ps_suppkey
                and s_nationkey = n_nationkey
                and n_regionkey = r_regionkey
                and r_name = 'EUROPE'
        )
      order by
        s_acctbal desc,
        n_name,
        s_name,
        p_partkey;
  
  )";

  for (auto _ : state) {
    run_multiple_agg(query, ExecutorDeviceType::GPU);
  }
}

static void tpch_q3(benchmark::State& state) {
  std::string query = R"(
    select
      l_orderkey,
      sum(l_extendedprice*(1-l_discount)) as revenue,
      o_orderdate,
      o_shippriority
    from
      customer,
      orders,
      lineitem
    where
      c_mktsegment = 'BUILDING'
      and c_custkey = o_custkey
      and l_orderkey = o_orderkey
      and o_orderdate < date '1995-03-05'
      and l_shipdate > date '1995-03-05'
    group by
      l_orderkey,
      o_orderdate,
      o_shippriority
    order by
      revenue desc,
      o_orderdate;
  
  )";

  for (auto _ : state) {
    run_multiple_agg(query, ExecutorDeviceType::GPU);
  }
}

static void tpch_q4(benchmark::State& state) {
  std::string query = R"(
    select
      o_orderpriority,
      count(*) as order_count
    from
      orders
    where
      o_orderdate >= date '1993-07-01'
      and o_orderdate < date '1993-07-01' + interval '3' month
      and exists (
          select
            *
          from
            lineitem
          where
            l_orderkey = o_orderkey
            and l_commitdate < l_receiptdate
      )
    group by
      o_orderpriority
    order by
      o_orderpriority;
  
  )";

  for (auto _ : state) {
    run_multiple_agg(query, ExecutorDeviceType::GPU);
  }
}

BENCHMARK(tpch_q1);
BENCHMARK(tpch_q2);
BENCHMARK(tpch_q3);
BENCHMARK(tpch_q4);

int main(int argc, char* argv[]) {
  ::benchmark::Initialize(&argc, argv);

  fs::path data_path;
  char delim;

  po::options_description desc("Options");
  desc.add_options()("enable-heterogeneous",
                     po::value<bool>(&g_enable_heterogeneous_execution)
                         ->default_value(g_enable_heterogeneous_execution)
                         ->implicit_value(true),
                     "Allow heterogeneous execution.");
  desc.add_options()("enable-multifrag",
                     po::value<bool>(&g_enable_multifrag_heterogeneous_execution)
                         ->default_value(g_enable_multifrag_heterogeneous_execution)
                         ->implicit_value(true),
                     "Allow multifrag heterogeneous execution.");
  desc.add_options()("data",
                     po::value<fs::path>(&data_path),
                     "Path to tpch dataset. Should be directory with <tablename>.csv "
                     "files (e.g. lineitem.csv)");
  desc.add_options()("csv-delimiter",
                     po::value<char>(&delim)->default_value(','),
                     ".csv files delimiter");
  desc.add_options()("fragment-size",
                     po::value<size_t>(&g_fragment_size)->default_value(g_fragment_size),
                     "Table fragment size.");

  logger::LogOptions log_options(argv[0]);
  log_options.severity_ = logger::Severity::FATAL;
  log_options.set_options();
  desc.add(log_options.get_options());

  po::variables_map vm;
  po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << "Usage:" << std::endl << desc << std::endl;
  }

  logger::init(log_options);
  init();

  std::vector<std::string> tpch_tables_names = {"customer",
                                                "lineitem",
                                                "nation",
                                                "orders",
                                                "partsupp",
                                                "part",
                                                "region",
                                                "supplier"};

  std::vector<TableColumnsDescription> tpch_tables_columns = {
      {
          {"C_CUSTKEY", SQLTypeInfo(kBIGINT)},
          {"C_NAME", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
          {"C_ADDRESS", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
          {"C_NATIONKEY", SQLTypeInfo(kBIGINT)},
          {"C_PHONE", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
          {"C_ACCTBAL", SQLTypeInfo(kDECIMAL, 16, 2)},
          {"C_MKTSEGMENT", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
          {"C_COMMENT", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},

      },

      {
          {"L_ORDERKEY", SQLTypeInfo(kBIGINT)},
          {"L_PARTKEY", SQLTypeInfo(kBIGINT)},
          {"L_SUPPKEY", SQLTypeInfo(kBIGINT)},
          {"L_LINENUMBER", SQLTypeInfo(kINT)},
          {"L_QUANTITY", SQLTypeInfo(kINT)},
          {"L_EXTENDEDPRICE", SQLTypeInfo(kDECIMAL, 16, 2)},
          {"L_DISCOUNT", SQLTypeInfo(kDECIMAL, 16, 2)},
          {"L_TAX", SQLTypeInfo(kDECIMAL, 16, 2)},
          {"L_RETURNFLAG", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
          {"L_LINESTATUS", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
          {"L_SHIPDATE", SQLTypeInfo(kTIMESTAMP, 0, 0)},
          {"L_COMMITDATE", SQLTypeInfo(kTIMESTAMP, 0, 0)},
          {"L_RECEIPTDATE", SQLTypeInfo(kTIMESTAMP, 0, 0)},
          {"L_SHIPINSTRUCT", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
          {"L_SHIPMODE", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
          {"L_COMMENT", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
      },

      {
          {"N_NATIONKEY", SQLTypeInfo(kBIGINT)},
          {"N_NAME", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
          {"N_REGIONKEY", SQLTypeInfo(kBIGINT)},
          {"N_COMMENT", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
      },

      {
          {"O_ORDERKEY", SQLTypeInfo(kBIGINT)},
          {"O_CUSTKEY", SQLTypeInfo(kBIGINT)},
          {"O_ORDERSTATUS", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
          {"O_TOTALPRICE", SQLTypeInfo(kDECIMAL, 16, 2)},
          {"O_ORDERDATE", SQLTypeInfo(kTIMESTAMP, 0, 0)},
          {"O_ORDERPRIORITY", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
          {"O_CLERK", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
          {"O_SHIPPRIORITY", SQLTypeInfo(kINT)},
          {"O_COMMENT", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},

      },

      {
          {"PS_PARTKEY", SQLTypeInfo(kBIGINT)},
          {"PS_SUPPKEY", SQLTypeInfo(kBIGINT)},
          {"PS_AVAILQTY", SQLTypeInfo(kINT)},
          {"PS_SUPPLYCOST", SQLTypeInfo(kDECIMAL, 16, 2)},
          {"PS_COMMENT", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
      },

      {
          {"P_PARTKEY", SQLTypeInfo(kBIGINT)},
          {"P_NAME", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
          {"P_MFGR", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
          {"P_BRAND", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
          {"P_TYPE", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
          {"P_SIZE", SQLTypeInfo(kINT)},
          {"P_CONTAINER", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
          {"P_RETAILPRICE", SQLTypeInfo(kDECIMAL, 16, 2)},
          {"P_COMMENT", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)}
      },

      {
          {"R_REGIONKEY", SQLTypeInfo(kBIGINT)},
          {"R_NAME", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
          {"R_COMMENT", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
      },

      {
          {"S_SUPPKEY", SQLTypeInfo(kBIGINT)},
          {"S_NAME", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
          {"S_ADDRESS", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
          {"S_NATIONKEY", SQLTypeInfo(kBIGINT)},
          {"S_PHONE", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
          {"S_ACCTBAL", SQLTypeInfo(kDECIMAL, 16, 2)},
          {"S_COMMENT", SQLTypeInfo(kVARCHAR, true, kENCODING_DICT)},
      }
    };

  try {
    for (size_t i = 0; i < tpch_tables_names.size(); i++) {
      createTPCHTables(tpch_tables_names[i], tpch_tables_columns[i]);
      populateTPCHTables(
          data_path / (tpch_tables_names[i] + ".csv"), tpch_tables_names[i], delim);
    }
    warmup();
    ::benchmark::RunSpecifiedBenchmarks();
  } catch (const std::exception& e) {
    LOG(ERROR) << e.what();
    return -1;
  }

  reset();
}