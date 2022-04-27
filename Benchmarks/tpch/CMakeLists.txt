set(bench_name tpch_bench)
set(EXECUTE_TEST_LIBS gtest ArrowStorage ArrowQueryRunner ${MAPD_LIBRARIES} ${Arrow_LIBRARIES} ${CMAKE_DL_LIBS} ${CUDA_LIBRARIES} ${Boost_LIBRARIES} ${ZLIB_LIBRARIES})
add_executable(${bench_name} tpch_bench.cpp)
target_link_libraries(${bench_name} ${EXECUTE_TEST_LIBS} benchmark)