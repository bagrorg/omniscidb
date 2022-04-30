set(lib_name BenchmarksUtils)
set(EXECUTE_TEST_LIBS ArrowQueryRunner ArrowStorage ${MAPD_LIBRARIES} ${Arrow_LIBRARIES} ${CMAKE_DL_LIBS} ${CUDA_LIBRARIES} ${Boost_LIBRARIES} ${ZLIB_LIBRARIES})
add_library(${lib_name} benchmarks_utils.cpp)
target_link_libraries(${lib_name} ${EXECUTE_TEST_LIBS})
