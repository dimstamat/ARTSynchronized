cmake_minimum_required(VERSION 2.8)
project(ARTSynchronized)

if( NOT CMAKE_BUILD_TYPE )
    message(STATUS "No build type selected, default to Release")
    set( CMAKE_BUILD_TYPE Release)
else()
    message(STATUS "Build type is set to ${CMAKE_BUILD_TYPE}")
endif()

set(MURMURHASH_DIR "/home/dimos/sto/smhasher/src")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14 -Wall -Wextra -march=native -g -I${MURMURHASH_DIR} -I/home/dimos/tbb/include -L/home/dimos/tbb/build/linux_intel64_gcc_cc5.3.1_libc2.17_kernel3.10.0_release")

find_library(JemallocLib jemalloc)
find_library(TbbLib tbb)
find_package (Threads)

set(ART_FILES OptimisticLockCoupling/Tree.cpp ROWEX/Tree.cpp ART/Tree.cpp)
add_library(ARTSynchronized ${ART_FILES})
target_link_libraries(ARTSynchronized ${TbbLib} ${JemallocLib} ${CMAKE_THREAD_LIBS_INIT})


set(EXAMPLE_SRC example.cpp)
set(TEST_SIMPLE_SRC test_simple.cpp)
set(TEST_BLOOM_SRC test_bloom.cpp)
set(TEST_BLOOM_NOTBB_SRC test_bloom_notbb.cpp)
add_executable(example ${EXAMPLE_SRC})
add_executable(test_simple ${TEST_SIMPLE_SRC})
add_executable(test_bloom ${TEST_BLOOM_SRC})
add_executable(test_bloom_notbb ${TEST_BLOOM_NOTBB_SRC})
target_link_libraries(example ARTSynchronized)
target_link_libraries(test_simple ARTSynchronized)
target_link_libraries(test_bloom ARTSynchronized ${MURMURHASH_DIR}/libSMHasherSupport.a)
target_link_libraries(test_bloom_notbb ARTSynchronized)
