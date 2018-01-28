cmake_minimum_required(VERSION 3.5)
project(perq)
set(CMAKE_CXX_STANDARD 14)

SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -Wall -Werror")

include(ExternalProject)

find_package(Boost 1.58 REQUIRED COMPONENTS filesystem system)

option(DOWNLOAD_ROCKSDB "Downloads rocksdb" OFF)
option(DOWNLOAD_CATCH "Downloads catch" OFF)

if (DOWNLOAD_ROCKSDB)
  link_directories("${CMAKE_CURRENT_BINARY_DIR}/rocksdbdist/lib/")
endif()

add_executable(tests tests/tests.cpp)

if (DOWNLOAD_ROCKSDB)
  ExternalProject_Add(rocksdb_project
    URL https://github.com/facebook/rocksdb/archive/v5.9.2.tar.gz
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/rocksdb
    CMAKE_ARGS -DCMAKE_CXX_COMPILER=${DCMAKE_CXX_COMPILER} -DCMAKE_C_COMPILER=${DCMAKE_C_COMPILER} -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_CURRENT_BINARY_DIR}/rocksdbdist  -DWITH_TESTS=0 -DWITH_TOOLS=0
  )

  add_dependencies(tests rocksdb_project)

  target_include_directories(tests PRIVATE "${CMAKE_CURRENT_BINARY_DIR}/rocksdbdist/include")
  target_link_libraries(tests PRIVATE rocksdb pthread)
else()
  find_package(RocksDB 5.8 REQUIRED)

  target_link_libraries(tests PRIVATE RocksDB::rocksdb-shared)
endif()

if (DOWNLOAD_ROCKSDB)
  # Later in version 5.8
  # DOWNLOAD_NO_EXTRACT 1
  # URL https://github.com/catchorg/Catch2/releases/download/v2.1.1/catch.hpp
  ExternalProject_Add(catch_project
    URL https://github.com/catchorg/Catch2/archive/v2.1.1.tar.gz
    PREFIX ${CMAKE_CURRENT_BINARY_DIR}/catch
    CMAKE_ARGS -DCMAKE_CXX_COMPILER=${DCMAKE_CXX_COMPILER} -DCMAKE_C_COMPILER=${DCMAKE_C_COMPILER} -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_CURRENT_BINARY_DIR}/catchdist
  )

  add_dependencies(tests catch_project)

  target_include_directories(tests PRIVATE "${CMAKE_CURRENT_BINARY_DIR}/catchdist/include")
else()
  find_file(CATCH_INCLUDE_DIR "catch.hpp" PATH_SUFFIXES include/catch)
  get_filename_component(CATCH_INCLUDE_DIR "${CATCH_INCLUDE_DIR}" DIRECTORY)
  get_filename_component(CATCH_INCLUDE_DIR "${CATCH_INCLUDE_DIR}" DIRECTORY)

  message(STATUS "Found Catch2 at ${CATCH_INCLUDE_DIR}")

  target_include_directories(tests PRIVATE "${CATCH_INCLUDE_DIR}")
endif()

target_include_directories(tests PRIVATE
  include
  ${Boost_INCLUDE_DIRS}
  )
target_link_libraries(tests PRIVATE
  ${Boost_LIBRARIES}
  )
