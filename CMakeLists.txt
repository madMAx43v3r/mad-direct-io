cmake_minimum_required(VERSION 3.10)

set(CMAKE_CXX_STANDARD 11)

project(mad-direct-io CXX)

include_directories(include)

add_executable(test_write test/test_write.cpp)
