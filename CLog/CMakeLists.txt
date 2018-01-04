cmake_minimum_required(VERSION 2.8)

project(CLog)

add_definitions(-std=c++1y)

include_directories(CRoaring/include)
include_directories(CRoaring/cpp)
add_subdirectory(CRoaring)

add_executable(${PROJECT_NAME} "main.cpp")
target_link_libraries(${PROJECT_NAME} roaring lz4)
