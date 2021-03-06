cmake_minimum_required(VERSION 2.8)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")

project(CLog)

add_definitions(-std=c++1y)


set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
find_package(Boost 1.54.0
    COMPONENTS
    filesystem
    REQUIRED
)
find_package(APR REQUIRED)

include_directories(${Boost_INCLUDE_DIRS})
include_directories("${APR_INCLUDE_DIR}")
include_directories("./")

add_executable(${PROJECT_NAME} main.cpp roaring.c)

target_link_libraries(
    ${PROJECT_NAME}
    "${Boost_LIBRARIES}"
    "${APR_LIBRARY}"
    lz4
)
