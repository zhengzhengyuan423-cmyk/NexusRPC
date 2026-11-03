cmake_minimum_required(VERSION 3.14)
project(Nexus LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 单测依赖 assert（EXPECT_DEATH 用例），默认 Debug
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

find_package(Threads REQUIRED)

# jsoncpp：没有自己下载
find_package(jsoncpp REQUIRED)

# gtest：先下载，没装跑不了
find_package(GTest REQUIRED)

enable_testing()
add_subdirectory(src)
add_subdirectory(tests)
add_subdirectory(demo)
