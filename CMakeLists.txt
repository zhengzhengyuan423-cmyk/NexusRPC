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

# gtest：系统里装了就用系统的，没有就拉源码编译
find_package(GTest QUIET)
if(NOT GTest_FOUND)
    include(FetchContent)
    FetchContent_Declare(googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.14.0)
    FetchContent_MakeAvailable(googletest)
endif()

add_library(nexus_net STATIC
    src/net/conn/buffer.cpp
    src/net/conn/socket.cpp
    src/net/event/poller.cpp
    src/net/event/event_loop.cpp
    src/net/conn/connection.cpp
    src/net/server/tcp_server.cpp
)
target_include_directories(nexus_net PUBLIC src)
target_compile_options(nexus_net PRIVATE -Wall -Wextra)
target_link_libraries(nexus_net PUBLIC Threads::Threads)

enable_testing()
foreach(tgt buffer_test event_loop_test connection_test server_test)
    add_executable(${tgt} tests/net/${tgt}.cpp)
    target_link_libraries(${tgt} PRIVATE nexus_net GTest::gtest GTest::gtest_main)
    target_compile_options(${tgt} PRIVATE -Wall -Wextra)
    add_test(NAME ${tgt} COMMAND ${tgt})
endforeach()

add_executable(echo_server demo/echo_server.cpp)
target_link_libraries(echo_server PRIVATE nexus_net)
add_executable(echo_client demo/echo_client.cpp)
target_link_libraries(echo_client PRIVATE nexus_net)
