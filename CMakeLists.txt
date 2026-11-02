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

# jsoncpp：多策略查找
find_package(jsoncpp QUIET)
if(jsoncpp_FOUND AND NOT TARGET jsoncpp_lib)
    if(TARGET jsoncpp_static)
        add_library(jsoncpp_lib INTERFACE IMPORTED)
        set_target_properties(jsoncpp_lib PROPERTIES INTERFACE_LINK_LIBRARIES jsoncpp_static)
    elseif(TARGET jsoncpp_shared)
        add_library(jsoncpp_lib INTERFACE IMPORTED)
        set_target_properties(jsoncpp_lib PROPERTIES INTERFACE_LINK_LIBRARIES jsoncpp_shared)
    elseif(TARGET JsonCpp::JsonCpp)
        add_library(jsoncpp_lib INTERFACE IMPORTED)
        set_target_properties(jsoncpp_lib PROPERTIES INTERFACE_LINK_LIBRARIES JsonCpp::JsonCpp)
    endif()
endif()
if(NOT TARGET jsoncpp_lib)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(JSONCPP REQUIRED jsoncpp)
    add_library(jsoncpp_lib INTERFACE IMPORTED)
    set_target_properties(jsoncpp_lib PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${JSONCPP_INCLUDE_DIRS}"
        INTERFACE_LINK_LIBRARIES "${JSONCPP_LIBRARIES}"
        INTERFACE_LINK_DIRECTORIES "${JSONCPP_LIBRARY_DIRS}"
    )
endif()

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
    src/net/server/tcp_client.cpp
    src/net/api/inet.cpp
)
target_include_directories(nexus_net PUBLIC src)
target_compile_options(nexus_net PRIVATE -Wall -Wextra)
target_link_libraries(nexus_net PUBLIC Threads::Threads)

# 协议+RPC：INet 门面的唯一使用方（除 demo/测试）
add_library(nexus_rpc STATIC
    src/protocol/message.cpp
    src/protocol/protocol.cpp
    src/protocol/pump.cpp
    src/protocol/requestor.cpp
    src/rpc/rpc_router.cpp
    src/rpc/rpc_server.cpp
    src/rpc/rpc_client.cpp
)
target_compile_options(nexus_rpc PRIVATE -Wall -Wextra)
target_link_libraries(nexus_rpc PUBLIC nexus_net jsoncpp_lib)

enable_testing()
foreach(tgt buffer_test event_loop_test connection_test server_test)
    add_executable(${tgt} tests/net/${tgt}.cpp)
    target_link_libraries(${tgt} PRIVATE nexus_net GTest::gtest GTest::gtest_main)
    target_compile_options(${tgt} PRIVATE -Wall -Wextra)
    add_test(NAME ${tgt} COMMAND ${tgt})
endforeach()

foreach(tgt inet_test message_test requestor_test rpc_test)
    if(tgt STREQUAL "inet_test")
        set(src tests/net/inet_test.cpp)
    elseif(tgt STREQUAL "rpc_test")
        set(src tests/rpc/rpc_test.cpp)
    else()
        set(src tests/protocol/${tgt}.cpp)
    endif()
    add_executable(${tgt} ${src})
    target_include_directories(${tgt} PRIVATE tests)
    target_link_libraries(${tgt} PRIVATE nexus_rpc GTest::gtest GTest::gtest_main)
    target_compile_options(${tgt} PRIVATE -Wall -Wextra)
    add_test(NAME ${tgt} COMMAND ${tgt})
endforeach()

add_executable(echo_server demo/echo_server.cpp)
target_link_libraries(echo_server PRIVATE nexus_net)
add_executable(echo_client demo/echo_client.cpp)
target_link_libraries(echo_client PRIVATE nexus_net)

add_executable(add_sub_server demo/add_sub_server.cpp)
target_link_libraries(add_sub_server PRIVATE nexus_rpc)
add_executable(add_sub_client demo/add_sub_client.cpp)
target_link_libraries(add_sub_client PRIVATE nexus_rpc)
