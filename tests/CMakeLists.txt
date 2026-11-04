
foreach(tgt buffer_test event_loop_test connection_test server_test)
    add_executable(${tgt} net/${tgt}.cpp)
    target_link_libraries(${tgt} PRIVATE nexus_net GTest::gtest GTest::gtest_main)
    target_compile_options(${tgt} PRIVATE -Wall -Wextra)
    add_test(NAME ${tgt} COMMAND ${tgt})
endforeach()

add_executable(inet_test net/inet_test.cpp)
target_link_libraries(inet_test PRIVATE nexus_rpc GTest::gtest GTest::gtest_main)
target_compile_options(inet_test PRIVATE -Wall -Wextra)
add_test(NAME inet_test COMMAND inet_test)

foreach(tgt message_test requestor_test)
    add_executable(${tgt} protocol/${tgt}.cpp)
    target_include_directories(${tgt} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}) # fake_net.hpp
    target_link_libraries(${tgt} PRIVATE nexus_rpc GTest::gtest GTest::gtest_main)
    target_compile_options(${tgt} PRIVATE -Wall -Wextra)
    add_test(NAME ${tgt} COMMAND ${tgt})
endforeach()

add_executable(rpc_test rpc/rpc_test.cpp)
target_link_libraries(rpc_test PRIVATE nexus_rpc GTest::gtest GTest::gtest_main)
target_compile_options(rpc_test PRIVATE -Wall -Wextra)
add_test(NAME rpc_test COMMAND rpc_test)

add_executable(registry_test registry/registry_test.cpp)
target_link_libraries(registry_test PRIVATE nexus_rpc GTest::gtest GTest::gtest_main)
target_compile_options(registry_test PRIVATE -Wall -Wextra)
add_test(NAME registry_test COMMAND registry_test)

add_executable(entry_test entry/entry_test.cpp)
target_link_libraries(entry_test PRIVATE nexus_rpc GTest::gtest GTest::gtest_main)
target_compile_options(entry_test PRIVATE -Wall -Wextra)
add_test(NAME entry_test COMMAND entry_test)

add_executable(sentinel_test sentinel/sentinel_test.cpp)
target_link_libraries(sentinel_test PRIVATE nexus_rpc GTest::gtest GTest::gtest_main)
target_compile_options(sentinel_test PRIVATE -Wall -Wextra)
add_test(NAME sentinel_test COMMAND sentinel_test)
