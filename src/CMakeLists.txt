add_library(nexus_net STATIC)
target_include_directories(nexus_net PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_options(nexus_net PRIVATE -Wall -Wextra)
target_link_libraries(nexus_net PUBLIC Threads::Threads)

# 协议+RPC+注册中心+入口层
add_library(nexus_rpc STATIC)
target_compile_options(nexus_rpc PRIVATE -Wall -Wextra)
target_link_libraries(nexus_rpc PUBLIC nexus_net jsoncpp_lib)

# 库目标在此定义，源文件清单下放到各模块目录
add_subdirectory(net)
add_subdirectory(protocol)
add_subdirectory(rpc)
add_subdirectory(registry)
add_subdirectory(entry)
