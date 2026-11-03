foreach(demo echo_server echo_client)
    add_executable(${demo} ${demo}.cpp)
    target_link_libraries(${demo} PRIVATE nexus_net)
endforeach()

foreach(demo add_sub_server add_sub_client registry_server_main provider_main consumer_main)
    add_executable(${demo} ${demo}.cpp)
    target_link_libraries(${demo} PRIVATE nexus_rpc)
endforeach()
