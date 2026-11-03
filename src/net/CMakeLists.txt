target_sources(nexus_net PRIVATE
    conn/buffer.cpp
    conn/socket.cpp
    event/poller.cpp
    event/event_loop.cpp
    conn/connection.cpp
    server/tcp_server.cpp
    server/tcp_client.cpp
    api/inet.cpp
)
