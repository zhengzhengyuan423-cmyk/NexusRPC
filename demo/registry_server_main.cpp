// 用法: ./registry_server_main [port]
#include <csignal>
#include <cstdlib>
#include "logger.hpp"
#include "registry/registry_server.hpp"

using namespace nexus;

static RegistryServer *g_srv = nullptr;
static void OnStop(int) { if (g_srv) g_srv->Stop(); }

int main(int argc, char *argv[])
{
    int port = argc > 1 ? atoi(argv[1]) : 19890;
    RegistryServer srv(port, NetOptions{1, 2, 0});
    g_srv = &srv;
    signal(SIGINT, OnStop);
    signal(SIGTERM, OnStop);
    ILOG << "registry_server on " << port;
    srv.Start(); // 阻塞直至关停
    ILOG << "registry_server exited cleanly";
    return 0;
}
