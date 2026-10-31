// 用法示例：nexus echo 客户端（阻塞 socket，发三条收三条）
#include <iostream>
#include "net/conn/socket.hpp"

using namespace nexus;

int main(int argc, char *argv[])
{
    int port = argc > 1 ? std::atoi(argv[1]) : 9527;
    Socket cli;
    if (!cli.Connect(port, "127.0.0.1"))
    {
        std::cerr << "connect failed" << std::endl;
        return 1;
    }
    for (int i = 1; i <= 3; i++)
    {
        std::string msg = "hello-" + std::to_string(i) + "\n";
        cli.Send(msg.c_str(), msg.size());
        char buf[128] = {0};
        ssize_t ret = cli.Recv(buf, sizeof(buf) - 1);
        if (ret <= 0)
        {
            std::cerr << "recv failed, ret=" << ret << std::endl;
            return 1;
        }
        std::cout << "echo back: " << buf;
    }
    return 0;
}
