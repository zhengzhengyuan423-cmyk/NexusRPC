#include "net/conn/socket.hpp"
#include "logger.hpp"
#include <cstring>
#include <cerrno>
#include <fcntl.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>

namespace nexus {

bool Socket::BindAddr(const std::string &ip, uint16_t port)
{
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = inet_addr(ip.c_str());
    socklen_t len = sizeof(struct sockaddr_in);
    int ret = bind(_sockfd, (struct sockaddr *)&addr, len);
    if (ret < 0)
    {
        ELOG << "BIND ADDRESS FAILED!";
        return false;
    }
    return true;
}

void Socket::ReuseAddr()
{
    int val = 1;
    setsockopt(_sockfd, SOL_SOCKET, SO_REUSEADDR, (void *)&val, sizeof(int));
    val = 1;
    setsockopt(_sockfd, SOL_SOCKET, SO_REUSEPORT, (void *)&val, sizeof(int));
}

bool Socket::Create()
{
    _sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (_sockfd < 0)
    {
        ELOG << "CREATE SOCKET FAILED!";
        return false;
    }
    return true;
}

void Socket::NonBlock()
{
    int flag = fcntl(_sockfd, F_GETFL, 0);
    fcntl(_sockfd, F_SETFL, flag | O_NONBLOCK);
}

int Socket::Accept()
{
    int newfd = accept(_sockfd, NULL, NULL);
    if (newfd < 0)
    {
        if (errno != EAGAIN && errno != EWOULDBLOCK)
            ELOG << "SOCKET ACCEPT FAILED!";
        return -1;
    }
    // accept 不继承监听 fd 的 O_NONBLOCK，新 fd 必须显式置非阻塞
    int flag = fcntl(newfd, F_GETFL, 0);
    fcntl(newfd, F_SETFL, flag | O_NONBLOCK);
    return newfd;
}

ssize_t Socket::Recv(void *buf, size_t len, int flag)
{
    ssize_t ret = recv(_sockfd, buf, len, flag);
    if (ret == 0) return 0; // 对端关闭
    if (ret < 0)
    {
        if (errno == EAGAIN || errno == EINTR) return -1;
        ELOG << "SOCKET RECV FAILED!";
        return -1;
    }
    return ret;
}

ssize_t Socket::Send(const void *buf, size_t len, int flag)
{
    // MSG_NOSIGNAL：对端已关时 send 返回 EPIPE，而不是用 SIGPIPE 杀死进程
    ssize_t ret = send(_sockfd, buf, len, flag | MSG_NOSIGNAL);
    if (ret < 0)
    {
        if (errno == EAGAIN || errno == EINTR) return -1;
        ELOG << "SOCKET SEND FAILED!";
        return -1;
    }
    return ret;
}

ssize_t Socket::TryRecv(void *buf, size_t len)
{
    return Recv(buf, len, MSG_DONTWAIT);
}

ssize_t Socket::TrySend(const void *buf, size_t len)
{
    if (len == 0) return 0;
    return Send(buf, len, MSG_DONTWAIT);
}

bool Socket::Listen(uint16_t port, const std::string &ip, bool nonblock_flag)
{
    if (Create() == false) return false;
    ReuseAddr(); // 必须在 Bind 之前
    if (nonblock_flag) NonBlock();
    if (BindAddr(ip, port) == false) return false;
    if (listen(_sockfd, MAX_LISTEN) < 0)
    {
        ELOG << "SOCKET LISTEN FAILED!";
        return false;
    }
    return true;
}

bool Socket::Connect(uint16_t port, const std::string &ip)
{
    if (Create() == false) return false;
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = inet_addr(ip.c_str());
    socklen_t len = sizeof(struct sockaddr_in);
    int ret = connect(_sockfd, (struct sockaddr *)&addr, len);
    if (ret < 0)
    {
        ELOG << "CONNECT SERVER FAILED!";
        return false;
    }
    return true;
}

void Socket::Close()
{
    if (_sockfd != -1)
    {
        close(_sockfd);
        _sockfd = -1;
    }
}

} 
