#pragma once
#include <string>
#include <cstddef>
#include <sys/types.h>

#define MAX_LISTEN 1024

namespace nexus {


class Socket
{
private:
    int _sockfd;
    bool BindAddr(const std::string &ip, uint16_t port);
    void ReuseAddr();

public:
    Socket() : _sockfd(-1) {}
    explicit Socket(int fd) : _sockfd(fd) {}
    ~Socket() { Close(); }
    Socket(const Socket &) = delete;
    Socket &operator=(const Socket &) = delete;
    Socket(Socket &&other) noexcept : _sockfd(other._sockfd) { other._sockfd = -1; }
    Socket &operator=(Socket &&other) noexcept
    {
        if (this != &other)
        {
            Close();
            _sockfd = other._sockfd;
            other._sockfd = -1;
        }
        return *this;
    }

    int Fd() { return _sockfd; }
    bool Create();
    // 放弃 fd 所有权，析构不 close（fd 移交给 Connection 等下家时用）
    void Detach() { _sockfd = -1; }
    void NonBlock();

    int Accept();

    // 阻塞语义：ret==0 对端关闭，-1 暂时无数据/出错
    ssize_t Recv(void *buf, size_t len, int flag = 0);
    ssize_t Send(const void *buf, size_t len, int flag = 0);
    // 非阻塞一次尝试：EAGAIN/EINTR 归一为 -1，0 仍是 EOF
    ssize_t TryRecv(void *buf, size_t len);
    ssize_t TrySend(const void *buf, size_t len);

    // 建监听：socket+重用+bind+listen，port 冲突返回 false
    bool Listen(uint16_t port, const std::string &ip = "0.0.0.0", bool nonblock_flag = false);
    // 建客户端连接（阻塞式）
    bool Connect(uint16_t port, const std::string &ip);

    void Close();
};

} 
