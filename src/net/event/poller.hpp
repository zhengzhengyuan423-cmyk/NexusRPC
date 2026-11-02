#pragma once
#include <vector>
#include <unordered_map>
#include <sys/epoll.h>
#include "net/event/channel.hpp"
#include "logger.hpp"

#define MAX_EPOLLEVENTS 1024

namespace nexus {

// epoll 薄封装，归属一个 EventLoop 线程
class Poller
{
private:
    int _epfd;
    struct epoll_event _evs[MAX_EPOLLEVENTS];
    std::unordered_map<int, Channel *> _channels;

private:
    // 对 epoll 的直接操作
    void Update(Channel *channel, int op);
    bool HasChannel(Channel *channel);

public:
    Poller();
    ~Poller();
    // 添加或修改监控事件
    void UpdateEvent(Channel *channel);
    // 移除监控
    void RemoveEvent(Channel *channel);
    // 阻塞等待，回填就绪 Channel
    void Poll(std::vector<Channel *> *active);
};

} 
