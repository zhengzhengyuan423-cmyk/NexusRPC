#include "net/event/poller.hpp"
#include <cassert>
#include <cerrno>
#include <cstring>
#include <unistd.h>

namespace nexus {

Poller::Poller()
{
    _epfd = epoll_create(MAX_EPOLLEVENTS);
    if (_epfd < 0)
    {
        ELOG << "EPOLL CREATE FAILED!";
        abort();
    }
}

Poller::~Poller()
{
    if (_epfd >= 0) close(_epfd);
}

void Poller::Update(Channel *channel, int op)
{
    int fd = channel->Fd();
    struct epoll_event ev;
    ev.data.fd = fd;
    ev.events = channel->Events();
    int ret = epoll_ctl(_epfd, op, fd, &ev);
    if (ret < 0)
    {
        ELOG << "EPOLLCTL FAILED!";
    }
}

bool Poller::HasChannel(Channel *channel)
{
    auto it = _channels.find(channel->Fd());
    if (it == _channels.end())
    {
        return false;
    }
    return true;
}

void Poller::UpdateEvent(Channel *channel)
{
    bool ret = HasChannel(channel);
    if (ret == false)
    {
        _channels.insert(std::make_pair(channel->Fd(), channel));
        return Update(channel, EPOLL_CTL_ADD);
    }
    return Update(channel, EPOLL_CTL_MOD);
}

void Poller::RemoveEvent(Channel *channel)
{
    auto it = _channels.find(channel->Fd());
    if (it != _channels.end())
    {
        _channels.erase(it);
    }
    Update(channel, EPOLL_CTL_DEL);
}

void Poller::Poll(std::vector<Channel *> *active)
{
    int nfds = epoll_wait(_epfd, _evs, MAX_EPOLLEVENTS, -1);
    if (nfds < 0)
    {
        if (errno == EINTR)
        {
            return;
        }
        ELOG << "EPOLL WAIT ERROR:" << strerror(errno);
        abort();
    }
    for (int i = 0; i < nfds; i++)
    {
        auto it = _channels.find(_evs[i].data.fd);
        assert(it != _channels.end());
        it->second->SetREvents(_evs[i].events);
        active->push_back(it->second);
    }
}

} 
