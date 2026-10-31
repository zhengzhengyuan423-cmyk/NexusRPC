#include "net/event/event_loop.hpp"

namespace nexus {

// Channel 依赖 EventLoop 完整定义的方法
void Channel::Remove() { _loop->RemoveEvent(this); }
void Channel::Update() { _loop->UpdateEvent(this); }

// TimerWheel 的线程安全投递壳：统一转进属主 loop 执行
void TimerWheel::TimerAdd(uint64_t id, uint32_t delay, const TaskFunc &cb)
{
    _loop->RunInLoop(std::bind(&TimerWheel::TimerAddInLoop, this, id, delay, cb));
}

void TimerWheel::TimerRefresh(uint64_t id)
{
    _loop->RunInLoop(std::bind(&TimerWheel::TimerRefreshInLoop, this, id));
}

void TimerWheel::TimerCancel(uint64_t id)
{
    _loop->RunInLoop(std::bind(&TimerWheel::TimerCancelInLoop, this, id));
}

bool TimerWheel::HasTimer(uint64_t id)
{
    auto it = _timers.find(id);
    if (it == _timers.end()) return false;
    return true;
}

TimerWheel::TimerWheel(EventLoop *loop)
    : _capacity(60), _tick(0), _wheel(_capacity), _loop(loop),
      _timerfd(CreateTimerfd()), _timer_channel(new Channel(_loop, _timerfd))
{
    _timer_channel->SetReadCallback(std::bind(&TimerWheel::OnTime, this));
    _timer_channel->EnableRead();
}

TimerWheel::~TimerWheel()
{
    // 清轮必须发生在 _timers 销毁之前：~TimerTask 的 _release 回调要 RemoveTimer
    for (auto &slot : _wheel)
    {
        slot.clear();
    }
}

EventLoop::EventLoop()
    : _thread_id(std::this_thread::get_id()),
      _event_fd(CreateEventFd()),
      _event_channel(new Channel(this, _event_fd)),
      _timer_wheel(this)
{
    _event_channel->SetReadCallback(std::bind(&EventLoop::ReadEventfd, this));
    _event_channel->EnableRead();
}

void EventLoop::Start()
{
    _running = true;
    while (_running)
    {
        std::vector<Channel *> actives;
        _poller.Poll(&actives);
        for (auto &channel : actives)
        {
            channel->HandleEvent();
        }
        RunAllTask();
    }
}

void EventLoop::Stop()
{
    _running = false;
    if (!IsInLoop())
    {
        Wakeup();
    }
}

void EventLoop::RunInLoop(const TaskFunc &cb)
{
    if (IsInLoop())
    {
        return cb();
    }
    return QueueInLoop(cb);
}

void EventLoop::QueueInLoop(const TaskFunc &cb)
{
    {
        std::unique_lock<std::mutex> _lock(_mutex);
        _tasks.push_back(cb);
    }
    Wakeup();
}

} 
