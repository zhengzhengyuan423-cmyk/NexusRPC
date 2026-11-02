#pragma once
#include <functional>
#include <memory>
#include <unordered_map>
#include <csignal>
#include "net/conn/acceptor.hpp"
#include "net/conn/connection.hpp"
#include "net/event/event_loop.hpp"
#include "net/server/loop_thread.hpp"
#include "net/server/biz_thread.hpp"
#include "logger.hpp"

namespace nexus {

// 多线程 TCP 服务器：baseloop 收连，从 loop 管 IO，业务可再交 biz 池。
// 连接与 loop 的归属在 NewConnection 时一次定死，之后终生不变
class TcpServer
{
private:
    uint64_t _next_id; // 自增连接 ID，兼作定时器 ID
    int _port;
    int _timeout;            // 空闲超时秒数
    bool _idle_release;      // 是否启用空闲超时释放
    bool _shutdown_done = false; // Shutdown 只执行一次（仅 baseloop 线程读写）
    EventLoop _baseloop;     // 主循环：监听 accept
    Acceptor _acceptor;
    LoopThreadPool _pool;    // 从属 EventLoop 线程池
    int _biz_threads;        // 业务线程数，0=业务就地跑在 io loop
    BizThreadPool _biz_pool;
    std::unordered_map<uint64_t, PtrConnection> _conns;
    std::unique_ptr<Channel> _signal_channel; // signalfd 事件通道

    using ConnectedCallback = std::function<void(const PtrConnection &)>;
    // 对外回调是值语义 string：biz>0 时在 biz worker 执行，biz=0 时在本连接属主 io loop 执行
    using MessageCallback = std::function<void(const PtrConnection &, std::string)>;
    using ClosedCallback = std::function<void(const PtrConnection &)>;
    using AnyEventCallback = std::function<void(const PtrConnection &)>;
    using Functor = std::function<void()>;
    // 连接准入过滤器：false 即拒绝（fd 由本类关闭）。网络层不含治理逻辑，判定由业务注入
    using AcceptFilter = std::function<bool(int fd)>;
    AcceptFilter _accept_filter;
    ConnectedCallback _connected_callback;
    MessageCallback _message_callback;
    ClosedCallback _closed_callback;
    AnyEventCallback _event_callback;

private:
    void RunAfterInLoop(const Functor &task, int delay);
    void NewConnection(int fd);
    void RemoveConnectionInLoop(const PtrConnection &conn);
    void RemoveConnection(const PtrConnection &conn);
    void HandleSignal(); // signalfd 可读：取出信号号并触发 Shutdown

public:
    TcpServer(int port);
    // 以下配置项必须在 Start 前设置
    void SetThreadCount(int count) { _pool.SetThreadCount(count); }
    void SetBizThreadCount(int count)
    {
        _biz_threads = count;
        _biz_pool.SetThreadCount(count);
    }
    void SetBizQueueSize(size_t size) { _biz_pool.SetQueueSize(size); }
    void SetConnectedCallback(const ConnectedCallback &cb) { _connected_callback = cb; }
    void SetMessageCallback(const MessageCallback &cb) { _message_callback = cb; }
    void SetClosedCallback(const ClosedCallback &cb) { _closed_callback = cb; }
    void SetAnyEventCallback(const AnyEventCallback &cb) { _event_callback = cb; }
    void SetAcceptFilter(const AcceptFilter &f) { _accept_filter = f; }
    void SetIdleTimeout(int timeout)
    {
        _timeout = timeout;
        _idle_release = true;
    }
    // 挂一个延迟 delay 秒的定时任务（在 baseloop 上执行）
    void RunAfter(const Functor &task, int delay)
    {
        _baseloop.RunInLoop(std::bind(&TcpServer::RunAfterInLoop, this, task, delay));
    }
    // 指定 id 的定时器：任意线程安全，TimerAdd 内部即投递
    void TimerAdd(uint64_t id, int sec, const Functor &task) { _baseloop.TimerAdd(id, sec, task); }
    void TimerCancel(uint64_t id) { _baseloop.TimerCancel(id); }
    // 把某连接的清理任务排在该连接已投递任务之后：biz>0 走同连接队列（FIFO），
    // biz=0 就地执行（io loop 本来串行）
    void PostConnCleanup(uint64_t conn_id, const Functor &task)
    {
        if (_biz_threads > 0)
            _biz_pool.Post(conn_id, task);
        else
            task();
    }
    // 监听 SIGINT/SIGTERM：信号被屏蔽后走 signalfd 进 baseloop，
    // 收到即在 loop 线程执行 Shutdown()——不在信号上下文里做任何非异步安全的事
    void CatchSignal();
    void Start();

    // 有序退出：摘监听 → 逐连接 Shutdown → 停 biz → 停从 loop → 停 base。
    // 必须在 baseloop 线程执行（CatchSignal/RunAfter 路径保证）
    void Shutdown();

    void ReportMetrics();
    // 仅 baseloop 自己的计数；全量用 TotalMetrics()
    LoopMetrics &Metrics() { return _baseloop.GetMetrics(); }
    // baseloop+从 loop 求和快照。可加的依据：连接终生归属一个 loop，
    // 各 loop 计数是划分不是重叠。count=0 时退化为 base 快照
    LoopMetrics::Snapshot TotalMetrics();

private:
    TcpServer(const TcpServer &) = delete;
    TcpServer &operator=(const TcpServer &) = delete;
};

} 
