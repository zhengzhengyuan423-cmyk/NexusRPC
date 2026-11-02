#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace nexus {

class INet
{
public:
    using BytesFn = std::function<void(uint64_t conn_id, const std::string &bytes)>;
    using CloseFn = std::function<void(uint64_t conn_id)>;
    using ConnFn = CloseFn;
    using TimerFn = std::function<void()>;

    virtual ~INet() = default;

    // Start/Connect 之前注册。未设 on_message = 进线字节吞掉。
    // 回调在 net 线程执行：不许长阻塞，重活自己开线程
    virtual void SetOnMessage(BytesFn fn) = 0;
    virtual void SetOnClose(CloseFn fn) = 0;
    // 建链事件：先于该 conn 的首条 on_message。未注册=丢弃
    virtual void SetOnConnect(ConnFn fn) = 0;

    // data 由 net 层内部拷贝，调用完立即可回收
    virtual bool Send(uint64_t conn_id, const std::string &data) = 0;
    virtual void Close(uint64_t conn_id) = 0; // 半关闭，幂等

    // 秒级定时器：cb 恰好执行一次后自动退役；执行前可 Cancel。
    // 同一 timer_id 未到期时重复 After = 覆盖旧任务（一 id 一槽）。cb 在 net 线程跑。
    virtual void After(uint64_t timer_id, int sec, TimerFn cb) = 0;
    virtual void CancelTimer(uint64_t timer_id) = 0;
};

// 调优旋钮不是接口行为，改数值不改变契约语义
struct NetOptions
{
    int io_threads = 1;        // 从 loop 数；0 = base loop 亲自管连接
    int biz_threads = 0;       // >0：on_message 在 biz 队列执行，C1 靠同 conn 同队列兑现
    int idle_release_sec = 0;  // 0 = 不启用闲置摘除
};

class NetServer : public INet
{
public:
    NetServer(uint16_t port, NetOptions opts = {});
    ~NetServer() override;

    void SetOnMessage(BytesFn fn) override;
    void SetOnClose(CloseFn fn) override;
    void SetOnConnect(ConnFn fn) override;
    bool Send(uint64_t conn_id, const std::string &data) override;
    void Close(uint64_t conn_id) override;
    void After(uint64_t timer_id, int sec, TimerFn cb) override;
    void CancelTimer(uint64_t timer_id) override;

    void Start(); 
    void Stop();

private:
    class Impl;
    std::unique_ptr<Impl> _impl;
};

class NetClient : public INet
{
public:
    NetClient(const std::string &ip, uint16_t port);
    ~NetClient() override;

    void SetOnMessage(BytesFn fn) override;
    void SetOnClose(CloseFn fn) override;
    void SetOnConnect(ConnFn fn) override;
    bool Send(uint64_t conn_id, const std::string &data) override;
    void Close(uint64_t conn_id) override;
    void After(uint64_t timer_id, int sec, TimerFn cb) override;
    void CancelTimer(uint64_t timer_id) override;

    bool Connect();      // 同步建链；失败可重调；Stop 之后重调=重连
    uint64_t Id() const; 
    void Stop();

private:
    class Impl;
    std::unique_ptr<Impl> _impl;
};

}
