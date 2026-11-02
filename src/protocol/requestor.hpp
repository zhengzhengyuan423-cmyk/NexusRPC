#pragma once
#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include "net/api/inet.hpp"
#include "protocol/message.hpp"

namespace nexus {

// 请求-响应匹配器：rid → 等待方 的登记表。三形态只是"等待发生在哪"。
// 完备性契约：每个登记过的请求要么被回包兑现，要么被超时/断线兑现——不许永等
class Requestor : public std::enable_shared_from_this<Requestor>
{
public:
    using Callback = std::function<void(Message::ptr)>; // 收到 nullptr = 超时或断线

    explicit Requestor(INet &net) : _net(net) {}

    // 同步：就地阻塞。超时先摘表再返回——迟到的回包会扑空，不会二次兑现
    bool Send(uint64_t conn_id, const std::string &wire, const std::string &rid,
              int timeout_sec, Message::ptr &rsp);
    // future：把等待凭证交给调用方，何时等、等多久归调用方
    bool Send(uint64_t conn_id, const std::string &wire, const std::string &rid,
              std::future<Message::ptr> &fut);
    // 回调：回包线程直接执行 cb；定时器到点用 nullptr 兑现
    bool Send(uint64_t conn_id, const std::string &wire, const std::string &rid,
              const Callback &cb, int timeout_sec);

    // 迟到或查无此 rid：静默丢
    void OnResponse(const std::string &rid, Message::ptr &msg);
    // 断线清算：pending 全部用 nullptr 兑现
    void FailAll();

    std::string NextRid();

private:
    enum class Way
    {
        SYNC,
        FUTURE,
        CALLBACK
    };
    struct Describe
    {
        Way way;
        std::promise<Message::ptr> promise;
        Callback cb;
    };
    using DescribePtr = std::shared_ptr<Describe>;

    DescribePtr Register(const std::string &rid, Way way, const Callback &cb);
    DescribePtr GetAndDel(const std::string &rid);
    void DropIfStill(const std::string &rid, const DescribePtr &rd);
    void Timeout(const std::string &rid);
    static void Fulfill(const DescribePtr &rd, Message::ptr &rsp);

    std::mutex _mutex;
    std::unordered_map<std::string, DescribePtr> _pending;
    INet &_net; // 发送与定时都走门面
    std::atomic<uint64_t> _rid_seq{0};
    std::atomic<uint64_t> _timer_seq{0}; // 定时器键位，允许任意线程取号
};

}
