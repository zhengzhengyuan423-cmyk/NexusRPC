#pragma once
#include <atomic>
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

// 请求-响应匹配器：rid → 等待方 的登记表

class Requestor
{
public:
    using Callback = std::function<void(Message::ptr)>; // 收到 nullptr = 断线

    explicit Requestor(INet &net) : _net(net) {}

    // 同步：就地阻塞，回包或断线才醒
    bool Send(uint64_t conn_id, const std::string &wire, const std::string &rid,
              Message::ptr &rsp);
    // future：把等待凭证交给调用方，何时等、等多久归调用方
    bool Send(uint64_t conn_id, const std::string &wire, const std::string &rid,
              std::future<Message::ptr> &fut);
    // 回调：回包线程直接执行 cb；断线用 nullptr 兑现
    bool Send(uint64_t conn_id, const std::string &wire, const std::string &rid,
              const Callback &cb);

  
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
    static void Fulfill(const DescribePtr &rd, Message::ptr &rsp);

    std::mutex _mutex;
    std::unordered_map<std::string, DescribePtr> _pending;
    INet &_net; 
    std::atomic<uint64_t> _rid_seq{0};
};

}
