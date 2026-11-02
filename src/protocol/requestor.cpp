#include "protocol/requestor.hpp"
#include <future>

namespace nexus {

std::string Requestor::NextRid()
{
    return "r-" + std::to_string(++_rid_seq);
}

Requestor::DescribePtr Requestor::Register(const std::string &rid, Way way, const Callback &cb)
{
    std::lock_guard<std::mutex> lock(_mutex);
    auto rd = std::make_shared<Describe>();
    rd->way = way;
    rd->cb = cb;
    auto ret = _pending.emplace(rid, rd);
    if (!ret.second)
        return DescribePtr(); // rid 重复：登记失败，发送方不许出门
    return rd;
}

Requestor::DescribePtr Requestor::GetAndDel(const std::string &rid)
{
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _pending.find(rid);
    if (it == _pending.end())
        return DescribePtr();
    auto rd = it->second;
    _pending.erase(it);
    return rd;
}

void Requestor::DropIfStill(const std::string &rid, const DescribePtr &rd)
{
    std::lock_guard<std::mutex> lock(_mutex);
    auto it = _pending.find(rid);
    if (it != _pending.end() && it->second == rd)
        _pending.erase(it);
}

void Requestor::Fulfill(const DescribePtr &rd, Message::ptr &rsp)
{
    if (rd->way == Way::CALLBACK)
    {
        if (rd->cb)
            rd->cb(rsp);
        return;
    }
    try
    {
        rd->promise.set_value(rsp);
    }
    catch (const std::future_error &)
    {
        // 已被兑现（超时摘表与回包竞争的残余窗口）：静默
    }
}

bool Requestor::Send(uint64_t conn_id, const std::string &wire, const std::string &rid,
                     int timeout_sec, Message::ptr &rsp)
{
    auto rd = Register(rid, Way::SYNC, nullptr);
    if (!rd)
        return false;
    if (!_net.Send(conn_id, wire))
    {
        DropIfStill(rid, rd); // C3：链路不在，就地撤登记
        return false;
    }

    auto fut = rd->promise.get_future();
    if (fut.wait_for(std::chrono::seconds(timeout_sec)) == std::future_status::timeout)
    {
        DropIfStill(rid, rd); // 先摘表再返回：迟到回包此后只会扑空
        return false;
    }
    rsp = fut.get();
    return rsp != nullptr; // 断线清算给的 nullptr 也走这里
}

bool Requestor::Send(uint64_t conn_id, const std::string &wire, const std::string &rid,
                     std::future<Message::ptr> &fut)
{
    auto rd = Register(rid, Way::FUTURE, nullptr);
    if (!rd)
        return false;
    if (!_net.Send(conn_id, wire))
    {
        DropIfStill(rid, rd);
        return false;
    }
    fut = rd->promise.get_future();
    return true;
}

bool Requestor::Send(uint64_t conn_id, const std::string &wire, const std::string &rid,
                     const Callback &cb, int timeout_sec)
{
    auto rd = Register(rid, Way::CALLBACK, cb);
    if (!rd)
        return false;
    if (!_net.Send(conn_id, wire))
    {
        DropIfStill(rid, rd);
        return false;
    }

    // 定时兜底：After 恰好执行一次后退役，任意线程可调；
    // weak 捕获：Requestor 先于定时器到点死掉时，任务醒来扑空即散
    uint64_t tid = ++_timer_seq;
    std::weak_ptr<Requestor> wp = shared_from_this();
    _net.After(tid, timeout_sec, [wp, rid]()
               {
                   if (auto s = wp.lock())
                       s->Timeout(rid);
               });
    return true;
}

void Requestor::Timeout(const std::string &rid)
{
    auto rd = GetAndDel(rid);
    if (!rd)
        return; // 已被回包兑现
    Message::ptr nothing;
    Fulfill(rd, nothing);
}

void Requestor::OnResponse(const std::string &rid, Message::ptr &msg)
{
    auto rd = GetAndDel(rid);
    if (!rd)
        return; // 迟到/未知 rid：静默丢
    Fulfill(rd, msg);
}

void Requestor::FailAll()
{
    std::vector<DescribePtr> doomed;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        doomed.reserve(_pending.size());
        for (auto &[rid, rd] : _pending)
            doomed.push_back(rd);
        _pending.clear();
    }
    Message::ptr nothing;
    for (auto &rd : doomed)
        Fulfill(rd, nothing); // 锁外兑现：cb 可能在 cb 里再发请求
}

}
