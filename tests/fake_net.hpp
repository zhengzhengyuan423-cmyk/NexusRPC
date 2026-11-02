#pragma once
#include <mutex>
#include <string>
#include <utility>
#include <vector>
#include "net/api/inet.hpp"

// 把 Send 的字节记下来供断言
class FakeNet : public nexus::INet
{
public:
    void SetOnMessage(BytesFn) override {}
    void SetOnClose(CloseFn) override {}
    void SetOnConnect(ConnFn) override {}
    bool Send(uint64_t conn_id, const std::string &data) override
    {
        std::lock_guard<std::mutex> lk(_mutex);
        _sent.emplace_back(conn_id, data);
        return _alive;
    }
    void Close(uint64_t) override {}
    void After(uint64_t, int, TimerFn) override {}
    void CancelTimer(uint64_t) override {}

    void SetAlive(bool a) { _alive = a; }
    std::vector<std::pair<uint64_t, std::string>> Sent()
    {
        std::lock_guard<std::mutex> lk(_mutex);
        return _sent;
    }

private:
    std::mutex _mutex;
    std::vector<std::pair<uint64_t, std::string>> _sent;
    bool _alive = true;
};
