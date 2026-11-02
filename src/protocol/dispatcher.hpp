#pragma once
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include "net/api/inet.hpp"
#include "protocol/message.hpp"

namespace nexus {

// 存储侧要基类指针（map 装得下），使用侧要具体消息类型（handler 签名带 shared_ptr<T>）
// 模板擦掉注册端类型，虚函数统一存储，派发时 cast 还原
class Callback
{
public:
    using ptr = std::shared_ptr<Callback>;
    virtual ~Callback() = default;
    virtual void OnMessage(INet &net, uint64_t conn_id, Message::ptr &msg) = 0;
};

template <typename T>
class CallbackT : public Callback
{
public:
    using Handler = std::function<void(INet &, uint64_t, std::shared_ptr<T> &)>;
    CallbackT(const Handler &h) : _handler(h) {}
    void OnMessage(INet &net, uint64_t conn_id, Message::ptr &msg) override
    {
        auto typed = std::dynamic_pointer_cast<T>(msg);
        if (!typed)
            return; 
        _handler(net, conn_id, typed);
    }

private:
    Handler _handler;
};

class Dispatcher
{
public:
    template <typename T>
    void Register(MType mtype, const typename CallbackT<T>::Handler &handler)
    {
        std::unique_lock<std::shared_mutex> lock(_mutex); // 写路径独占
        _handlers[mtype] = std::make_shared<CallbackT<T>>(handler);
    }


    bool Dispatch(INet &net, uint64_t conn_id, Message::ptr &msg)
    {
        Callback::ptr cb;
        {
            std::shared_lock<std::shared_mutex> lock(_mutex); // 读路径共享
            auto it = _handlers.find(msg->GetType());
            if (it != _handlers.end())
                cb = it->second;
        } 
        if (!cb)
            return false;
        cb->OnMessage(net, conn_id, msg);
        return true;
    }

private:
    std::shared_mutex _mutex;
    std::unordered_map<MType, Callback::ptr> _handlers;
};

}
