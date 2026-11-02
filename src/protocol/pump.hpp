#pragma once
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include "net/api/inet.hpp"
#include "net/conn/buffer.hpp"
#include "protocol/protocol.hpp"

namespace nexus {

class FramePump
{
public:
    using FrameHandler = std::function<bool(uint64_t conn_id, uint32_t mtype,
                                            const std::string &rid, const std::string &body)>;

    FramePump(INet &net, const FrameHandler &handler) : _net(net), _handler(handler) {}

    void Feed(uint64_t conn_id, const std::string &data);
    // 清掉一条连接的累积状态
    void Forget(uint64_t conn_id);

private:
    INet &_net;
    std::mutex _mutex; 
    std::unordered_map<uint64_t, std::shared_ptr<Buffer>> _accs;
    FrameHandler _handler;
};

}
