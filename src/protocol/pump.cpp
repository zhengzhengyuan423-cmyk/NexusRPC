#include "protocol/pump.hpp"
#include "logger.hpp"

namespace nexus {

void FramePump::Feed(uint64_t conn_id, const std::string &data)
{
    std::shared_ptr<Buffer> acc;
    {
        std::lock_guard<std::mutex> lock(_mutex);
        auto &slot = _accs[conn_id];
        if (!slot)
            slot = std::make_shared<Buffer>();
        acc = slot;
    }
    acc->WriteStringAndPush(data);

    while (true)
    {
        FrameState st = PeekFrame(*acc);
        if (st == FrameState::NEED_MORE)
            return; // 半包：残余留着，下个回调续上
        if (st == FrameState::BAD)
        {
            WLOG << "bad frame on conn " << conn_id;
            _net.Close(conn_id);
            return;
        }

        uint32_t mtype;
        std::string rid, body;
        ParseFrame(*acc, mtype, rid, body);
        if (!_handler(conn_id, mtype, rid, body))
        {
            _net.Close(conn_id); 
            return;
        }
    }
}

void FramePump::Forget(uint64_t conn_id)
{
    std::lock_guard<std::mutex> lock(_mutex);
    _accs.erase(conn_id);
}

}
