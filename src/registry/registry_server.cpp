#include "registry/registry_server.hpp"
#include "protocol/protocol.hpp"

namespace nexus {

RegistryServer::RegistryServer(int port, NetOptions opts)
    : _net((uint16_t)port, opts)
{
    EnsureRegistryMessages();
    _pump.reset(new FramePump(
        _net,
        [this](uint64_t id, uint32_t mtype, const std::string &rid, const std::string &body)
        { return OnFrame(id, mtype, rid, body); }));
}

void RegistryServer::Stop()
{
    _net.Stop();
}

void RegistryServer::Start()
{
    _net.SetOnMessage([this](uint64_t id, const std::string &bytes)
                      { _pump->Feed(id, bytes); });
    _net.SetOnClose([this](uint64_t id)
                    { OnClosed(id); });
    _net.Start();
}

bool RegistryServer::OnFrame(uint64_t conn_id, uint32_t mtype,
                             const std::string &rid, const std::string &body)
{
    if (mtype != (uint32_t)MType::REQ_REG)
        return false; // 服务端只吃命令帧，回包/推送递过来=对面胡言，掐线
    Message::ptr msg = MessageFactory::Create(mtype);
    RegistryRequest::ptr req = std::dynamic_pointer_cast<RegistryRequest>(msg);
    if (!req)
        return false;
    req->SetRid(rid);
    req->SetType(MType::REQ_REG);
    if (!req->Unserialize(body) || !req->Check())
        return false;
    Handle(conn_id, rid, req);
    return true;
}

void RegistryServer::Handle(uint64_t conn_id, const std::string &rid, RegistryRequest::ptr &req)
{
    const std::string service = req->Service();
    const std::string instance = req->Instance();
    switch (req->Op())
    {
    case RegistryRequest::REGISTER:
    {
        if (instance.empty())
            return Respond(conn_id, rid, RCode::RCODE_INVALID_PARAMS, Json::Value());
        std::lock_guard<std::mutex> lk(_mutex);
        _leases[instance] = {service, conn_id};
        Respond(conn_id, rid, RCode::RCODE_OK, Json::Value()); // 先回执后广播
        Push(service, instance, true);
        return;
    }
    case RegistryRequest::HEARTBEAT:
    {
        RCode code = RCode::RCODE_NOT_FOUND_SERVICE;
        {
            std::lock_guard<std::mutex> lk(_mutex);
            if (_leases.count(instance))
                code = RCode::RCODE_OK;
        }
        Respond(conn_id, rid, code, Json::Value()); // NOT_FOUND 即"重新登记"信号
        return;
    }
    case RegistryRequest::LIST:
    {
        Json::Value arr(Json::arrayValue);
        {
            std::lock_guard<std::mutex> lk(_mutex);
            for (auto &[inst, e] : _leases)
                if (e.service == service)
                    arr.append(inst);
        }
        Respond(conn_id, rid, RCode::RCODE_OK, arr);
        return;
    }
    case RegistryRequest::WATCH:
    {
        std::lock_guard<std::mutex> lk(_mutex);
        _watchers[service].insert(conn_id);
        Respond(conn_id, rid, RCode::RCODE_OK, Json::Value());
        // 订阅即快照：同一把锁里从表现抄现推，快照永远不比表新
        for (auto &[inst, e] : _leases)
            if (e.service == service)
                Push(service, inst, true);
        return;
    }
    default:
        Respond(conn_id, rid, RCode::RCODE_INVALID_MSG, Json::Value());
    }
}

void RegistryServer::Respond(uint64_t conn_id, const std::string &rid,
                             RCode code, const Json::Value &data)
{
    RegistryResponse rsp;
    rsp.SetCode(code);
    rsp.SetData(data);
    _net.Send(conn_id, Encode((uint32_t)MType::RSP_REG, rid, rsp.Serialize()));
}

void RegistryServer::Push(const std::string &service, const std::string &instance, bool added)
{
    auto it = _watchers.find(service);
    if (it == _watchers.end())
        return;
    RegistryPush push;
    push.Set(service, instance, added);
    std::string wire = Encode((uint32_t)MType::PUSH_REG, "", push.Serialize());
    for (uint64_t c : it->second)
        _net.Send(c, wire); // 中途死掉的 watcher 只是 false（C3）
}

void RegistryServer::OnClosed(uint64_t conn_id)
{
    std::lock_guard<std::mutex> lk(_mutex);
    // 唯一驱逐路径：进程死（OS 关 fd）或显式 Stop，FIN 一到秒级生效；
    // 摘名与推 REMOVE 同锁原子，后到的快照不可能带出这具尸体
    for (auto it = _leases.begin(); it != _leases.end();)
    {
        if (it->second.conn == conn_id)
        {
            Push(it->second.service, it->first, false);
            it = _leases.erase(it);
        }
        else
            ++it;
    }
    for (auto &[service, conns] : _watchers)
        conns.erase(conn_id);
}

}
