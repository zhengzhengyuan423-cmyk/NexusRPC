#include "registry/registry_client.hpp"
#include "protocol/protocol.hpp"

namespace nexus {

RegistryClient::RegistryClient(const std::string &ip, uint16_t port) : _net(ip, port)
{
    EnsureRegistryMessages();
    _requestor = std::make_shared<Requestor>(_net);
    _pump.reset(new FramePump(
        _net,
        [this](uint64_t id, uint32_t mtype, const std::string &rid, const std::string &body)
        { return OnFrame(id, mtype, rid, body); }));
    _net.SetOnMessage([this](uint64_t id, const std::string &bytes)
                      { _pump->Feed(id, bytes); });
    _net.SetOnClose([this](uint64_t)
                    { _requestor->FailAll(); });
}

RegistryClient::~RegistryClient() { Stop(); }

bool RegistryClient::Connect() { return _net.Connect(); }

void RegistryClient::Stop()
{
    _requestor->FailAll();
    _net.Stop();
}

bool RegistryClient::OnFrame(uint64_t, uint32_t mtype,
                             const std::string &rid, const std::string &body)
{
    Message::ptr msg = MessageFactory::Create(mtype);
    if (!msg)
        return false;
    if (mtype == (uint32_t)MType::PUSH_REG)
    {
        if (!msg->Unserialize(body))
            return false;
        RegistryPush::ptr push = std::dynamic_pointer_cast<RegistryPush>(msg);
        if (_on_push && push)
            _on_push(push->Service(), push->Instance(), push->Added());
        return true;
    }
    if (mtype != (uint32_t)MType::RSP_REG)
        return false; 
    if (!msg->Unserialize(body))
        return false;
    _requestor->OnResponse(rid, msg);
    return true;
}

bool RegistryClient::Call(int op, const std::string &service, const std::string &instance,
                          RCode &code, Json::Value &data)
{
    std::string rid = _requestor->NextRid();
    RegistryRequest req;
    req.SetOp((RegistryRequest::Kind)op);
    req.SetService(service);
    req.SetInstance(instance);
    std::string wire = Encode((uint32_t)MType::REQ_REG, rid, req.Serialize());

    Message::ptr rsp;
    if (!_requestor->Send(_net.Id(), wire, rid, rsp))
        return false;
    RegistryResponse::ptr typed = std::dynamic_pointer_cast<RegistryResponse>(rsp);
    if (!typed)
        return false;
    code = typed->Code();
    data = typed->Data();
    return true;
}

bool RegistryClient::Register(const std::string &service, const std::string &instance)
{
    RCode c;
    Json::Value d;
    return Call(RegistryRequest::REGISTER, service, instance, c, d) && c == RCode::RCODE_OK;
}

bool RegistryClient::Heartbeat(const std::string &service, const std::string &instance)
{
    RCode c;
    Json::Value d;
    return Call(RegistryRequest::HEARTBEAT, service, instance, c, d) && c == RCode::RCODE_OK;
}

bool RegistryClient::List(const std::string &service, std::vector<std::string> &out)
{
    RCode c;
    Json::Value d;
    out.clear();
    if (!Call(RegistryRequest::LIST, service, "", c, d) || c != RCode::RCODE_OK)
        return false;
    for (auto &v : d)
        out.push_back(v.asString());
    return true;
}

bool RegistryClient::Watch(const std::string &service)
{
    RCode c;
    Json::Value d;
    return Call(RegistryRequest::WATCH, service, "", c, d) && c == RCode::RCODE_OK;
}

}
