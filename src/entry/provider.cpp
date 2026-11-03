#include "entry/provider.hpp"
#include <chrono>
#include "logger.hpp"

namespace nexus {

using namespace std::chrono;

Provider::Provider(const std::string &service, uint16_t rpc_port,
                   const std::string &reg_ip, uint16_t reg_port)
    : _service(service), _inst("127.0.0.1:" + std::to_string(rpc_port)),
      _server(rpc_port, NetOptions{1, 2, 0}), _reg(reg_ip, reg_port)
{
}

Provider::~Provider()
{
    Stop();
}

void Provider::RegisterService(const std::string &method,
                               const std::vector<rpc::ServiceDescribe::ParamsDescribe> &desc,
                               rpc::VType return_type,
                               const rpc::ServiceDescribe::ServiceCallback &handler)
{
    _server.RegisterService(method, desc, return_type, handler);
}

void Provider::Start()
{
    _running = true;
    _loop_th = std::thread([this] { _server.Start(); });
    while (!_reg.Connect() && _running)
        std::this_thread::sleep_for(milliseconds(200));
    if (!_running)
        return; // 半路被 Stop：服务线程归 Stop 清算，这里不接登记
    _reg.Register(_service, _inst);
    ILOG << "provider " << _inst << " online";
    _hb_th = std::thread(&Provider::HeartbeatLoop, this);
}

void Provider::HeartbeatLoop()
{
    while (_running)
    {
        // 心跳即自检：回包 NOT_FOUND（registry 重启过）或断线就地重登记
        if (!_reg.Heartbeat(_service, _inst) && _reg.Register(_service, _inst))
            ILOG << "provider " << _inst << " re-registered";
        for (int i = 0; i < 10 && _running; i++)
            std::this_thread::sleep_for(milliseconds(100));
    }
}

void Provider::Stop()
{
    if (!_running.exchange(false))
        return;
    _reg.Stop(); // FailAll 叫醒可能卡在 Heartbeat 里的心跳线程
    if (_hb_th.joinable())
        _hb_th.join();
    _server.Stop();
    if (_loop_th.joinable())
        _loop_th.join();
    ILOG << "provider " << _inst << " exited cleanly";
}

}
