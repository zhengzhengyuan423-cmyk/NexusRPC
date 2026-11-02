#pragma once
#include <string>
#include <unordered_map>

namespace nexus {

// 帧种类
enum class MType
{
    REQ_RPC = 0,
    RSP_RPC,
};

enum class RCode
{
    RCODE_OK = 0,
    RCODE_PARSE_FAILED,
    RCODE_ERROR_MSGTYPE,
    RCODE_INVALID_MSG,
    RCODE_DISCONNECTED,
    RCODE_INVALID_PARAMS,
    RCODE_NOT_FOUND_SERVICE,
    RCODE_INTERNAL_ERROR,
};

inline std::string ErrReason(RCode code)
{
    static std::unordered_map<RCode, std::string> err_map = {
        {RCode::RCODE_OK, "success"},
        {RCode::RCODE_PARSE_FAILED, "parse failed"},
        {RCode::RCODE_ERROR_MSGTYPE, "bad mtype"},
        {RCode::RCODE_INVALID_MSG, "invalid msg"},
        {RCode::RCODE_DISCONNECTED, "disconnected"},
        {RCode::RCODE_INVALID_PARAMS, "invalid params"},
        {RCode::RCODE_NOT_FOUND_SERVICE, "service not found"},
        {RCode::RCODE_INTERNAL_ERROR, "internal error"},
    };
    auto it = err_map.find(code);
    return it == err_map.end() ? "unknown error" : it->second;
}

enum class RType
{
    REQ_SYNC = 0,
    REQ_ASYNC,
    REQ_CALLBACK,
};

// JSON 字段名：constexpr 常量替代裸宏
namespace key
{
    inline constexpr char METHOD[] = "method";
    inline constexpr char PARAMS[] = "parameters";
    inline constexpr char RCODE[] = "rcode";
    inline constexpr char RESULT[] = "result";
} 

}
