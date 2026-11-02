#pragma once
#include <cstdint>
#include <string>
#include "net/conn/buffer.hpp"

namespace nexus {

// LV 帧格式：[4B total_len][4B mtype][4B idlen][id][body]
// total_len 只数长度头之后的字节（mtype+idlen+id+body），不含头自己
enum class FrameState
{
    NEED_MORE, // 半包
    GOOD,      // 整帧齐且字段合法
    BAD        // 坏帧
};

static constexpr int32_t kMaxFrameSize = 1 << 16;
static constexpr int32_t kMaxIdSize = 1024;

std::string Encode(uint32_t mtype, const std::string &id, const std::string &body);

// 纯判定
FrameState PeekFrame(Buffer &buf);

// 解析
bool ParseFrame(Buffer &buf, uint32_t &mtype, std::string &id, std::string &body);

}
