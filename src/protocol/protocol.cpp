#include "protocol/protocol.hpp"
#include <arpa/inet.h>
#include <cassert>

namespace nexus {

// 帧头三字段固定 12 字节
static constexpr int32_t kHeaderSize = 12;
static constexpr int32_t kLenFieldSize = 4;

std::string Encode(uint32_t mtype, const std::string &id, const std::string &body)
{
    int32_t total = kHeaderSize - kLenFieldSize + (int32_t)id.size() + (int32_t)body.size();
    int32_t n_total = htonl(total);
    int32_t n_mtype = htonl((int32_t)mtype);
    int32_t n_idlen = htonl((int32_t)id.size());

    std::string out;
    out.reserve(total + kLenFieldSize);
    out.append((char *)&n_total, kLenFieldSize);
    out.append((char *)&n_mtype, kLenFieldSize);
    out.append((char *)&n_idlen, kLenFieldSize);
    out.append(id);
    out.append(body);
    return out;
}

FrameState PeekFrame(Buffer &buf)
{
    if (buf.ReadableSize() < (uint64_t)kLenFieldSize)
        return FrameState::NEED_MORE;

    // 先只 peek 长度头：可读字节不足 12 时 peek 整帧头会超读
    int32_t n_total = 0;
    buf.Read(&n_total, kLenFieldSize);
    int32_t total = ntohl(n_total);
    if (total < kHeaderSize - kLenFieldSize || total > kMaxFrameSize)
        return FrameState::BAD;
    if (buf.ReadableSize() < (uint64_t)total + kLenFieldSize)
        return FrameState::NEED_MORE;

    // 走到这里整帧已在手里：可读 >= 4+total >= 12，peek 全头安全
    int32_t hdr[3];
    buf.Read(hdr, sizeof(hdr));
    int32_t idlen = ntohl(hdr[2]);
    if (idlen < 0 || idlen > kMaxIdSize || total < kHeaderSize - kLenFieldSize + idlen)
        return FrameState::BAD;
    return FrameState::GOOD;
}

bool ParseFrame(Buffer &buf, uint32_t &mtype, std::string &id, std::string &body)
{
    assert(PeekFrame(buf) == FrameState::GOOD);

    int32_t hdr[3];
    buf.ReadAndPop(hdr, sizeof(hdr));
    int32_t total = ntohl(hdr[0]);
    int32_t idlen = ntohl(hdr[2]);

    id = buf.ReadStringAndPop(idlen);
    body = buf.ReadStringAndPop(total - (kHeaderSize - kLenFieldSize) - idlen);
    mtype = (uint32_t)ntohl(hdr[1]);
    return true;
}

}
