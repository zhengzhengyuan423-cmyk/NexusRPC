#include "net/conn/buffer.hpp"
#include "net/logger.hpp"

namespace nexus {

void Buffer::EnsureWriteSpace(uint64_t len)
{
    if (TailIdleSize() >= len) return;
    if (len <= TailIdleSize() + HeadIdleSize())
    {
        // 头尾拼得下：数据整体前移，复用头部
        uint64_t rsz = ReadableSize();
        std::copy(ReadPosition(), ReadPosition() + rsz, Begin());
        _reader_idx = 0;
        _writer_idx = rsz;
    }
    else
    {
        // 扩容只增不缩：高水位即容量
        DLOG << "RESIZE" << _writer_idx + len;
        _buffer.resize(_writer_idx + len);
    }
}

void Buffer::Write(const void *data, uint64_t len)
{
    if (len == 0) return;
    EnsureWriteSpace(len);
    const char *d = (const char *)data;
    std::copy(d, d + len, WritePosition());
}

void Buffer::Read(void *buf, uint64_t len)
{
    assert(len <= ReadableSize());
    std::copy(ReadPosition(), ReadPosition() + len, (char *)buf);
}

std::string Buffer::ReadAsString(uint64_t len)
{
    assert(len <= ReadableSize());
    std::string str;
    str.resize(len);
    Read(&str[0], len);
    return str;
}

char *Buffer::FindCRLF()
{
    char *res = (char *)memchr(ReadPosition(), '\n', ReadableSize());
    return res;
}

std::string Buffer::GetLine()
{
    char *pos = FindCRLF();
    if (pos == NULL) return "";
    return ReadAsString(pos - ReadPosition() + 1);
}

} 
