#pragma once
#include <vector>
#include <string>
#include <cassert>
#include <cstring>
#include <algorithm>

#define BUFFER_DEFAULT_SIZE 1024

namespace nexus {

// 字节环形缓冲：读写双偏移，归属使用它的 Connection
class Buffer
{
private:
    uint64_t _reader_idx; // 读偏移
    uint64_t _writer_idx; // 写偏移
    std::vector<char> _buffer;
public:
    Buffer() : _reader_idx(0), _writer_idx(0), _buffer(BUFFER_DEFAULT_SIZE) {}
    char *Begin() { return &*_buffer.begin(); }
    char *WritePosition() { return Begin() + _writer_idx; }
    char *ReadPosition() { return Begin() + _reader_idx; }

    // 尾部剩余空间
    uint64_t TailIdleSize() { return _buffer.size() - _writer_idx; }
    // 头部可回收空间
    uint64_t HeadIdleSize() { return _reader_idx; }
    // 可读数据量
    uint64_t ReadableSize() { return _writer_idx - _reader_idx; }

    void MoveReadOffset(uint64_t len)
    {
        if (len == 0)
            return;
        assert(len <= ReadableSize()); // 超读是编程错误，当场抓，不钳制
        _reader_idx += len;
    }
    void MoveWriteOffset(uint64_t len)
    {
        assert(len <= TailIdleSize());
        _writer_idx += len;
    }

    // 整体空闲够就前移压缩，不够就扩容（只增不缩）
    void EnsureWriteSpace(uint64_t len);

    // 写入（只写不推 / 写后推）
    void Write(const void *data, uint64_t len);
    void WriteAndPush(const void *data, uint64_t len)
    {
        Write(data, len);
        MoveWriteOffset(len);
    }
    void WriteString(const std::string &data) { return Write(data.c_str(), data.size()); }
    void WriteStringAndPush(const std::string &data)
    {
        WriteString(data);
        MoveWriteOffset(data.size());
    }
    void WriteBuffer(Buffer &data) { return Write(data.ReadPosition(), data.ReadableSize()); }
    void WriteBufferAndPush(Buffer &data)
    {
        WriteBuffer(data);
        MoveWriteOffset(data.ReadableSize());
    }

    // 读取（只读不取 / 边读边取）
    void Read(void *buf, uint64_t len);
    void ReadAndPop(void *buf, uint64_t len)
    {
        Read(buf, len);
        MoveReadOffset(len);
    }
    std::string ReadAsString(uint64_t len);
    std::string ReadStringAndPop(uint64_t len)
    {
        assert(len <= ReadableSize());
        std::string str = ReadAsString(len);
        MoveReadOffset(len);
        return str;
    }

    char *FindCRLF();
    // 取到 \n 为止的一行（含 \n）
    std::string GetLine();
    std::string GetLineAndPop()
    {
        std::string str = GetLine();
        MoveReadOffset(str.size());
        return str;
    }

    // 偏移归零即清空，数据原地留着等覆盖
    void Clear()
    {
        _reader_idx = 0;
        _writer_idx = 0;
    }
};

} 
