#pragma once
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <memory>
#include <mutex>
#include <ctime>
#include <cstdio>
#include <filesystem>
#include <unistd.h>

namespace nexus {

class Strategy
{
public:
    virtual ~Strategy() = default;
    virtual void SyncLog(const std::string &txt) = 0;
};

class ConsoleLogStrategy : public Strategy
{
public:
    void SyncLog(const std::string &txt) override
    {
        std::unique_lock<std::mutex> lock(_mutex);
        std::cout << txt << "\r\n";
    }

private:
    std::mutex _mutex;
};

class FileLogStrategy : public Strategy
{
public:
    FileLogStrategy() : _filepath("./log"), _filename("my.log")
    {
        std::unique_lock<std::mutex> lock(_mutex);
        if (_filepath.back() != '/')
            _filepath += "/";
        if (!std::filesystem::exists(_filepath))
        {
            std::error_code ec;
            std::filesystem::create_directories(_filepath, ec);
            if (ec)
                std::cerr << "create log dir failed: " << ec.message() << '\n';
        }
        _name = _filepath + _filename;
    }
    void SyncLog(const std::string &txt) override
    {
        std::unique_lock<std::mutex> lock(_mutex);
        std::ofstream out(_name.c_str(), std::ios::app);
        if (out.is_open())
        {
            out << txt << "\r\n";
            out.close();
        }
    }

private:
    std::string _filepath;
    std::string _filename;
    std::string _name;
    std::mutex _mutex;
};

enum class LogLevel
{
    DEBUG,
    INFO,
    WARNING,
    ERROR,
    FATAL
};

inline std::string Level2Str(LogLevel level)
{
    switch (level)
    {
    case LogLevel::DEBUG:   return "DEBUG";
    case LogLevel::INFO:    return "INFO";
    case LogLevel::WARNING: return "WARNING";
    case LogLevel::ERROR:   return "ERROR";
    case LogLevel::FATAL:   return "FATAL";
    default:                return "UNKNOWN";
    }
}

inline std::string GetTimeStamp()
{
    time_t curr = time(nullptr);
    struct tm curr_tm;
    localtime_r(&curr, &curr_tm);
    char timebuffer[128];
    snprintf(timebuffer, sizeof(timebuffer), "%4d-%02d-%02d %02d:%02d:%02d",
             curr_tm.tm_year + 1900, curr_tm.tm_mon + 1, curr_tm.tm_mday,
             curr_tm.tm_hour, curr_tm.tm_min, curr_tm.tm_sec);
    return timebuffer;
}

class Logger
{
public:
    class LoggerMessage
    {
    public:
        LoggerMessage(LogLevel level, const std::string &name, int line, Logger &log)
            : _log(log)
        {
            std::stringstream ss;
            ss << "[" << GetTimeStamp() << "]"
               << "[" << Level2Str(level) << "]"
               << "[" << getpid() << "]"
               << "[" << name << "]"
               << "[" << line << "]";
            _loginfo += ss.str();
        }
        template <class T>
        LoggerMessage &operator<<(const T &data)
        {
            std::stringstream ss;
            ss << data;
            _loginfo += ss.str();
            return *this;
        }

        ~LoggerMessage()
        {
            if (_log._s)
                _log._s->SyncLog(_loginfo);
        }

    private:
        Logger &_log;
        std::string _loginfo;
    };

    Logger() { EnableConsoleLogStrategy(); }
    LoggerMessage operator()(LogLevel level, const std::string &name, int line)
    {
        return LoggerMessage(level, name, line, *this);
    }

    void EnableConsoleLogStrategy() { _s = std::make_unique<ConsoleLogStrategy>(); }
    void EnableFileLogStrategy() { _s = std::make_unique<FileLogStrategy>(); }

private:
    std::unique_ptr<Strategy> _s;
};


inline Logger _logger;

} 

#define DLOG ::nexus::_logger(::nexus::LogLevel::DEBUG, __FILE__, __LINE__)
#define ILOG ::nexus::_logger(::nexus::LogLevel::INFO, __FILE__, __LINE__)
#define WLOG ::nexus::_logger(::nexus::LogLevel::WARNING, __FILE__, __LINE__)
#define ELOG ::nexus::_logger(::nexus::LogLevel::ERROR, __FILE__, __LINE__)
#define FLOG ::nexus::_logger(::nexus::LogLevel::FATAL, __FILE__, __LINE__)
