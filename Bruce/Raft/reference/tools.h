#include <iostream>
#include <bits/stdc++.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <chrono>
#include <unistd.h>
#include <fcntl.h>
#include "locker.h"
#include "./buttonrpc-master/buttonrpc.hpp"
// 需要结合LAB3实现应用层dataBase和Raft交互用的，通过getCmd()转化为applyMsg的command
// 实际上这只是LAB2的raft.hpp，在LAB3中改了很多，LAB4又改了不少，所以每个LAB都引了单独的raft.hpp
class Operation
{
public:
    string getCmd();
    string op;
    string key;
    string value;
    int clientId;
    int requestId;
};

string Operation::getCmd()
{
    string cmd = op + " " + key + " " + value;
    return cmd;
}

// 通过传入raft.start()得到的返回值，封装成类
class StartRet
{
public:
    StartRet() : m_cmdIndex(-1), m_curTerm(-1), isLeader(false) {}
    int m_cmdIndex;
    int m_curTerm;
    bool isLeader;
};

// 同应用层交互的需要提交到应用层并apply的封装成applyMsg的日志信息
class ApplyMsg
{
    bool CommandValid;
    string command;
    int CommandIndex;
};

// 一个存放当前raft的ID及自己两个RPC端口号的class(为了减轻负担，一个选举，一个日志同步，分开来)
class PeersInfo
{
public:
    pair<int, int> m_port;
    int m_peerId;
};

// 日志
class LogEntry
{
public:
    LogEntry(string cmd = "", int term = -1) : m_command(cmd), m_term(term) {}
    string m_command;
    int m_term;
};

// 持久化类，LAB2中需要持久化的内容就这3个，后续会修改
class Persister
{
public:
    vector<LogEntry> logs;
    int cur_term;
    int votedFor;
};

class AppendEntriesArgs
{
public:
    // AppendEntriesArgs():m_term(-1), m_leaderId(-1), m_prevLogIndex(-1), m_prevLogTerm(-1){
    //     //m_leaderCommit = 0;
    //     m_sendLogs.clear();
    // }
    int m_term;
    int m_leaderId;
    int m_prevLogIndex;
    int m_prevLogTerm;
    int m_leaderCommit;
    string m_sendLogs;
    friend Serializer &operator>>(Serializer &in, AppendEntriesArgs &d)
    {
        in >> d.m_term >> d.m_leaderId >> d.m_prevLogIndex >> d.m_prevLogTerm >> d.m_leaderCommit >> d.m_sendLogs;
        return in;
    }
    friend Serializer &operator<<(Serializer &out, AppendEntriesArgs d)
    {
        out << d.m_term << d.m_leaderId << d.m_prevLogIndex << d.m_prevLogTerm << d.m_leaderCommit << d.m_sendLogs;
        return out;
    }
};

class AppendEntriesReply
{
public:
    int m_term;
    bool m_success;
    int m_conflict_term;  // 用于冲突时日志快速匹配
    int m_conflict_index; // 用于冲突时日志快速匹配
};

class RequestVoteArgs
{
public:
    int term;
    int candidateId;
    int lastLogTerm;
    int lastLogIndex;
};

class RequestVoteReply
{
public:
    int term;
    bool VoteGranted;
};
