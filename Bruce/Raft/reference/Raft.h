#include "tools.h"

class Raft
{
public:
    static void *listenForVote(void *arg);      // 用于监听voteRPC的server线程
    static void *listenForAppend(void *arg);    // 用于监听appendRPC的server线程
    static void *processEntriesLoop(void *arg); // 持续处理日志同步的守护线程
    static void *electionLoop(void *arg);       // 持续处理选举的守护线程
    static void *callRequestVote(void *arg);    // 发voteRPC的线程
    static void *sendAppendEntries(void *arg);  // 发appendRPC的线程
    static void *applyLogLoop(void *arg);       // 持续向上层应用日志的守护线程
    // static void* apply(void* arg);
    // static void* save(void* arg);
    enum RAFT_STATE
    {
        LEADER = 0,
        CANDIDATE,
        FOLLOWER
    }; // 用枚举定义的raft三种状态
    void Make(vector<PeersInfo> peers, int id);               // raft初始化
    int getMyduration(timeval last);                          // 传入某个特定计算到当下的持续时间
    void setBroadcastTime();                                  // 重新设定BroadcastTime，成为leader发心跳的时候需要重置
    pair<int, bool> getState();                               // 在LAB3中会用到，提前留出来的接口判断是否leader
    RequestVoteReply requestVote(RequestVoteArgs args);       // vote的RPChandler
    AppendEntriesReply appendEntries(AppendEntriesArgs args); // append的RPChandler
    bool checkLogUptodate(int term, int index);               // 判断是否最新日志(两个准则)，vote时会用到
    void push_backLog(LogEntry log);                          // 插入新日志
    vector<LogEntry> getCmdAndTerm(string text);              // 用的RPC不支持传容器，所以封装成string，这个是解封装恢复函数
    StartRet start(Operation op);                             // 向raft传日志的函数，只有leader响应并立即返回，应用层用到
    void printLogs();

    void serialize();     // 序列化
    bool deserialize();   // 反序列化
    void saveRaftState(); // 持久化
    void readRaftState(); // 读取持久化状态
    bool isKilled();      //->check is killed?
    void kill();          // 设定raft状态为dead，LAB3B快照测试时会用到

private:
    locker m_lock; // 成员变量不一一注释了，基本在论文里都有，函数实现也不注释了，看过论文看过我写的函数说明
    cond m_cond;   // 自然就能理解了，不然要写太多了，这样整洁一点，注释了太乱了，论文才是最关键的
    vector<PeersInfo> m_peers;
    Persister persister;
    int m_peerId;
    int dead;

    // 需要持久化的data
    int m_curTerm;
    int m_votedFor;
    vector<LogEntry> m_logs;

    vector<int> m_nextIndex;
    vector<int> m_matchIndex;
    int m_lastApplied;
    int m_commitIndex;

    // unordered_map<int, int> m_firstIndexOfEachTerm;
    // vector<int> m_nextIndex;
    // vector<int> m_matchIndex;

    int recvVotes;
    int finishedVote;
    int cur_peerId;

    RAFT_STATE m_state;
    int m_leaderId;
    struct timeval m_lastWakeTime;
    struct timeval m_lastBroadcastTime;
};