#include <iostream>
#include <vector>
#include <set>
#include <string>
#include <list>
#include <thread>
#include "./buttonrpc-master/buttonrpc.hpp"
#include <bits/stdc++.h>
#include <mutex>
#include <chrono>

using namespace std;

#define map_timer_ 2
#define reduce_timer_ 3

class Master
{
public:
    // 构造函数
    Master(int map_tasks, int reduce_tasks) : m_map_tasks(map_tasks),
                                              m_reduce_tasks(reduce_tasks),
                                              cur_map_task(0),
                                              cur_reduce_task(0){};

    // 获取mapwork任务数量
    int get_mapwork_num();

    // 获取reduceworker任务数量
    int get_reducework_num();

    // 获取文件名
    void get_files(int argc, char *argv[]);

    // map worker 完成任务后记录
    bool mapTasksHaveDone(string map_tasks);

    // reduce worker 完成任务后记录
    bool reduceTasksHaveDone(string reduce_tasks);

    // 获取map任务
    string get_map_tasks();

    // 获取reduce任务
    string get_reduce_tasks();

    // 判断map任务是否都完成
    bool is_map_done();

    // 判断所有map和reduce任务都完成
    bool is_all_done();

    // 判断reduce任务是否都完成
    bool is_reduce_done();

    // 计时map线程函数
    void map_timer();

    // 计时reduce线程函数
    void reduce_timer();

private:
    // 设置锁
    mutex m_mutex;

    // 设置map worker和reduce worker的数量
    int m_map_tasks;
    int m_reduce_tasks;

    // map 和 reduce 任务指针
    int cur_map_task;
    int cur_reduce_task;

    // 存储map文件名列表和文件数量
    list<string> m_files;
    int fileNum;

    // 存储reduce文件名列表和文件数量
    list<string> reduce_files;
    int reduce_fileNum;

    // 正在运行中的map任务
    vector<string> running_map_tasks;

    // 正在运行中的reduce任务
    vector<string> running_reduce_tasks;

    // 完成的map任务
    multiset<string> finished_map_tasks;

    // 完成的reduce任务
    multiset<string> finished_reduce_tasks;
};

/**
 * @brief 记录map任务完成
 * @param map任务的名称
 * @return NULL
 */

bool Master::mapTasksHaveDone(string map_tasks)
{
    lock_guard<mutex> lock(m_mutex);
    finished_map_tasks.insert(map_tasks);
    return true;
}

/**
 * @brief 记录reduce任务完成
 * @param reduce任务的名称
 * @return NULL
 */

bool Master::reduceTasksHaveDone(string reduce_tasks)
{
    lock_guard<mutex> lock(m_mutex);
    finished_reduce_tasks.insert(reduce_tasks);
    return true;
}

/**
 * @brief 获取map任务数量
 * @param 无
 * @return 返回map任务数量
 *
 * */
int Master::get_mapwork_num()
{
    return m_map_tasks;
}

/***
 *@brief 获取reduce任务数量
 *@param 无
 *@return 返回reduce任务数量
 *
 */
int Master::get_reducework_num()
{
    return m_reduce_tasks;
}

/**
 * @brief 获取文件名
 * @param argc, argv 是直接从main函数传过来的两个参数
 * @return NULL
 *
 **/

void Master::get_files(int argc, char *argv[])
{

    // 注意此处是将main函数的所有参数都传过来了，第一个参数是程序的名字
    for (int i = 1; i < argc; i++)
    {
        // 将文件名都存在m_files中
        m_files.push_back(argv[i]);
    }
    fileNum = m_files.size();

    // 顺便在这里初始化一下reduce任务

    for (int i = 0; i < m_reduce_tasks; i++)
    {
        reduce_files.emplace_back("./reduce-" + to_string(i));
    }
    reduce_fileNum = m_reduce_tasks;
}

/**
 * @brief 计时map线程函数
 * @param 传入对象指针
 * @return NULL
 *
 **/

void Master::map_timer()
{

    // map 任务计时
    std::this_thread::sleep_for(std::chrono::seconds(map_timer_));

    // 计时完成判断map任务是否完成
    m_mutex.lock();
    string map_task = running_map_tasks[cur_map_task++];
    if (finished_map_tasks.find(map_task) == finished_map_tasks.end())
    {
        // 如果没有完成则将任务重新分配
        m_files.push_back(map_task);
    }
    m_mutex.unlock();
}

/**
 * @brief 计时reduce线程函数
 * @param 传入对象指针
 * @return NULL
 *
 **/

void Master::reduce_timer()
{

    // map 任务计时
    std::this_thread::sleep_for(std::chrono::seconds(reduce_timer_));

    // 计时完成判断map任务是否完成
    m_mutex.lock();
    string reduce_task = running_reduce_tasks[cur_reduce_task++];
    if (finished_reduce_tasks.find(reduce_task) == finished_reduce_tasks.end())
    {
        // 如果没有完成则将任务重新分配
        reduce_files.push_back(reduce_task);
    }
    m_mutex.unlock();
}

/**
 * @brief 获取map任务
 * @param NULL
 * @return 返回map任务
 */

string Master::get_map_tasks()
{
    // 如果任务都完成则将返回empty
    if (Master::is_map_done())
    {
        return "empty";
    }
    string task_name;
    // 如果任务没有完成，则将任务分配给worker
    lock_guard<mutex> lock(m_mutex);
    if (!m_files.empty())
    {
        task_name = m_files.front();
        m_files.pop_front();
        running_map_tasks.push_back(task_name);
        // 此处调用计时线程
        //  创建计时线程
        thread timer_thread(&Master::map_timer, this);
        timer_thread.detach();
        return task_name;
    }
    else
    {
        return "empty";
    }
}

string Master::get_reduce_tasks()
{
    // 如果任务都完成则将返回empty
    if (Master::is_reduce_done())
    {
        return "empty";
    }
    string task_name;
    // 如果任务没有完成，则将任务分配给worker
    lock_guard<mutex> lock(m_mutex);
    if (!reduce_files.empty())
    {
        task_name = reduce_files.front();
        reduce_files.pop_front();
        running_reduce_tasks.push_back(task_name);

        // 此处调用计时线程
        //  创建计时线程
        thread timer_thread(&Master::reduce_timer, this);
        timer_thread.detach();
        return task_name;
    }
    else
    {
        return "empty";
    }
}

/**
 * @brief 判断map任务是否都完成
 * @param NULL
 * @return 返回是否完成
 *
 **/

bool Master::is_map_done()
{
    lock_guard<mutex> lock(m_mutex);
    return finished_map_tasks.size() == fileNum;
}

/**
 * @brief 判断reuuce任务是否都完成
 * @param NULL
 * @return 返回是否完成
 *
 *
 */

bool Master::is_reduce_done()
{
    lock_guard<mutex> lock(m_mutex);
    return finished_reduce_tasks.size() == reduce_fileNum;
}

/**
 * @brief 判断所有任务是否都完成
 * @param NULL
 * @return 返回是否完成
 *
 *
 */
bool Master::is_all_done()
{
    return Master::is_reduce_done() && Master::is_map_done();
}

int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        cout << "plaese input: ./master pg*.txt" << endl;
        return 0;
    }
    buttonrpc server;
    server.as_server(55555);

    // 设置map_worker和reduce_worker的数量
    Master master(12, 5);

    master.get_files(argc, argv);
    server.bind("get_map_tasks", &Master::get_map_tasks, &master);
    server.bind("get_reduce_tasks", &Master::get_reduce_tasks, &master);
    server.bind("get_mapwork_num", &Master::get_mapwork_num, &master);
    server.bind("get_reducework_num", &Master::get_reducework_num, &master);
    server.bind("mapTasksHaveDone", &Master::mapTasksHaveDone, &master);
    server.bind("reduceTasksHaveDone", &Master::reduceTasksHaveDone, &master);
    server.bind("is_map_done", &Master::is_map_done, &master);
    server.bind("is_reduce_done", &Master::is_reduce_done, &master);
    server.bind("is_all_done", &Master::is_all_done, &master);
    server.run();

    return 0;
}
