#include <iostream>
#include <vector>
#include <string>
#include <list>
#include "./buttonrpc-master/buttonrpc.hpp"
#include <bits/stdc++.h>
#include <mutex>

using namespace std;

class Master
{
public:
    // 构造函数
    Master(int map_tasks, int reduce_tasks) : m_map_tasks(map_tasks),
                                              m_reduce_tasks(reduce_tasks){};

    // 获取mapwork任务数量
    int get_mapwork_num();

    // 获取reduceworker任务数量
    int get_reducework_num();

    // 获取文件名
    void get_files(int argc, char *argv[]);

    // 获取map任务
    string get_map_tasks();

    // 判断map任务是否都完成
    bool is_map_done();

private:
    // 设置锁
    mutex m_mutex;

    // 设置map worker和reduce worker的数量
    int m_map_tasks;
    int m_reduce_tasks;

    // 存储map文件名列表和文件数量
    list<string> m_files;
    int fileNum;

    // 正在运行中的map任务
    vector<string> running_map_tasks;

    // 完成的任务
    vector<string> finished_map_tasks;
};

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
    server.bind("get_mapwork_num", &Master::get_mapwork_num, &master);
    server.bind("get_reducework_num", &Master::get_reducework_num, &master);
    server.run();

    return 0;
}
