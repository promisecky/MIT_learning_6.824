#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <sstream>
#include <dlfcn.h>
#include <thread>
#include <sys/stat.h>
#include <mutex>
#include "./buttonrpc-master/buttonrpc.hpp"

using namespace std;

#define LAB_PATH "./libfun.so"

struct KeyValue
{

    std::string key;
    std::string value;
};

typedef std::vector<KeyValue> (*MapFun)(KeyValue kv);
MapFun map_fun;

mutex m_mutex;
int map_worket_id;

std::string readFile(const std::string &fileName)
{
    std::ifstream file(fileName); // 打开文件
    if (!file.is_open())
    {
        std::cerr << "Failed to open file: " << fileName << std::endl;
        return "";
    }

    std::stringstream buffer;
    buffer << file.rdbuf(); // 读取文件到stringstream缓冲区中

    file.close();        // 关闭文件
    return buffer.str(); // 将stringstream转换为string
}

// Function to calculate the hash value of a string
int calculateHash(const std::string &str)
{
    int hash = 0;
    for (char c : str)
    {
        hash = hash + c;
    }
    return hash;
}

// 函数用于将键值对写入文件
void writeKVs(const std::vector<KeyValue> &keyValues, const int &map_worker_id, const int &reduce_tasks_num)
{
    // 根据字符串key分配文件
    // 获取reduce任务数量
    
    vector<ofstream> filenames;

    for (int i = 0; i < reduce_tasks_num; i++)
    {
        string filename = "reduce-" + to_string(i) + "/mr-" + to_string(map_worker_id);
        // cout << "file name is :" << filename << endl;
        std::ofstream file(filename);
        if (!file.is_open())
        {
            std::cerr << "Failed to open file: " << filename << std::endl;
            return;
        }
        filenames.emplace_back(move(file));
    }

    for (const auto &kv : keyValues)
    {
        int index = calculateHash(kv.key) % reduce_tasks_num;

        filenames[index] << kv.key << " " << kv.value << std::endl;
    }

    for (int i = 0; i < reduce_tasks_num; i++)
    {
        filenames[i].close();
    }
}

void map_worker()
{
    // 每个worker线程初始化一个client
    buttonrpc client;
    client.as_client("127.0.0.1", 55555);

    // 设置map_workID
    m_mutex.lock();
    int map_worker_id = map_worket_id++;
    m_mutex.unlock();

    // 获取分配的map任务名称
    string map_tasks = client.call<string>("get_map_tasks").val();

    cout << "map_worker " << map_worker_id << " get map_tasks: " << map_tasks << endl;
    // 如果任务名称不为empty，那么就进行map函数
    if (map_tasks != "empty")
    {
        // 读取文件
        string file_content = readFile(map_tasks);

        // 构造kv
        KeyValue kv;
        kv.key = map_tasks;
        kv.value = file_content;

        // map 函数进行处理
        vector<KeyValue> map_result = map_fun(kv);

        // 获取reduce任务数量
        int reduce_tasks_num = client.call<int>("get_reducework_num").val();
        // cout << "map_worker 中的reduce任务数量 " << reduce_tasks_num << endl;
        // 将结果写入文件
        writeKVs(map_result, map_worker_id, reduce_tasks_num);
    }
    else
    {
        cout << "map_worker " << map_worker_id << "don't get any map tasks Done!" << endl;
    }
}

int main(int argc, char const *argv[])
{
    // 设置rpc client
    buttonrpc client;
    client.as_client("127.0.0.1", 55555);
    client.set_timeout(5000);

    // 加载动态库
    void *handle = dlopen(LAB_PATH, RTLD_LAZY);
    if (!handle)
    {
        cerr << "Cannot open library: " << dlerror() << '\n';
        return 1;
    }
    dlerror();
    // 加载动态库中的map_fun函数
    map_fun = (MapFun)dlsym(handle, "map_fun");

    if (!map_fun)
    {
        cerr << "Cannot load symbol map_fun: " << dlerror() << '\n';
        dlclose(handle);
        return 1;
    }

    // 获取map任务数量
    int map_workers = 0;
    map_workers = client.call<int>("get_mapwork_num").val();

    cout << "map_workers: " << map_workers << endl;

    // 创建输出文件夹，如果不存在
    int reduce_worker_num = client.call<int>("get_reducework_num").val();
    for(int i = 0; i < reduce_worker_num; i++){
        std::string outputDir = "reduce-" + to_string(i);
        if (mkdir(outputDir.c_str(), 0755) == -1) {
            std::cerr << "Error creating directory: " << strerror(errno) << '\n';
        }
    }
    
    // 创建map worker线程
    vector<thread> map_threads;
    for (int i = 0; i < map_workers; i++)
    {
        map_threads.push_back(thread(map_worker));
        map_threads[i].join();
    }

    return 0;
}
