#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <sstream>
#include <dlfcn.h>
#include <thread>
#include <sys/stat.h>
#include <map>
#include <boost/filesystem.hpp>
#include <mutex>
#include <condition_variable>
#include "./buttonrpc-master/buttonrpc.hpp"

using namespace std;

#define LAB_PATH "./libfun.so"

struct KeyValue
{
    string key;
    string value;
};

typedef vector<KeyValue> (*MapFun)(KeyValue kv);
typedef vector<string> (*ReduceFun)(map<string, string> kvs);
MapFun map_fun;
ReduceFun reduce_fun;

// 初始化锁变量
mutex m_mutex;

// 初始化条件变量
condition_variable cv;

// 用于判断条件变量是否满足要求的map任务数量
int target_map_num;

// map 和 reduce 进程的id
int map_id = 0, reduce_id = 0;

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

/**
 * @breif map worker 线程工作函数
 *
 */
void map_worker()
{
    // 每个worker线程初始化一个client
    buttonrpc client;
    client.as_client("127.0.0.1", 55555);

    // 设置map_workID
    m_mutex.lock();
    int map_worker_id = map_id++;
    m_mutex.unlock();

    // while (1)
    // {

    // 获取分配的map任务名称
    string map_tasks = client.call<string>("get_map_tasks").val();

    // 如果任务名称不为empty，那么就进行map函数
    if (map_tasks != "empty")
    {
        std::cout << "map_worker " << map_worker_id << " get map_tasks: " << map_tasks << endl;

        // // 模拟map线程超时
        // if (map_worker_id % 2 == 0)
        // {
        //     while (1)
        //     {
        //         sleep(1200);
        //     }
        // }
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

        // 文件处理完成，通知master
        if (client.call<bool>("mapTasksHaveDone", map_tasks).val())
        {
            std::cout << "map_worker " << map_worker_id << " have done the " << map_tasks << endl;
        }
        // 当完成所有map任务时，通知master进行reduce工作
        if (client.call<bool>("is_map_done").val())
        {
            cv.notify_one();
            return;
        }
    }
    // }
}

// 读取文件夹中所有文件的函数，用于获取reduce的原数据
std::vector<std::string> getFilesInDirectory(const std::string &directoryPath)
{
    std::vector<std::string> files;

    try
    {
        for (const auto &entry : boost::filesystem::directory_iterator(directoryPath))
        {
            if (boost::filesystem::is_regular_file(entry.path()))
            {
                files.push_back(entry.path().filename().string());
            }
        }
    }
    catch (const boost::filesystem::filesystem_error &err)
    {
        std::cerr << "Filesystem error: " << err.what() << std::endl;
    }
    catch (const std::exception &ex)
    {
        std::cerr << "General exception: " << ex.what() << std::endl;
    }

    return files;
}

std::map<string, string> shuffle(std::string &reduceDataDir, vector<string> &getFiles)
{

    std::map<string, string> result;
    for (const auto &file : getFiles)
    {
        std::ifstream inputFile(reduceDataDir + "/" + file);
        if (!inputFile.is_open())
        {
            std::cerr << "Failed to open file: " << file << std::endl;
            continue;
        }
        std::string line;

        while (std::getline(inputFile, line))
        {
            // Process each line of the file
            std::istringstream iss(line);
            std::string r_key, r_value;
            iss >> r_key >> r_value;

            // 使用迭代器避免重复查找
            auto it = result.find(r_key);
            if (it != result.end())
            {
                it->second += r_value;
            }
            else
            {
                result[r_key] = r_value;
            }
        }
        inputFile.close();
    }
    return result;
}

void reduceWrite(int reduce_worker_id, vector<string> &reduce_result)
{
    std::ofstream output_file("rm-" + std::to_string(reduce_worker_id));
    if (!output_file.is_open())
    {
        std::cerr << "Failed to open output file" << std::endl;
        return;
    }
    for (const auto &line : reduce_result)
    {

        output_file << line << std::endl;
    }
    output_file.close();
}

/***
 * @brief reduce worker线程工作函数
 *
 *
 */
void reduce_worker()
{
    // 每个worker线程初始化一个client
    buttonrpc client;
    client.as_client("127.0.0.1", 55555);

    // 设置map_workID
    m_mutex.lock();
    int reduce_worker_id = reduce_id++;
    m_mutex.unlock();

    std::string reduceDataDir = "./reduce-" + to_string(reduce_worker_id);

    // 获取reduce数据源文件
    m_mutex.lock();
    vector<string> getFiles = getFilesInDirectory(reduceDataDir);
    m_mutex.unlock();

    std::map<string, string> kvs = shuffle(reduceDataDir, getFiles);
    vector<string> reduce_result = reduce_fun(kvs);
    // 将结果写入文件
    reduceWrite(reduce_worker_id, reduce_result);
}

// 删除上一次的输出文件夹和文件

void deleteReduceDirectories(const std::string &directoryPath)
{
    try
    {
        for (const auto &entry : boost::filesystem::directory_iterator(directoryPath))
        {
            if (boost::filesystem::is_directory(entry.path()) && entry.path().filename().string().find("reduce-") == 0)
            {
                // std::cout << "Deleting directory: " << entry.path().string() << std::endl;
                boost::filesystem::remove_all(entry.path());
            }
        }
    }
    catch (const boost::filesystem::filesystem_error &err)
    {
        std::cerr << "Filesystem error: " << err.what() << std::endl;
    }
    catch (const std::exception &ex)
    {
        std::cerr << "General exception: " << ex.what() << std::endl;
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
    reduce_fun = (ReduceFun)dlsym(handle, "reduce_fun");

    if (!map_fun)
    {
        cerr << "Cannot load symbol map_fun: " << dlerror() << '\n';
        dlclose(handle);
        return 1;
    }

    if (!reduce_fun)
    {
        cerr << "Cannot load symbol reduce_fun: " << dlerror() << '\n';
        dlclose(handle);
        return 1;
    }

    // 删除上一次的reduce文件
    deleteReduceDirectories(".");

    // 获取map worker和reduce worker的数量
    int map_worker_num = 0;
    int reduce_worker_num = 0;
    map_worker_num = client.call<int>("get_mapwork_num").val();
    reduce_worker_num = client.call<int>("get_reducework_num").val();

    std::cout << "map_workers: " << map_worker_num << endl;
    std::cout << "reduce_workers: " << reduce_worker_num << endl;

    // 创建输出文件夹，如果不存在
    for (int i = 0; i < reduce_worker_num; i++)
    {
        std::string outputDir = "reduce-" + to_string(i);
        if (mkdir(outputDir.c_str(), 0755) == -1)
        {
            std::cerr << "Error creating directory: " << strerror(errno) << '\n';
        }
    }

    // 创建map worker线程
    vector<thread> map_threads;
    for (int i = 0; i < map_worker_num; i++)
    {
        map_threads.push_back(thread(map_worker));
        map_threads[i].detach();
    }

    {
        // 条件变量阻塞并唤醒，当map任务都完成后再创建reduce线程
        unique_lock<mutex> lck(m_mutex);
        cv.wait(lck, [&]
                { return client.call<bool>("is_map_done").val(); });
    }

    // 创建reduce worker线程
    vector<thread> reduce_threads;
    for (int i = 0; i < reduce_worker_num; i++)
    {
        reduce_threads.push_back(thread(reduce_worker));
        reduce_threads[i].join();
    }
    // 删除中间生成的reduce文件
    deleteReduceDirectories(".");

    return 0;
}
