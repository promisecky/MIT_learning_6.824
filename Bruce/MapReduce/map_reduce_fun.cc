#include <string>
#include <iostream>
#include <vector>
#include <map>

/**
 * @brief Defines a key-value data type.
 */
struct KeyValue
{

    std::string key;
    std::string value;
};

/**
 * @brief 用于处理 <文件名, 文件内容> 将文件内容按照单词分割, 分割成<word, 1>的形式
 *
 * @param kv The input key-value pair.
 * @return std::vector<KeyValue> The output key-value pairs.
 */
extern "C" std::vector<KeyValue> map_fun(KeyValue kv)
{
    // 1. 获取文件内容，将文件内容分割
    std::string file_content = kv.value;
    int len = file_content.size();

    std::vector<KeyValue> output;

    // 2. 将文件内容分割成单词
    std::string temp = "";
    for (int i = 0; i < len; i++)
    {
        if ((file_content[i] >= 'a' && file_content[i] <= 'z') || (file_content[i] >= 'A' && file_content[i] <= 'Z'))
        {
            temp += file_content[i];
        }
        else
        {
            if (!temp.empty())
            {
                output.push_back({temp, "1"});
                temp = "";
            }
        }
    }
    // 确保最后一个单词被添加到输出中
    if (!temp.empty())
    {
        output.push_back(KeyValue{temp, "1"});
    }

    return output;
}

/***
 * @brief reduce 函数 将 <"a", "111111"> 形式数据转换成 "a 6" 形式
 * @param kvs
 * @return vectored output
 */

extern "C" std::vector<std::string> reduce_fun(std::map<std::string, std::string> kvs)
{
    std::vector<std::string> result;
    for (auto kv : kvs)
    {
        result.emplace_back(kv.first + " " + std::to_string(kv.second.size()));
        if (kv.first == "1")
        {
            std::cout << kv.first << " " << kv.second << std::endl;
        }
    }
    return result;
}