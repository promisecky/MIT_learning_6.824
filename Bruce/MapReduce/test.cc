#include <string>
#include <vector>
#include <iostream>
#include <map>

using namespace std;

struct KeyValue
{

    std::string key;
    std::string value;
};

std::vector<std::string> reduce_fun(std::vector<KeyValue> kvs)
{
    std::vector<std::string> result;
    for (auto kv : kvs)
    {
        result.emplace_back(kv.key + " " + std::to_string(kv.value.size()));
    }
    return result;
}

int main(int argc, const char **argv)
{
    KeyValue kv1;
    kv1.key = "a";
    kv1.value = "111";

    KeyValue kv2;
    kv2.key = "b";
    kv2.value = "11111";

    vector<KeyValue> kvs;
    kvs.push_back(kv1);
    kvs.push_back(kv2);
    std::vector<std::string> res = reduce_fun(kvs);
    for (const auto &r : res)
    {
        std::cout << r << std::endl;
    }

    std::map<std::string, std::string> res;
    // res["hello"] += "1";
    return 0;
}