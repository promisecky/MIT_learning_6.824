#include <iostream>
#include <boost/filesystem.hpp>
#include <vector>
#include <string>

using namespace std;
namespace fs = boost::filesystem;

void deleteReduceDirectories(const std::string &directoryPath)
{
    try
    {
        for (const auto &entry : fs::directory_iterator(directoryPath))
        {
            if (fs::is_directory(entry.path()) && entry.path().filename().string().find("reduce-") == 0)
            {
                std::cout << "Deleting directory: " << entry.path().string() << std::endl;
                fs::remove_all(entry.path());
            }
        }
    }
    catch (const fs::filesystem_error &err)
    {
        std::cerr << "Filesystem error: " << err.what() << std::endl;
    }
    catch (const std::exception &ex)
    {
        std::cerr << "General exception: " << ex.what() << std::endl;
    }
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

int main()
{
    std::string path = "."; // 当前目录
    // deleteReduceDirectories(path);
    vector<string> result = getFilesInDirectory("reduce-0");
    for (auto res : result)
    {
        cout << res;
    }
    return 0;
}
