#pragma once
#include <cstdint>
#include <fstream>
#include <string>

class PageManager {
public:
    explicit PageManager(const std::string& file);
    template<typename T>
    void readPage(uint32_t pid, T* buffer) {
        file.seekg(pid * PAGE_SIZE);
        file.read(reinterpret_cast<char*>(buffer), sizeof(T));
    }
    template<typename T>
    void writePage(uint32_t pid, const T* buffer) {
        file.seekp(pid * PAGE_SIZE);
        file.write(reinterpret_cast<const char*>(buffer), sizeof(T));
        file.flush();
    }
    uint32_t allocatePage();
    uint32_t getRoot();
    void setRoot(uint32_t pid);

private:
    std::fstream file;
    static constexpr size_t PAGE_SIZE = 4096;
};
