#include "page_manager.h"
#include <cstdint>
#include <fstream>
#include <string>

struct FileHeader {
    uint32_t rootPage;
    uint32_t nextFreePage;
};

static constexpr uint32_t INVALID_PAGE = 0xFFFFFFFF;
static constexpr size_t PAGE_SIZE = 4096;

PageManager::PageManager(const std::string& filename) {
    file.open(filename,
              std::ios::in | std::ios::out | std::ios::binary);

    if (!file.is_open()) {
        // Create new file
        file.open(filename,
                  std::ios::out | std::ios::binary);
        FileHeader header{INVALID_PAGE, 1};
        file.write(reinterpret_cast<char*>(&header), sizeof(header));

        // Pad header page
        char zero[PAGE_SIZE - sizeof(header)]{};
        file.write(zero, sizeof(zero));
        file.close();

        // Reopen in read/write mode
        file.open(filename,
                  std::ios::in | std::ios::out | std::ios::binary);
    }
}

uint32_t PageManager::allocatePage() {
    FileHeader header{};
    file.seekg(0);
    file.read(reinterpret_cast<char*>(&header), sizeof(header));

    uint32_t pid = header.nextFreePage++;
    file.seekp(0);
    file.write(reinterpret_cast<char*>(&header), sizeof(header));
    file.flush();
    return pid;
}

uint32_t PageManager::getRoot() {
    FileHeader header{};
    file.seekg(0);
    file.read(reinterpret_cast<char*>(&header), sizeof(header));
    return header.rootPage;
}

void PageManager::setRoot(uint32_t pid) {
    FileHeader header{};
    file.seekg(0);
    file.read(reinterpret_cast<char*>(&header), sizeof(header));
    header.rootPage = pid;
    file.seekp(0);
    file.write(reinterpret_cast<char*>(&header), sizeof(header));
    file.flush();
}
