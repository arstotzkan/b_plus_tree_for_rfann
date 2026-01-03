#include "page_manager.h"
#include "node.h"
#include <cstdint>
#include <fstream>
#include <string>
#include <vector>

struct FileHeader {
    uint32_t rootPage;
    uint32_t nextFreePage;
};

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
        std::vector<char> zero(PAGE_SIZE - sizeof(header), 0);
        file.write(zero.data(), zero.size());
        file.close();

        // Reopen in read/write mode
        file.open(filename,
                  std::ios::in | std::ios::out | std::ios::binary);
    }
}

PageManager::~PageManager() {
    if (file.is_open()) {
        file.close();
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
