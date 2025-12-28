#pragma once
#include <cstdint>
constexpr size_t PAGE_SIZE = 4096;
constexpr uint32_t INVALID_PAGE = 0xFFFFFFFF;
constexpr int ORDER = 32;

template <typename T>
struct BPlusNode {
    bool isLeaf;
    uint16_t keyCount;
    T keys[ORDER];
    uint32_t children[ORDER + 1];
    uint32_t next;
};
