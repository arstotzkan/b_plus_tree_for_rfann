#pragma once
#include <cstdint>
#include "DataObject.h"

// Default values - can be overridden at compile time with -D flags
#ifndef BPTREE_PAGE_SIZE
#define BPTREE_PAGE_SIZE 8192
#endif

#ifndef BPTREE_ORDER
#define BPTREE_ORDER 4
#endif

#ifndef BPTREE_MAX_VECTOR_SIZE
#define BPTREE_MAX_VECTOR_SIZE 128
#endif

constexpr size_t PAGE_SIZE = BPTREE_PAGE_SIZE;
constexpr uint32_t INVALID_PAGE = 0xFFFFFFFF;
constexpr int ORDER = BPTREE_ORDER;
constexpr int MAX_VECTOR_SIZE = BPTREE_MAX_VECTOR_SIZE;

// BPlusNode for DataObject storage only
struct BPlusNode {
    bool isLeaf;
    uint16_t keyCount;
    int keys[ORDER]; // Store numeric values as keys
    uint32_t children[ORDER + 1];
    uint32_t next;
    
    // For DataObject storage in leaf nodes - use fixed-size arrays
    int vector_sizes[ORDER]; // Store the size of each vector
    float data_vectors[ORDER][MAX_VECTOR_SIZE]; // Float storage for vectors
};
