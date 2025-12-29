#pragma once
#include <cstdint>
#include "DataObject.h"
constexpr size_t PAGE_SIZE = 4096;
constexpr uint32_t INVALID_PAGE = 0xFFFFFFFF;
constexpr int ORDER = 8; // Further reduced to prevent overflow
constexpr int MAX_VECTOR_SIZE = 5; // Reduced to fit comfortably in page

// BPlusNode for DataObject storage only
struct BPlusNode {
    bool isLeaf;
    uint16_t keyCount;
    int keys[ORDER]; // Store numeric values as keys
    uint32_t children[ORDER + 1];
    uint32_t next;
    
    // For DataObject storage in leaf nodes - use fixed-size arrays
    int vector_sizes[ORDER]; // Store the size of each vector
    int data_vectors[ORDER][MAX_VECTOR_SIZE]; // Fixed-size storage for vectors
};
