#pragma once
#include <cstdint>
#include <cstring>
#include <vector>
#include "DataObject.h"
#include "bptree_config.h"

// Default values - used when no config is provided
#ifndef BPTREE_PAGE_SIZE
#define BPTREE_PAGE_SIZE 8192
#endif

#ifndef BPTREE_ORDER
#define BPTREE_ORDER 4
#endif

#ifndef BPTREE_MAX_VECTOR_SIZE
#define BPTREE_MAX_VECTOR_SIZE 128
#endif

// Legacy compile-time constants (for backward compatibility)
constexpr size_t DEFAULT_PAGE_SIZE = BPTREE_PAGE_SIZE;
constexpr uint32_t INVALID_PAGE = 0xFFFFFFFF;
constexpr int DEFAULT_ORDER = BPTREE_ORDER;
constexpr int DEFAULT_MAX_VECTOR_SIZE = BPTREE_MAX_VECTOR_SIZE;

// Maximum supported values for runtime configuration
constexpr int MAX_ORDER = 64;
constexpr int MAX_VECTOR_DIM = 2048;

// Dynamic BPlusNode that works with runtime configuration
// Model B: Each key is unique within a leaf, and maps to a LIST of vectors in VectorStore
struct BPlusNode {
    bool isLeaf;
    uint16_t keyCount;
    std::vector<int> keys;              // Unique keys in this node
    std::vector<uint32_t> children;     // Child pointers (internal nodes only)
    uint32_t next;                      // Next leaf pointer (leaf nodes only)
    
    // For DataObject storage in leaf nodes (Model B: unique key -> vector list)
    std::vector<uint64_t> vector_list_ids;   // ID of first vector in the list for each key
    std::vector<uint32_t> vector_counts;     // Number of vectors for each key
    
    // Initialize with given order (max_vec_size no longer needed for node structure)
    void init(uint32_t order, uint32_t /*max_vec_size*/ = 0) {
        isLeaf = false;
        keyCount = 0;
        next = INVALID_PAGE;
        keys.resize(order, 0);
        children.resize(order + 1, INVALID_PAGE);
        vector_list_ids.resize(order, 0);
        vector_counts.resize(order, 0);
    }
    
    // Serialize node to raw bytes for disk storage
    void serialize(char* buffer, const BPTreeConfig& config) const {
        char* ptr = buffer;
        
        // isLeaf (1 byte, but we write 4 for alignment)
        uint32_t leaf_flag = isLeaf ? 1 : 0;
        std::memcpy(ptr, &leaf_flag, sizeof(uint32_t));
        ptr += sizeof(uint32_t);
        
        // keyCount (4 bytes for alignment)
        uint32_t kc = keyCount;
        std::memcpy(ptr, &kc, sizeof(uint32_t));
        ptr += sizeof(uint32_t);
        
        // keys[order]
        for (uint32_t i = 0; i < config.order; i++) {
            int k = (i < keys.size()) ? keys[i] : 0;
            std::memcpy(ptr, &k, sizeof(int));
            ptr += sizeof(int);
        }
        
        // children[order+1]
        for (uint32_t i = 0; i < config.order + 1; i++) {
            uint32_t c = (i < children.size()) ? children[i] : INVALID_PAGE;
            std::memcpy(ptr, &c, sizeof(uint32_t));
            ptr += sizeof(uint32_t);
        }
        
        // next
        std::memcpy(ptr, &next, sizeof(uint32_t));
        ptr += sizeof(uint32_t);
        
        // vector_list_ids[order] - ID of first vector in list for each key
        for (uint32_t i = 0; i < config.order; i++) {
            uint64_t vid = (i < vector_list_ids.size()) ? vector_list_ids[i] : 0;
            std::memcpy(ptr, &vid, sizeof(uint64_t));
            ptr += sizeof(uint64_t);
        }
        
        // vector_counts[order] - number of vectors for each key
        for (uint32_t i = 0; i < config.order; i++) {
            uint32_t vc = (i < vector_counts.size()) ? vector_counts[i] : 0;
            std::memcpy(ptr, &vc, sizeof(uint32_t));
            ptr += sizeof(uint32_t);
        }
    }
    
    // Deserialize node from raw bytes
    void deserialize(const char* buffer, const BPTreeConfig& config) {
        const char* ptr = buffer;
        
        // Initialize vectors with proper sizes
        init(config.order);
        
        // isLeaf
        uint32_t leaf_flag;
        std::memcpy(&leaf_flag, ptr, sizeof(uint32_t));
        isLeaf = (leaf_flag != 0);
        ptr += sizeof(uint32_t);
        
        // keyCount
        uint32_t kc;
        std::memcpy(&kc, ptr, sizeof(uint32_t));
        keyCount = static_cast<uint16_t>(kc);
        ptr += sizeof(uint32_t);
        
        // keys[order]
        for (uint32_t i = 0; i < config.order; i++) {
            std::memcpy(&keys[i], ptr, sizeof(int));
            ptr += sizeof(int);
        }
        
        // children[order+1]
        for (uint32_t i = 0; i < config.order + 1; i++) {
            std::memcpy(&children[i], ptr, sizeof(uint32_t));
            ptr += sizeof(uint32_t);
        }
        
        // next
        std::memcpy(&next, ptr, sizeof(uint32_t));
        ptr += sizeof(uint32_t);
        
        // vector_list_ids[order]
        for (uint32_t i = 0; i < config.order; i++) {
            std::memcpy(&vector_list_ids[i], ptr, sizeof(uint64_t));
            ptr += sizeof(uint64_t);
        }
        
        // vector_counts[order]
        for (uint32_t i = 0; i < config.order; i++) {
            std::memcpy(&vector_counts[i], ptr, sizeof(uint32_t));
            ptr += sizeof(uint32_t);
        }
    }
};
