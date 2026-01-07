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
struct BPlusNode {
    bool isLeaf;
    uint16_t keyCount;
    std::vector<int> keys;
    std::vector<uint32_t> children;
    uint32_t next;
    
    // For DataObject storage in leaf nodes
    std::vector<int> vector_sizes;
    std::vector<std::vector<float>> data_vectors;
    
    // Initialize with given order and max vector size
    void init(uint32_t order, uint32_t max_vec_size) {
        isLeaf = false;
        keyCount = 0;
        next = INVALID_PAGE;
        keys.resize(order, 0);
        children.resize(order + 1, INVALID_PAGE);
        vector_sizes.resize(order, 0);
        data_vectors.resize(order);
        for (auto& v : data_vectors) {
            v.resize(max_vec_size, 0.0f);
        }
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
        
        // vector_sizes[order]
        for (uint32_t i = 0; i < config.order; i++) {
            int vs = (i < vector_sizes.size()) ? vector_sizes[i] : 0;
            std::memcpy(ptr, &vs, sizeof(int));
            ptr += sizeof(int);
        }
        
        // data_vectors[order][max_vector_size]
        for (uint32_t i = 0; i < config.order; i++) {
            for (uint32_t j = 0; j < config.max_vector_size; j++) {
                float v = (i < data_vectors.size() && j < data_vectors[i].size()) ? data_vectors[i][j] : 0.0f;
                std::memcpy(ptr, &v, sizeof(float));
                ptr += sizeof(float);
            }
        }
    }
    
    // Deserialize node from raw bytes
    void deserialize(const char* buffer, const BPTreeConfig& config) {
        const char* ptr = buffer;
        
        // Initialize vectors with proper sizes
        init(config.order, config.max_vector_size);
        
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
        
        // vector_sizes[order]
        for (uint32_t i = 0; i < config.order; i++) {
            std::memcpy(&vector_sizes[i], ptr, sizeof(int));
            ptr += sizeof(int);
        }
        
        // data_vectors[order][max_vector_size]
        for (uint32_t i = 0; i < config.order; i++) {
            for (uint32_t j = 0; j < config.max_vector_size; j++) {
                std::memcpy(&data_vectors[i][j], ptr, sizeof(float));
                ptr += sizeof(float);
            }
        }
    }
};
