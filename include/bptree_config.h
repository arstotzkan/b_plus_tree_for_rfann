#pragma once
#include <cstdint>
#include <cstddef>
#include <algorithm>

// B+ Tree runtime configuration
struct BPTreeConfig {
    uint32_t page_size;
    uint32_t order;
    uint32_t max_vector_size;
    uint32_t magic;  // Magic number to identify valid config
    uint32_t use_separate_storage;  // 0 = inline vectors (default), 1 = separate file
    
    static constexpr uint32_t MAGIC_NUMBER = 0x42505433;  // "BPT3"
    
    BPTreeConfig() : page_size(8192), order(4), max_vector_size(128), magic(MAGIC_NUMBER), use_separate_storage(0) {}
    
    BPTreeConfig(uint32_t order_, uint32_t max_vec_size_) 
        : order(order_), max_vector_size(max_vec_size_), magic(MAGIC_NUMBER), use_separate_storage(0) {
        // Auto-calculate minimum page size needed
        page_size = calculate_min_page_size();
    }
    
    // Calculate the minimum page size needed for a BPlusNode with this config
    size_t calculate_node_size() const {
        // BPlusNode layout:
        // - isLeaf: 1 byte (padded to 4)
        // - keyCount: 2 bytes (padded to 4)  
        // - keys[order]: order * 4 bytes
        // - children[order+1]: (order+1) * 4 bytes
        // - next: 4 bytes
        // - vector_sizes[order]: order * 4 bytes
        // - For inline storage: data_vectors[order][max_vector_size]: order * max_vector_size * 4 bytes
        // - For separate storage: vector_ids[order]: order * 8 bytes
        
        size_t fixed_overhead = 4 + 4;  // isLeaf + keyCount (with padding)
        size_t keys_size = order * sizeof(int);
        size_t children_size = (order + 1) * sizeof(uint32_t);
        size_t next_size = sizeof(uint32_t);
        size_t vector_sizes_size = order * sizeof(int);
        
        size_t vector_data_size;
        if (use_separate_storage) {
            vector_data_size = order * sizeof(uint64_t);  // vector_ids
        } else {
            vector_data_size = order * max_vector_size * sizeof(float);  // inline vectors
        }
        
        return fixed_overhead + keys_size + children_size + next_size + vector_sizes_size + vector_data_size;
    }
    
    uint32_t calculate_min_page_size() const {
        size_t node_size = calculate_node_size();
        // Round up to next power of 2 or at least 4KB
        uint32_t min_size = 4096;
        while (min_size < node_size) {
            min_size *= 2;
        }
        return min_size;
    }
    
    bool is_valid() const {
        return magic == MAGIC_NUMBER && order > 0 && max_vector_size > 0 && page_size >= calculate_min_page_size();
    }
    
    // Suggest optimal order for a given vector size and target page size
    static uint32_t suggest_order(uint32_t max_vec_size, uint32_t target_page_size = 8192, uint32_t use_separate_storage = 0) {
        // Start with order=2 and increase until we exceed page size
        for (uint32_t o = 2; o <= 64; o++) {
            BPTreeConfig test_config;
            test_config.order = o;
            test_config.max_vector_size = max_vec_size;
            test_config.use_separate_storage = use_separate_storage;
            if (test_config.calculate_node_size() > target_page_size) {
                return (o > 2) ? o - 1 : 2;
            }
        }
        return 64;
    }
};

// Header stored at the beginning of the index file (page 0)
struct IndexFileHeader {
    BPTreeConfig config;
    uint32_t root_page;
    uint32_t next_free_page;
    uint32_t total_entries;
    uint32_t reserved[4];  // Reserved for future use
    
    IndexFileHeader() : root_page(0xFFFFFFFF), next_free_page(1), total_entries(0) {
        for (int i = 0; i < 4; i++) reserved[i] = 0;
    }
};
