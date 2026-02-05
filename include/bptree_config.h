#pragma once
#include <cstdint>
#include <cstddef>
#include <algorithm>

// B+ Tree runtime configuration
// Model B: Vectors are always stored separately in VectorStore
struct BPTreeConfig {
    uint32_t page_size;
    uint32_t order;
    uint32_t max_vector_size;  // Max dimension of vectors (for VectorStore)
    uint32_t magic;  // Magic number to identify valid config
    
    static constexpr uint32_t MAGIC_NUMBER = 0x42505434;  // "BPT4" - new version for Model B
    
    BPTreeConfig() : page_size(8192), order(4), max_vector_size(128), magic(MAGIC_NUMBER) {}
    
    BPTreeConfig(uint32_t order_, uint32_t max_vec_size_) 
        : order(order_), max_vector_size(max_vec_size_), magic(MAGIC_NUMBER) {
        // Auto-calculate minimum page size needed
        page_size = calculate_min_page_size();
    }
    
    // Calculate the minimum page size needed for a BPlusNode with this config
    // Model B layout: unique keys with vector list references
    size_t calculate_node_size() const {
        // BPlusNode layout (Model B):
        // - isLeaf: 4 bytes (padded)
        // - keyCount: 4 bytes (padded)  
        // - keys[order]: order * 4 bytes
        // - children[order+1]: (order+1) * 4 bytes
        // - next: 4 bytes
        // - vector_list_ids[order]: order * 8 bytes (first vector ID for each key)
        // - vector_counts[order]: order * 4 bytes (count of vectors per key)
        
        size_t fixed_overhead = 4 + 4;  // isLeaf + keyCount (with padding)
        size_t keys_size = order * sizeof(int);
        size_t children_size = (order + 1) * sizeof(uint32_t);
        size_t next_size = sizeof(uint32_t);
        size_t vector_list_ids_size = order * sizeof(uint64_t);
        size_t vector_counts_size = order * sizeof(uint32_t);
        
        return fixed_overhead + keys_size + children_size + next_size + vector_list_ids_size + vector_counts_size;
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
    
    // Suggest optimal order for a given target page size
    // Model B: node size is independent of vector dimension (vectors stored separately)
    static uint32_t suggest_order(uint32_t max_vec_size, uint32_t target_page_size = 8192) {
        // Start with order=2 and increase until we exceed page size
        for (uint32_t o = 2; o <= 64; o++) {
            BPTreeConfig test_config;
            test_config.order = o;
            test_config.max_vector_size = max_vec_size;
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
