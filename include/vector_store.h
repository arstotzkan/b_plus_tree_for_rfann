#pragma once
#include <cstdint>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>

class VectorStore {
public:
    VectorStore(const std::string& filename, uint32_t max_vector_size);
    ~VectorStore();
    
    void storeVector(uint64_t vector_id, const std::vector<float>& vector, uint32_t actual_size);
    void retrieveVector(uint64_t vector_id, std::vector<float>& vector, uint32_t& actual_size);
    
    uint64_t getNextVectorId() const { return next_vector_id_; }
    void setNextVectorId(uint64_t id) { next_vector_id_ = id; }
    
    void flush();
    void close();
    
    // In-memory vector cache (similar to memory_index_ in DiskBPlusTree)
    // max_memory_mb: 0 = load all, >0 = limit memory usage
    bool loadAllVectorsIntoMemory(size_t max_memory_mb = 0);
    void clearMemoryCache();
    bool isMemoryCacheLoaded() const { return memory_cache_loaded_; }
    size_t getMemoryCacheSize() const { return memory_cache_.size(); }
    size_t estimateMemoryUsageMB() const;
    
private:
    struct VectorMetadata {
        uint64_t offset;
        uint32_t size;
    };
    
    // In-memory cache for vectors (mirrors data_vectors structure in BPlusNode)
    struct CachedVector {
        std::vector<float> data;
        uint32_t size;
    };
    
    std::fstream file_;
    std::string filename_;
    uint32_t max_vector_size_;
    uint64_t next_vector_id_;
    std::unordered_map<uint64_t, VectorMetadata> metadata_;
    
    // In-memory vector cache
    std::unordered_map<uint64_t, CachedVector> memory_cache_;
    bool memory_cache_loaded_ = false;
    
    void initNewFile();
    void loadExistingFile();
    void writeMetadata();
    void readMetadata();
};
