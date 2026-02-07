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
    
    // Store a single vector, returns its ID
    uint64_t storeVector(const std::vector<float>& vector, uint32_t actual_size);
    
    // Append a vector to an existing list (or start a new list)
    // Returns the new first_vector_id for the list
    uint64_t appendVectorToList(uint64_t first_vector_id, const std::vector<float>& vector, uint32_t actual_size);
    
    // Retrieve a single vector by ID
    void retrieveVector(uint64_t vector_id, std::vector<float>& vector, uint32_t& actual_size);
    
    // Retrieve all vectors in a list starting from first_vector_id
    void retrieveVectorList(uint64_t first_vector_id, uint32_t count, 
                           std::vector<std::vector<float>>& vectors, 
                           std::vector<uint32_t>& sizes);
    
    // Delete a vector from a list, returns new first_vector_id (or 0 if list is empty)
    uint64_t removeVectorFromList(uint64_t first_vector_id, uint32_t count, 
                                  const std::vector<float>& vector_to_remove,
                                  uint32_t& new_count);
    
    uint64_t getNextVectorId() const { return next_vector_id_; }
    void setNextVectorId(uint64_t id) { next_vector_id_ = id; }
    uint32_t getMaxVectorSize() const { return max_vector_size_; }
    
    // Pre-reserve metadata hash map capacity to avoid rehashing during bulk load
    void reserveMetadata(size_t count) { metadata_.reserve(count); }
    
    void flush();
    void close();
    
    // In-memory vector cache
    bool loadAllVectorsIntoMemory(size_t max_memory_mb = 0);
    void clearMemoryCache();
    bool isMemoryCacheLoaded() const { return memory_cache_loaded_; }
    size_t getMemoryCacheSize() const { return memory_cache_.size(); }
    size_t estimateMemoryUsageMB() const;
    
private:
    struct VectorMetadata {
        uint64_t offset;      // File offset where vector data starts
        uint32_t size;        // Actual vector dimension
        uint64_t next_id;     // Next vector in list (0 = end of list)
    };
    
    struct CachedVector {
        std::vector<float> data;
        uint32_t size;
        uint64_t next_id;
    };
    
    std::fstream file_;
    std::string filename_;
    uint32_t max_vector_size_;
    uint64_t next_vector_id_;
    uint64_t write_pos_;  // Tracks append position to avoid seekp(end) per write
    std::unordered_map<uint64_t, VectorMetadata> metadata_;
    
    // Batched flush: flush to disk every N writes instead of every write
    uint32_t writes_since_flush_ = 0;
    static constexpr uint32_t FLUSH_INTERVAL = 1000;
    
    // In-memory vector cache
    std::unordered_map<uint64_t, CachedVector> memory_cache_;
    bool memory_cache_loaded_ = false;
    
    void initNewFile();
    void loadExistingFile();
    void writeMetadata();
    void readMetadata();
    
    // Internal: store vector with explicit ID and next pointer
    void storeVectorInternal(uint64_t vector_id, const std::vector<float>& vector, 
                            uint32_t actual_size, uint64_t next_id);
};
