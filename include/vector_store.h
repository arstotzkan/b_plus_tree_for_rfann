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
    
private:
    struct VectorMetadata {
        uint64_t offset;
        uint32_t size;
    };
    
    std::fstream file_;
    std::string filename_;
    uint32_t max_vector_size_;
    uint64_t next_vector_id_;
    std::unordered_map<uint64_t, VectorMetadata> metadata_;
    
    void initNewFile();
    void loadExistingFile();
    void writeMetadata();
    void readMetadata();
};
