#pragma once
#include <cstdint>
#include <fstream>
#include <string>
#include <vector>
#include "node.h"
#include "bptree_config.h"

class PageManager {
public:
    // Constructor for creating new index with specified config
    PageManager(const std::string& file, const BPTreeConfig& config);
    
    // Constructor for opening existing index (reads config from file)
    explicit PageManager(const std::string& file);
    
    ~PageManager();
    
    // Read/write nodes using serialization
    void readNode(uint32_t pid, BPlusNode& node);
    void writeNode(uint32_t pid, const BPlusNode& node);
    
    // Raw page read/write for header
    void readRawPage(uint32_t pid, char* buffer, size_t size);
    void writeRawPage(uint32_t pid, const char* buffer, size_t size);
    
    uint32_t allocatePage();
    uint32_t getRoot();
    void setRoot(uint32_t pid);
    
    // Access configuration
    const BPTreeConfig& getConfig() const { return header_.config; }
    uint32_t getPageSize() const { return header_.config.page_size; }
    uint32_t getOrder() const { return header_.config.order; }
    uint32_t getMaxVectorSize() const { return header_.config.max_vector_size; }
    
    // Save header to disk
    void saveHeader();

private:
    std::fstream file_;
    std::string filename_;
    IndexFileHeader header_;
    std::vector<char> page_buffer_;  // Reusable buffer for serialization
    
    void initNewFile(const BPTreeConfig& config);
    void loadExistingFile();
};
