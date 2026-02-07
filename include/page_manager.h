#pragma once
#include <cstdint>
#include <fstream>
#include <string>
#include <vector>
#include <memory>
#include "node.h"
#include "bptree_config.h"
#include "vector_store.h"

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
    
    // Bulk load all pages sequentially (much faster than random reads)
    // max_memory_mb: 0 = load all, >0 = limit memory usage
    void loadAllNodes(std::unordered_map<uint32_t, BPlusNode>& nodes, size_t max_memory_mb = 0);
    size_t estimateNodeMemoryMB() const;
    
    // Raw page read/write for header
    void readRawPage(uint32_t pid, char* buffer, size_t size);
    void writeRawPage(uint32_t pid, const char* buffer, size_t size);
    
    uint32_t allocatePage();
    // Allocate a page without immediately saving the header to disk.
    // Caller must call saveHeader() or flushHeader() when done with batch allocations.
    uint32_t allocatePageDeferred();
    uint32_t getRoot();
    void setRoot(uint32_t pid);
    void setRootDeferred(uint32_t pid);
    
    // Access configuration
    const BPTreeConfig& getConfig() const { return header_.config; }
    uint32_t getPageSize() const { return header_.config.page_size; }
    uint32_t getOrder() const { return header_.config.order; }
    uint32_t getMaxVectorSize() const { return header_.config.max_vector_size; }
    
    // Save header to disk
    void saveHeader();
    
    // Vector store access
    VectorStore* getVectorStore() { return vector_store_.get(); }

private:
    std::fstream file_;
    std::string filename_;
    IndexFileHeader header_;
    std::vector<char> page_buffer_;  // Reusable buffer for serialization
    std::unique_ptr<VectorStore> vector_store_;
    
    // Batched flush: flush to disk every N writes instead of every write
    uint32_t writes_since_flush_ = 0;
    static constexpr uint32_t FLUSH_INTERVAL = 1000;
    void maybeFlush();
    
    void initNewFile(const BPTreeConfig& config);
    void loadExistingFile();
};
