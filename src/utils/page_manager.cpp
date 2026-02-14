#include "page_manager.h"
#include "node.h"
#include <cstdint>
#include <fstream>
#include <string>
#include <vector>
#include <stdexcept>
#include <iostream>

// Constructor for creating new index with specified config
PageManager::PageManager(const std::string& filename, const BPTreeConfig& config)
    : filename_(filename) {
    
    // Try to open existing file first
    file_.open(filename, std::ios::in | std::ios::out | std::ios::binary);
    
    if (file_.is_open()) {
        // File exists - load and verify config
        loadExistingFile();
        
        // Verify config matches (or use existing config)
        if (header_.config.magic == BPTreeConfig::MAGIC_NUMBER) {
            // Existing file has valid config - use it
            if (header_.config.order != config.order || 
                header_.config.max_vector_size != config.max_vector_size) {
                std::cerr << "Warning: Existing index has different config. Using existing config." << std::endl;
                std::cerr << "  Existing: order=" << header_.config.order 
                          << ", max_vector_size=" << header_.config.max_vector_size << std::endl;
                std::cerr << "  Requested: order=" << config.order 
                          << ", max_vector_size=" << config.max_vector_size << std::endl;
            }
        } else {
            // Old format file - initialize with new config
            initNewFile(config);
        }
    } else {
        // Create new file
        initNewFile(config);
    }
    
    // Initialize page buffer
    page_buffer_.resize(header_.config.page_size, 0);
}

// Constructor for opening existing index (reads config from file)
PageManager::PageManager(const std::string& filename)
    : filename_(filename) {
    
    file_.open(filename, std::ios::in | std::ios::out | std::ios::binary);
    
    if (file_.is_open()) {
        loadExistingFile();
        
        // If old format, use default config
        if (header_.config.magic != BPTreeConfig::MAGIC_NUMBER) {
            std::cerr << "Warning: Old format index file. Using default config." << std::endl;
            header_.config = BPTreeConfig();
        }
    } else {
        // Create new file with default config
        BPTreeConfig default_config;
        initNewFile(default_config);
    }
    
    // Initialize page buffer
    page_buffer_.resize(header_.config.page_size, 0);
}

PageManager::~PageManager() {
    if (vector_store_) {
        vector_store_->flush();
    }
    if (file_.is_open()) {
        saveHeader();
        file_.flush();
        file_.close();
    }
}

void PageManager::initNewFile(const BPTreeConfig& config) {
    // Close if open
    if (file_.is_open()) {
        file_.close();
    }
    
    // Create new file
    file_.open(filename_, std::ios::out | std::ios::binary);
    if (!file_.is_open()) {
        throw std::runtime_error("Cannot create index file: " + filename_);
    }
    
    // Initialize header
    header_.config = config;
    header_.root_page = INVALID_PAGE;
    header_.next_free_page = 1;  // Page 0 is header
    header_.total_entries = 0;
    
    // Write header (pad to page size)
    std::vector<char> header_page(config.page_size, 0);
    std::memcpy(header_page.data(), &header_, sizeof(IndexFileHeader));
    file_.write(header_page.data(), config.page_size);
    file_.close();
    
    // Reopen in read/write mode
    file_.open(filename_, std::ios::in | std::ios::out | std::ios::binary);
    if (!file_.is_open()) {
        throw std::runtime_error("Cannot reopen index file: " + filename_);
    }
    
    // always initialize vector store
    std::string vector_store_filename = filename_ + ".vectors";
    vector_store_ = std::make_unique<VectorStore>(vector_store_filename, config.max_vector_size);
}

void PageManager::loadExistingFile() {
    // Read header
    file_.seekg(0);
    file_.read(reinterpret_cast<char*>(&header_), sizeof(IndexFileHeader));
    
    // Check if this is an old format file (no magic number)
    if (header_.config.magic != BPTreeConfig::MAGIC_NUMBER) {
        // Old format - read just root and next_free from beginning
        file_.seekg(0);
        uint32_t old_root, old_next;
        file_.read(reinterpret_cast<char*>(&old_root), sizeof(uint32_t));
        file_.read(reinterpret_cast<char*>(&old_next), sizeof(uint32_t));
        
        // Use default config for old files
        header_.config = BPTreeConfig();
        header_.root_page = old_root;
        header_.next_free_page = old_next;
        header_.total_entries = 0;
    }
    
    // always initialize vector store
    std::string vector_store_filename = filename_ + ".vectors";
    vector_store_ = std::make_unique<VectorStore>(vector_store_filename, header_.config.max_vector_size);
}

void PageManager::saveHeader() {
    if (!file_.is_open()) return;
    
    std::vector<char> header_page(header_.config.page_size, 0);
    std::memcpy(header_page.data(), &header_, sizeof(IndexFileHeader));
    
    file_.seekp(0);
    file_.write(header_page.data(), header_.config.page_size);
    maybeFlush();
}

void PageManager::maybeFlush() {
    writes_since_flush_++;
    if (writes_since_flush_ >= FLUSH_INTERVAL) {
        file_.flush();
        writes_since_flush_ = 0;
    }
}

void PageManager::readNode(uint32_t pid, BPlusNode& node) {
    if (pid == INVALID_PAGE) return;
    
    // Seek to page
    file_.seekg(static_cast<std::streamoff>(pid) * header_.config.page_size);
    
    // Read raw page data
    std::fill(page_buffer_.begin(), page_buffer_.end(), 0);
    file_.read(page_buffer_.data(), header_.config.page_size);
    
    // Deserialize node
    node.deserialize(page_buffer_.data(), header_.config);
}

void PageManager::writeNode(uint32_t pid, const BPlusNode& node) {
    // Serialize node to buffer
    std::fill(page_buffer_.begin(), page_buffer_.end(), 0);
    node.serialize(page_buffer_.data(), header_.config);
    
    // Write to page
    file_.seekp(static_cast<std::streamoff>(pid) * header_.config.page_size);
    file_.write(page_buffer_.data(), header_.config.page_size);
    maybeFlush();
}

void PageManager::readRawPage(uint32_t pid, char* buffer, size_t size) {
    file_.seekg(static_cast<std::streamoff>(pid) * header_.config.page_size);
    file_.read(buffer, size);
}

size_t PageManager::estimateNodeMemoryMB() const {
    uint32_t total_pages = header_.next_free_page;
    if (total_pages <= 1) return 0;
    
    // estimate per-node memory
    size_t per_node_bytes = 
        header_.config.order * sizeof(int) +                    // keys
        (header_.config.order + 1) * sizeof(uint32_t) +         // children
        header_.config.order * sizeof(uint64_t) +               // vector_list_ids
        header_.config.order * sizeof(uint32_t) +               // vector_counts
        100;  // overhead for std::vector headers, map entry, etc.
    
    return ((total_pages - 1) * per_node_bytes) / (1024 * 1024);
}

void PageManager::loadAllNodes(std::unordered_map<uint32_t, BPlusNode>& nodes, size_t max_memory_mb) {
    uint32_t total_pages = header_.next_free_page;
    if (total_pages <= 1) return;  // Only header page
    
    size_t estimated_mb = estimateNodeMemoryMB();
    std::cout << "Estimated memory for " << (total_pages - 1) << " nodes: " << estimated_mb << " MB" << std::endl;
    
    // Check memory limit
    if (max_memory_mb > 0 && estimated_mb > max_memory_mb) {
        std::cout << "Warning: Node memory (" << estimated_mb << " MB) exceeds limit (" << max_memory_mb << " MB)" << std::endl;
        std::cout << "Loading partial node cache..." << std::endl;
    }
    
    std::cout << "Bulk loading pages sequentially..." << std::endl;
    
    // Calculate how many nodes we can load
    size_t per_node_bytes = (estimated_mb * 1024 * 1024) / (total_pages - 1);
    size_t max_nodes = (max_memory_mb > 0 && per_node_bytes > 0) 
        ? std::min(static_cast<size_t>(total_pages - 1), (max_memory_mb * 1024 * 1024) / per_node_bytes)
        : (total_pages - 1);
    
    // Pre-reserve map capacity
    nodes.reserve(max_nodes);
    
    // Seek to first data page (page 1, after header)
    file_.seekg(static_cast<std::streamoff>(header_.config.page_size));
    
    size_t loaded = 0;
    size_t last_progress = 0;
    size_t memory_used = 0;
    const size_t memory_limit_bytes = max_memory_mb * 1024 * 1024;
    
    // Read all pages sequentially - much faster than random seeks
    for (uint32_t pid = 1; pid < total_pages; pid++) {
        // Check memory limit
        if (max_memory_mb > 0 && memory_used >= memory_limit_bytes) {
            std::cout << "Memory limit reached at " << loaded << " nodes (" << (memory_used / (1024*1024)) << " MB)" << std::endl;
            break;
        }
        
        std::fill(page_buffer_.begin(), page_buffer_.end(), 0);
        file_.read(page_buffer_.data(), header_.config.page_size);
        
        if (!file_.good()) {
            std::cerr << "Error reading page " << pid << std::endl;
            break;
        }
        
        BPlusNode node;
        node.deserialize(page_buffer_.data(), header_.config);
        memory_used += per_node_bytes;
        nodes[pid] = std::move(node);
        loaded++;
        
        // Progress logging every 10%
        size_t progress = (loaded * 100) / (total_pages - 1);
        if (progress >= last_progress + 10) {
            std::cout << "  Node loading progress: " << progress << "% (" << loaded << "/" << (total_pages - 1) << ", " << (memory_used / (1024*1024)) << " MB)" << std::endl;
            last_progress = progress;
        }
    }
    
    std::cout << "Loaded " << loaded << "/" << (total_pages - 1) << " nodes (" << (memory_used / (1024*1024)) << " MB)" << std::endl;
}

void PageManager::writeRawPage(uint32_t pid, const char* buffer, size_t size) {
    file_.seekp(static_cast<std::streamoff>(pid) * header_.config.page_size);
    file_.write(buffer, size);
    maybeFlush();
}

uint32_t PageManager::allocatePage() {
    uint32_t pid = header_.next_free_page++;
    saveHeader();
    return pid;
}

uint32_t PageManager::allocatePageDeferred() {
    return header_.next_free_page++;
}

uint32_t PageManager::getRoot() {
    return header_.root_page;
}

void PageManager::setRoot(uint32_t pid) {
    header_.root_page = pid;
    saveHeader();
}

void PageManager::setRootDeferred(uint32_t pid) {
    header_.root_page = pid;
}
