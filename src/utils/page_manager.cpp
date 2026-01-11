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
    
    // Initialize vector store
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
    
    // Initialize vector store
    std::string vector_store_filename = filename_ + ".vectors";
    vector_store_ = std::make_unique<VectorStore>(vector_store_filename, header_.config.max_vector_size);
}

void PageManager::saveHeader() {
    if (!file_.is_open()) return;
    
    std::vector<char> header_page(header_.config.page_size, 0);
    std::memcpy(header_page.data(), &header_, sizeof(IndexFileHeader));
    
    file_.seekp(0);
    file_.write(header_page.data(), header_.config.page_size);
    file_.flush();
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
    file_.flush();
}

void PageManager::readRawPage(uint32_t pid, char* buffer, size_t size) {
    file_.seekg(static_cast<std::streamoff>(pid) * header_.config.page_size);
    file_.read(buffer, size);
}

void PageManager::writeRawPage(uint32_t pid, const char* buffer, size_t size) {
    file_.seekp(static_cast<std::streamoff>(pid) * header_.config.page_size);
    file_.write(buffer, size);
    file_.flush();
}

uint32_t PageManager::allocatePage() {
    uint32_t pid = header_.next_free_page++;
    saveHeader();
    return pid;
}

uint32_t PageManager::getRoot() {
    return header_.root_page;
}

void PageManager::setRoot(uint32_t pid) {
    header_.root_page = pid;
    saveHeader();
}
