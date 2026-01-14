#include "vector_store.h"
#include <iostream>
#include <cstring>

// File format:
// Header (20 bytes):
//   - magic (4 bytes): 0x56535430
//   - version (4 bytes): 1
//   - next_vector_id (8 bytes)
//   - data_start_offset (4 bytes): where vector data begins
// 
// Vector data (starting at data_start_offset):
//   For each vector: size (4 bytes) + floats (size * 4 bytes)
//
// Metadata is stored in a separate .meta file to avoid conflicts

static constexpr uint32_t HEADER_SIZE = 20;

VectorStore::VectorStore(const std::string& filename, uint32_t max_vector_size)
    : filename_(filename), max_vector_size_(max_vector_size), next_vector_id_(0) {
    
    file_.open(filename, std::ios::binary | std::ios::in | std::ios::out);
    if (!file_.is_open()) {
        file_.open(filename, std::ios::binary | std::ios::out);
        file_.close();
        file_.open(filename, std::ios::binary | std::ios::in | std::ios::out);
        initNewFile();
    } else {
        loadExistingFile();
    }
}

VectorStore::~VectorStore() {
    close();
}

void VectorStore::initNewFile() {
    file_.seekp(0);
    
    uint32_t magic = 0x56535430;
    file_.write(reinterpret_cast<const char*>(&magic), sizeof(uint32_t));
    
    uint32_t version = 1;
    file_.write(reinterpret_cast<const char*>(&version), sizeof(uint32_t));
    
    uint64_t next_id = 0;
    file_.write(reinterpret_cast<const char*>(&next_id), sizeof(uint64_t));
    
    uint32_t data_start = HEADER_SIZE;
    file_.write(reinterpret_cast<const char*>(&data_start), sizeof(uint32_t));
    
    file_.flush();
    
    // Clear metadata file
    std::ofstream meta_file(filename_ + ".meta", std::ios::binary | std::ios::trunc);
    uint32_t count = 0;
    meta_file.write(reinterpret_cast<const char*>(&count), sizeof(uint32_t));
    meta_file.close();
}

void VectorStore::loadExistingFile() {
    file_.seekg(0);
    
    uint32_t magic;
    file_.read(reinterpret_cast<char*>(&magic), sizeof(uint32_t));
    if (magic != 0x56535430) {
        throw std::runtime_error("Invalid vector store file");
    }
    
    uint32_t version;
    file_.read(reinterpret_cast<char*>(&version), sizeof(uint32_t));
    
    file_.read(reinterpret_cast<char*>(&next_vector_id_), sizeof(uint64_t));
    
    // Skip data_start_offset (we don't need it for reading)
    
    readMetadata();
}

void VectorStore::storeVector(uint64_t vector_id, const std::vector<float>& vector, uint32_t actual_size) {
    if (actual_size > max_vector_size_) {
        actual_size = max_vector_size_;
    }
    
    file_.seekp(0, std::ios::end);
    uint64_t offset = file_.tellp();
    
    // Ensure we're past the header
    if (offset < HEADER_SIZE) {
        offset = HEADER_SIZE;
        file_.seekp(offset);
    }
    
    file_.write(reinterpret_cast<const char*>(&actual_size), sizeof(uint32_t));
    
    for (uint32_t i = 0; i < actual_size; i++) {
        float val = (i < vector.size()) ? vector[i] : 0.0f;
        file_.write(reinterpret_cast<const char*>(&val), sizeof(float));
    }
    file_.flush();
    
    metadata_[vector_id] = {offset, actual_size};
    
    if (vector_id >= next_vector_id_) {
        next_vector_id_ = vector_id + 1;
    }
}

void VectorStore::retrieveVector(uint64_t vector_id, std::vector<float>& vector, uint32_t& actual_size) {
    auto it = metadata_.find(vector_id);
    if (it == metadata_.end()) {
        throw std::runtime_error("Vector ID not found in store: " + std::to_string(vector_id));
    }
    
    const VectorMetadata& meta = it->second;
    actual_size = meta.size;
    
    vector.resize(actual_size);
    
    file_.seekg(meta.offset);
    
    uint32_t stored_size;
    file_.read(reinterpret_cast<char*>(&stored_size), sizeof(uint32_t));
    
    for (uint32_t i = 0; i < actual_size; i++) {
        file_.read(reinterpret_cast<char*>(&vector[i]), sizeof(float));
    }
}

void VectorStore::writeMetadata() {
    // Write next_vector_id to main file header
    file_.seekp(8);
    file_.write(reinterpret_cast<const char*>(&next_vector_id_), sizeof(uint64_t));
    file_.flush();
    
    // Write metadata to separate file
    std::ofstream meta_file(filename_ + ".meta", std::ios::binary | std::ios::trunc);
    if (!meta_file.is_open()) {
        return;
    }
    
    uint32_t count = metadata_.size();
    meta_file.write(reinterpret_cast<const char*>(&count), sizeof(uint32_t));
    
    for (const auto& pair : metadata_) {
        uint64_t id = pair.first;
        const VectorMetadata& meta = pair.second;
        
        meta_file.write(reinterpret_cast<const char*>(&id), sizeof(uint64_t));
        meta_file.write(reinterpret_cast<const char*>(&meta.offset), sizeof(uint64_t));
        meta_file.write(reinterpret_cast<const char*>(&meta.size), sizeof(uint32_t));
    }
    meta_file.close();
}

void VectorStore::readMetadata() {
    std::ifstream meta_file(filename_ + ".meta", std::ios::binary);
    if (!meta_file.is_open()) {
        return;  // No metadata file yet
    }
    
    uint32_t count;
    meta_file.read(reinterpret_cast<char*>(&count), sizeof(uint32_t));
    
    for (uint32_t i = 0; i < count; i++) {
        uint64_t id;
        VectorMetadata meta;
        
        meta_file.read(reinterpret_cast<char*>(&id), sizeof(uint64_t));
        meta_file.read(reinterpret_cast<char*>(&meta.offset), sizeof(uint64_t));
        meta_file.read(reinterpret_cast<char*>(&meta.size), sizeof(uint32_t));
        
        metadata_[id] = meta;
    }
    meta_file.close();
}

void VectorStore::flush() {
    writeMetadata();
}

void VectorStore::close() {
    if (file_.is_open()) {
        writeMetadata();
        file_.close();
    }
}
