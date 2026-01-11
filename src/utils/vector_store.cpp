#include "vector_store.h"
#include <iostream>
#include <cstring>

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
    
    uint32_t metadata_count = 0;
    file_.write(reinterpret_cast<const char*>(&metadata_count), sizeof(uint32_t));
    
    file_.flush();
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
    
    readMetadata();
}

void VectorStore::storeVector(uint64_t vector_id, const std::vector<float>& vector, uint32_t actual_size) {
    if (actual_size > max_vector_size_) {
        actual_size = max_vector_size_;
    }
    
    file_.seekp(0, std::ios::end);
    uint64_t offset = file_.tellp();
    
    file_.write(reinterpret_cast<const char*>(&actual_size), sizeof(uint32_t));
    
    for (uint32_t i = 0; i < actual_size; i++) {
        float val = (i < vector.size()) ? vector[i] : 0.0f;
        file_.write(reinterpret_cast<const char*>(&val), sizeof(float));
    }
    
    metadata_[vector_id] = {offset, actual_size};
    
    if (vector_id >= next_vector_id_) {
        next_vector_id_ = vector_id + 1;
    }
}

void VectorStore::retrieveVector(uint64_t vector_id, std::vector<float>& vector, uint32_t& actual_size) {
    auto it = metadata_.find(vector_id);
    if (it == metadata_.end()) {
        throw std::runtime_error("Vector ID not found in store");
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
    file_.seekp(16);
    
    uint32_t count = metadata_.size();
    file_.write(reinterpret_cast<const char*>(&count), sizeof(uint32_t));
    
    for (const auto& pair : metadata_) {
        uint64_t id = pair.first;
        const VectorMetadata& meta = pair.second;
        
        file_.write(reinterpret_cast<const char*>(&id), sizeof(uint64_t));
        file_.write(reinterpret_cast<const char*>(&meta.offset), sizeof(uint64_t));
        file_.write(reinterpret_cast<const char*>(&meta.size), sizeof(uint32_t));
    }
    
    file_.seekp(8);
    file_.write(reinterpret_cast<const char*>(&next_vector_id_), sizeof(uint64_t));
    
    file_.flush();
}

void VectorStore::readMetadata() {
    file_.seekg(16);
    
    uint32_t count;
    file_.read(reinterpret_cast<char*>(&count), sizeof(uint32_t));
    
    for (uint32_t i = 0; i < count; i++) {
        uint64_t id;
        VectorMetadata meta;
        
        file_.read(reinterpret_cast<char*>(&id), sizeof(uint64_t));
        file_.read(reinterpret_cast<char*>(&meta.offset), sizeof(uint64_t));
        file_.read(reinterpret_cast<char*>(&meta.size), sizeof(uint32_t));
        
        metadata_[id] = meta;
    }
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
