#include "vector_store.h"
#include <iostream>
#include <cstring>
#include <algorithm>

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
    // Check in-memory cache first (similar to memory_index_ lookup in DiskBPlusTree)
    if (memory_cache_loaded_) {
        auto cache_it = memory_cache_.find(vector_id);
        if (cache_it != memory_cache_.end()) {
            const CachedVector& cached = cache_it->second;
            actual_size = cached.size;
            vector = cached.data;  // Copy from cache
            return;
        }
    }
    
    // Fall back to disk read
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
    clearMemoryCache();
}

size_t VectorStore::estimateMemoryUsageMB() const {
    size_t total_bytes = 0;
    for (const auto& pair : metadata_) {
        // Each vector: size floats * 4 bytes + overhead (~40 bytes per entry)
        total_bytes += pair.second.size * sizeof(float) + 40;
    }
    return total_bytes / (1024 * 1024);
}

bool VectorStore::loadAllVectorsIntoMemory(size_t max_memory_mb) {
    memory_cache_.clear();
    memory_cache_loaded_ = false;
    
    size_t total_vectors = metadata_.size();
    if (total_vectors == 0) {
        memory_cache_loaded_ = true;
        return true;
    }
    
    // Estimate memory usage
    size_t estimated_mb = estimateMemoryUsageMB();
    std::cout << "Estimated memory for " << total_vectors << " vectors: " << estimated_mb << " MB" << std::endl;
    
    // Check memory limit
    if (max_memory_mb > 0 && estimated_mb > max_memory_mb) {
        std::cout << "Memory limit exceeded (" << max_memory_mb << " MB). Loading partial cache..." << std::endl;
    }
    
    std::cout << "Loading vectors into memory..." << std::endl;
    
    // Sort metadata by offset for sequential disk reads (much faster than random seeks)
    std::vector<std::pair<uint64_t, VectorMetadata>> sorted_meta(metadata_.begin(), metadata_.end());
    std::sort(sorted_meta.begin(), sorted_meta.end(), 
              [](const auto& a, const auto& b) { return a.second.offset < b.second.offset; });
    
    // Calculate how many vectors we can load within memory limit
    size_t vectors_to_load = total_vectors;
    if (max_memory_mb > 0) {
        size_t bytes_per_vector_avg = (estimated_mb * 1024 * 1024) / total_vectors;
        if (bytes_per_vector_avg > 0) {
            vectors_to_load = std::min(total_vectors, (max_memory_mb * 1024 * 1024) / bytes_per_vector_avg);
        }
    }
    
    // Pre-reserve hash map capacity
    memory_cache_.reserve(vectors_to_load);
    
    size_t loaded = 0;
    size_t last_progress = 0;
    size_t memory_used = 0;
    const size_t memory_limit_bytes = max_memory_mb * 1024 * 1024;
    
    for (const auto& pair : sorted_meta) {
        // Check memory limit before loading next vector
        if (max_memory_mb > 0 && memory_used >= memory_limit_bytes) {
            std::cout << "Memory limit reached at " << loaded << " vectors (" << (memory_used / (1024*1024)) << " MB)" << std::endl;
            break;
        }
        
        uint64_t vector_id = pair.first;
        const VectorMetadata& meta = pair.second;
        
        CachedVector cached;
        cached.size = meta.size;
        cached.data.resize(meta.size);
        
        // Seek to vector data
        file_.seekg(meta.offset);
        if (!file_.good()) {
            std::cerr << "Error seeking to vector " << vector_id << " at offset " << meta.offset << std::endl;
            return false;
        }
        
        // Skip stored_size (we already have it in metadata)
        file_.seekg(sizeof(uint32_t), std::ios::cur);
        
        // Bulk read entire vector at once instead of per-float
        file_.read(reinterpret_cast<char*>(cached.data.data()), meta.size * sizeof(float));
        if (!file_.good()) {
            std::cerr << "Error reading data for vector " << vector_id << std::endl;
            return false;
        }
        
        memory_used += meta.size * sizeof(float) + 40;  // Approximate overhead
        memory_cache_[vector_id] = std::move(cached);
        loaded++;
        
        // Progress logging every 10%
        size_t progress = (loaded * 100) / total_vectors;
        if (progress >= last_progress + 10) {
            std::cout << "  Vector loading progress: " << progress << "% (" << loaded << "/" << total_vectors << ", " << (memory_used / (1024*1024)) << " MB)" << std::endl;
            last_progress = progress;
        }
    }
    
    std::cout << "Loaded " << loaded << "/" << total_vectors << " vectors into memory (" << (memory_used / (1024*1024)) << " MB)" << std::endl;
    memory_cache_loaded_ = true;
    return true;
}

void VectorStore::clearMemoryCache() {
    memory_cache_.clear();
    memory_cache_loaded_ = false;
}
