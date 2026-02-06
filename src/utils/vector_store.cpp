#include "vector_store.h"
#include <iostream>
#include <cstring>
#include <algorithm>
#include <cmath>

// File format (Model B - linked list support):
// Header (24 bytes):
//   - magic (4 bytes): 0x56535432 (VS2 for version 2)
//   - version (4 bytes): 2
//   - next_vector_id (8 bytes)
//   - data_start_offset (4 bytes): where vector data begins
//   - max_vector_size (4 bytes): max dimension
// 
// Vector data (starting at data_start_offset):
//   For each vector: size (4 bytes) + next_id (8 bytes) + floats (size * 4 bytes)
//
// Metadata is stored in a separate .meta file

static constexpr uint32_t HEADER_SIZE = 24;
static constexpr uint32_t MAGIC_VS2 = 0x56535432;  // "VS2"

VectorStore::VectorStore(const std::string& filename, uint32_t max_vector_size)
    : filename_(filename), max_vector_size_(max_vector_size), next_vector_id_(1) {
    
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
    
    uint32_t magic = MAGIC_VS2;
    file_.write(reinterpret_cast<const char*>(&magic), sizeof(uint32_t));
    
    uint32_t version = 2;
    file_.write(reinterpret_cast<const char*>(&version), sizeof(uint32_t));
    
    uint64_t next_id = 1;  // Start from 1, 0 means "no vector"
    file_.write(reinterpret_cast<const char*>(&next_id), sizeof(uint64_t));
    
    uint32_t data_start = HEADER_SIZE;
    file_.write(reinterpret_cast<const char*>(&data_start), sizeof(uint32_t));
    
    file_.write(reinterpret_cast<const char*>(&max_vector_size_), sizeof(uint32_t));
    
    file_.flush();
    
    // Clear metadata file
    std::ofstream meta_file(filename_ + ".meta", std::ios::binary | std::ios::trunc);
    uint32_t count = 0;
    meta_file.write(reinterpret_cast<const char*>(&count), sizeof(uint32_t));
    meta_file.close();
    
    next_vector_id_ = 1;
}

void VectorStore::loadExistingFile() {
    file_.seekg(0);
    
    uint32_t magic;
    file_.read(reinterpret_cast<char*>(&magic), sizeof(uint32_t));
    if (magic != MAGIC_VS2) {
        throw std::runtime_error("Invalid or old version vector store file. Please rebuild index.");
    }
    
    uint32_t version;
    file_.read(reinterpret_cast<char*>(&version), sizeof(uint32_t));
    if (version != 2) {
        throw std::runtime_error("Unsupported vector store version: " + std::to_string(version));
    }
    
    file_.read(reinterpret_cast<char*>(&next_vector_id_), sizeof(uint64_t));
    
    uint32_t data_start;
    file_.read(reinterpret_cast<char*>(&data_start), sizeof(uint32_t));
    
    file_.read(reinterpret_cast<char*>(&max_vector_size_), sizeof(uint32_t));
    
    readMetadata();
}

void VectorStore::storeVectorInternal(uint64_t vector_id, const std::vector<float>& vector, 
                                      uint32_t actual_size, uint64_t next_id) {
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
    
    // Write: size (4 bytes) + next_id (8 bytes) + vector data (bulk write)
    file_.write(reinterpret_cast<const char*>(&actual_size), sizeof(uint32_t));
    file_.write(reinterpret_cast<const char*>(&next_id), sizeof(uint64_t));
    
    if (vector.size() >= actual_size) {
        // Common path: vector has enough data, single bulk write
        file_.write(reinterpret_cast<const char*>(vector.data()), actual_size * sizeof(float));
    } else {
        // Rare path: vector shorter than actual_size, zero-pad
        std::vector<float> padded(actual_size, 0.0f);
        std::copy(vector.begin(), vector.end(), padded.begin());
        file_.write(reinterpret_cast<const char*>(padded.data()), actual_size * sizeof(float));
    }
    
    // Batched flush: only flush every FLUSH_INTERVAL writes
    writes_since_flush_++;
    if (writes_since_flush_ >= FLUSH_INTERVAL) {
        file_.flush();
        writes_since_flush_ = 0;
    }
    
    metadata_[vector_id] = {offset, actual_size, next_id};
    
    if (vector_id >= next_vector_id_) {
        next_vector_id_ = vector_id + 1;
    }
}

uint64_t VectorStore::storeVector(const std::vector<float>& vector, uint32_t actual_size) {
    uint64_t vector_id = next_vector_id_++;
    storeVectorInternal(vector_id, vector, actual_size, 0);  // 0 = no next
    return vector_id;
}

uint64_t VectorStore::appendVectorToList(uint64_t first_vector_id, const std::vector<float>& vector, uint32_t actual_size) {
    // Store new vector, pointing to the old first
    uint64_t new_id = next_vector_id_++;
    storeVectorInternal(new_id, vector, actual_size, first_vector_id);
    return new_id;  // New vector becomes the head of the list
}

void VectorStore::retrieveVector(uint64_t vector_id, std::vector<float>& vector, uint32_t& actual_size) {
    if (vector_id == 0) {
        throw std::runtime_error("Invalid vector ID: 0");
    }
    
    // Check in-memory cache first
    if (memory_cache_loaded_) {
        auto cache_it = memory_cache_.find(vector_id);
        if (cache_it != memory_cache_.end()) {
            const CachedVector& cached = cache_it->second;
            actual_size = cached.size;
            vector = cached.data;
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
    
    // Skip next_id (8 bytes)
    file_.seekg(sizeof(uint64_t), std::ios::cur);
    
    for (uint32_t i = 0; i < actual_size; i++) {
        file_.read(reinterpret_cast<char*>(&vector[i]), sizeof(float));
    }
}

void VectorStore::retrieveVectorList(uint64_t first_vector_id, uint32_t count,
                                     std::vector<std::vector<float>>& vectors,
                                     std::vector<uint32_t>& sizes) {
    vectors.clear();
    sizes.clear();
    vectors.reserve(count);
    sizes.reserve(count);
    
    uint64_t current_id = first_vector_id;
    uint32_t retrieved = 0;
    
    while (current_id != 0 && retrieved < count) {
        // Check cache first
        if (memory_cache_loaded_) {
            auto cache_it = memory_cache_.find(current_id);
            if (cache_it != memory_cache_.end()) {
                const CachedVector& cached = cache_it->second;
                vectors.push_back(cached.data);
                sizes.push_back(cached.size);
                current_id = cached.next_id;
                retrieved++;
                continue;
            }
        }
        
        // Disk read
        auto it = metadata_.find(current_id);
        if (it == metadata_.end()) {
            break;  // End of valid chain
        }
        
        const VectorMetadata& meta = it->second;
        
        std::vector<float> vec(meta.size);
        file_.seekg(meta.offset);
        
        uint32_t stored_size;
        file_.read(reinterpret_cast<char*>(&stored_size), sizeof(uint32_t));
        
        uint64_t next_id;
        file_.read(reinterpret_cast<char*>(&next_id), sizeof(uint64_t));
        
        file_.read(reinterpret_cast<char*>(vec.data()), meta.size * sizeof(float));
        
        vectors.push_back(std::move(vec));
        sizes.push_back(meta.size);
        
        current_id = next_id;
        retrieved++;
    }
}

uint64_t VectorStore::removeVectorFromList(uint64_t first_vector_id, uint32_t count,
                                           const std::vector<float>& vector_to_remove,
                                           uint32_t& new_count) {
    // Retrieve all vectors in the list
    std::vector<std::vector<float>> vectors;
    std::vector<uint32_t> sizes;
    retrieveVectorList(first_vector_id, count, vectors, sizes);
    
    // Find and remove the matching vector
    int remove_idx = -1;
    for (size_t i = 0; i < vectors.size(); i++) {
        if (vectors[i].size() == vector_to_remove.size()) {
            bool match = true;
            for (size_t j = 0; j < vectors[i].size(); j++) {
                if (std::abs(vectors[i][j] - vector_to_remove[j]) > 1e-6f) {
                    match = false;
                    break;
                }
            }
            if (match) {
                remove_idx = static_cast<int>(i);
                break;
            }
        }
    }
    
    if (remove_idx < 0) {
        // Vector not found, return unchanged
        new_count = count;
        return first_vector_id;
    }
    
    // Remove the vector
    vectors.erase(vectors.begin() + remove_idx);
    sizes.erase(sizes.begin() + remove_idx);
    new_count = static_cast<uint32_t>(vectors.size());
    
    if (vectors.empty()) {
        return 0;  // List is now empty
    }
    
    // Rebuild the list (store vectors in reverse order so first stored becomes head)
    uint64_t new_first_id = 0;
    for (int i = static_cast<int>(vectors.size()) - 1; i >= 0; i--) {
        if (new_first_id == 0) {
            new_first_id = storeVector(vectors[i], sizes[i]);
        } else {
            new_first_id = appendVectorToList(new_first_id, vectors[i], sizes[i]);
        }
    }
    
    return new_first_id;
}

void VectorStore::writeMetadata() {
    // Write next_vector_id to main file header
    file_.seekp(8);
    file_.write(reinterpret_cast<const char*>(&next_vector_id_), sizeof(uint64_t));
    file_.flush();
    
    // Write metadata to separate file (now includes next_id)
    std::ofstream meta_file(filename_ + ".meta", std::ios::binary | std::ios::trunc);
    if (!meta_file.is_open()) {
        return;
    }
    
    uint32_t count = static_cast<uint32_t>(metadata_.size());
    meta_file.write(reinterpret_cast<const char*>(&count), sizeof(uint32_t));
    
    for (const auto& pair : metadata_) {
        uint64_t id = pair.first;
        const VectorMetadata& meta = pair.second;
        
        meta_file.write(reinterpret_cast<const char*>(&id), sizeof(uint64_t));
        meta_file.write(reinterpret_cast<const char*>(&meta.offset), sizeof(uint64_t));
        meta_file.write(reinterpret_cast<const char*>(&meta.size), sizeof(uint32_t));
        meta_file.write(reinterpret_cast<const char*>(&meta.next_id), sizeof(uint64_t));
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
        meta_file.read(reinterpret_cast<char*>(&meta.next_id), sizeof(uint64_t));
        
        metadata_[id] = meta;
    }
    meta_file.close();
}

void VectorStore::flush() {
    if (file_.is_open()) {
        file_.flush();
    }
    writeMetadata();
}

void VectorStore::close() {
    if (file_.is_open()) {
        file_.flush();
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
    
    // Sort metadata by offset for sequential disk reads
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
        cached.next_id = meta.next_id;
        cached.data.resize(meta.size);
        
        // Seek to vector data
        file_.seekg(meta.offset);
        if (!file_.good()) {
            std::cerr << "Error seeking to vector " << vector_id << " at offset " << meta.offset << std::endl;
            return false;
        }
        
        // Skip stored_size (4 bytes) and next_id (8 bytes) - we have them in metadata
        file_.seekg(sizeof(uint32_t) + sizeof(uint64_t), std::ios::cur);
        
        // Bulk read entire vector at once
        file_.read(reinterpret_cast<char*>(cached.data.data()), meta.size * sizeof(float));
        if (!file_.good()) {
            std::cerr << "Error reading data for vector " << vector_id << std::endl;
            return false;
        }
        
        memory_used += meta.size * sizeof(float) + 48;  // Approximate overhead (includes next_id)
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
