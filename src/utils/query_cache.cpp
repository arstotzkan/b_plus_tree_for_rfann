#include "query_cache.h"
#include <filesystem>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <fstream>
#include <cmath>
#include <cstring>

namespace fs = std::filesystem;

static std::string uint64_to_hex(uint64_t value) {
    std::ostringstream oss;
    oss << std::hex << std::setfill('0') << std::setw(16) << value;
    return oss.str();
}

QueryCache::QueryCache(const std::string& index_dir, bool enabled)
    : index_dir_(index_dir), enabled_(enabled) {
    cache_dir_ = index_dir_ + "/.cache";
    inverted_index_path_ = cache_dir_ + "/inverted_index.bin";
    
    if (enabled_) {
        ensure_directories();
        load_inverted_index();
    }
}

QueryCache::~QueryCache() {
    if (enabled_) {
        save_inverted_index();
    }
}

void QueryCache::set_enabled(bool enabled) {
    if (enabled && !enabled_) {
        ensure_directories();
        load_inverted_index();
    }
    enabled_ = enabled;
}

bool QueryCache::is_enabled() const {
    return enabled_;
}

void QueryCache::ensure_directories() {
    fs::create_directories(index_dir_);
    fs::create_directories(cache_dir_);
}

std::string QueryCache::compute_query_hash(const std::vector<float>& query_vector, 
                                            int min_key, int max_key) const {
    uint64_t hash = 14695981039346656037ULL;
    const uint64_t prime = 1099511628211ULL;
    
    for (float f : query_vector) {
        uint32_t bits;
        std::memcpy(&bits, &f, sizeof(bits));
        hash ^= bits;
        hash *= prime;
    }
    
    hash ^= static_cast<uint64_t>(min_key);
    hash *= prime;
    hash ^= static_cast<uint64_t>(max_key);
    hash *= prime;
    
    return uint64_to_hex(hash);
}

bool QueryCache::has_cached_result(const std::string& query_id, int k) const {
    if (!enabled_) return false;
    if (query_ranges_.find(query_id) == query_ranges_.end()) return false;
    
    // Load and check if we have at least k neighbors cached
    CachedQueryResult result;
    if (!load_query_result(query_id, result)) return false;
    
    return static_cast<int>(result.neighbors.size()) >= k;
}

CachedQueryResult QueryCache::get_cached_result(const std::string& query_id, int k) {
    CachedQueryResult result;
    if (!enabled_) return result;
    
    if (load_query_result(query_id, result)) {
        result.last_used_date = std::time(nullptr);
        
        // Return only first k neighbors
        if (static_cast<int>(result.neighbors.size()) > k) {
            result.neighbors.resize(k);
        }
        
        save_query_result(result);
    }
    return result;
}

void QueryCache::store_result(const std::string& query_id,
                              const std::vector<float>& input_vector,
                              int min_key, int max_key, int k,
                              const std::vector<CachedNeighbor>& results,
                              const std::string& used_similar_query_id) {
    if (!enabled_) return;
    
    // If we used a similar query's results, don't cache this query
    // Instead, update the similar query's last_used_date
    if (!used_similar_query_id.empty()) {
        CachedQueryResult similar_result;
        if (load_query_result(used_similar_query_id, similar_result)) {
            similar_result.last_used_date = std::time(nullptr);
            save_query_result(similar_result);
        }
        return;
    }
    
    // Check if we already have a cached result
    CachedQueryResult existing;
    bool has_existing = load_query_result(query_id, existing);
    
    if (has_existing && existing.max_k >= k) {
        // Existing cache has same or more neighbors, don't update
        return;
    }
    
    CachedQueryResult cached;
    cached.query_id = query_id;
    cached.created_date = has_existing ? existing.created_date : std::time(nullptr);
    cached.last_used_date = std::time(nullptr);
    cached.input_vector = input_vector;
    cached.min_key = min_key;
    cached.max_key = max_key;
    cached.max_k = k;
    cached.neighbors = results;
    
    save_query_result(cached);
    
    if (!has_existing) {
        add_to_inverted_index(query_id, min_key, max_key);
        save_inverted_index();
    }
    
    enforce_cache_limit();
}

void QueryCache::invalidate_for_key(int key) {
    if (!enabled_) return;
    
    // Use interval tree for efficient O(log N) lookup instead of O(N) linear search
    std::vector<std::string> queries_to_remove;
    find_overlapping_intervals(interval_root_.get(), key, queries_to_remove);
    
    for (const auto& query_id : queries_to_remove) {
        remove_from_inverted_index(query_id);
        delete_query_result(query_id);
    }
    
    if (!queries_to_remove.empty()) {
        save_inverted_index();
    }
}

int QueryCache::update_for_inserted_object(int key, const std::vector<float>& vector,
                                            std::function<double(const std::vector<float>&, const std::vector<float>&)> distance_fn) {
    if (!enabled_) return 0;
    
    // Find all queries whose range contains this key
    std::vector<std::string> affected_queries;
    find_overlapping_intervals(interval_root_.get(), key, affected_queries);
    
    if (affected_queries.empty()) return 0;
    
    int updated_count = 0;
    
    for (const auto& query_id : affected_queries) {
        CachedQueryResult result;
        if (!load_query_result(query_id, result)) continue;
        if (result.neighbors.empty()) continue;
        
        // Calculate distance from query vector to new object
        double new_dist = distance_fn(result.input_vector, vector);
        
        // Get the furthest cached neighbor's distance
        double furthest_dist = result.neighbors.back().distance;
        
        // If new object is closer than furthest cached neighbor, insert it
        if (new_dist < furthest_dist || static_cast<int>(result.neighbors.size()) < result.max_k) {
            // Create new neighbor entry
            CachedNeighbor new_neighbor;
            new_neighbor.vector = vector;
            new_neighbor.key = key;
            new_neighbor.distance = new_dist;
            
            // Find insertion position (keep sorted by distance)
            auto insert_pos = std::lower_bound(result.neighbors.begin(), result.neighbors.end(), new_neighbor,
                [](const CachedNeighbor& a, const CachedNeighbor& b) {
                    return a.distance < b.distance;
                });
            
            result.neighbors.insert(insert_pos, new_neighbor);
            
            // Keep the furthest neighbor - allows cache to grow and serve higher K queries
            
            result.last_used_date = std::time(nullptr);
            save_query_result(result);
            updated_count++;
        }
    }
    
    return updated_count;
}

int QueryCache::update_for_deleted_object(int key, const std::vector<float>& vector) {
    if (!enabled_) return 0;
    
    // Find all queries whose range contains this key
    std::vector<std::string> affected_queries;
    find_overlapping_intervals(interval_root_.get(), key, affected_queries);
    
    if (affected_queries.empty()) return 0;
    
    int updated_count = 0;
    const float epsilon = 1e-3f;
    
    for (const auto& query_id : affected_queries) {
        CachedQueryResult result;
        if (!load_query_result(query_id, result)) continue;
        
        // Find and remove the deleted object from neighbors
        bool found = false;
        auto it = result.neighbors.begin();
        while (it != result.neighbors.end()) {
            if (it->key == key) {
                // Compare vectors
                bool vectors_match = true;
                if (it->vector.size() == vector.size()) {
                    for (size_t i = 0; i < vector.size(); i++) {
                        if (std::abs(it->vector[i] - vector[i]) > epsilon) {
                            vectors_match = false;
                            break;
                        }
                    }
                } else {
                    vectors_match = false;
                }
                
                if (vectors_match) {
                    it = result.neighbors.erase(it);
                    found = true;
                    break;  // Only remove first match
                } else {
                    ++it;
                }
            } else {
                ++it;
            }
        }
        
        if (found) {
            result.last_used_date = std::time(nullptr);
            save_query_result(result);
            updated_count++;
        }
    }
    
    return updated_count;
}

void QueryCache::load_config(const std::string& config_path) {
    std::ifstream file(config_path);
    if (!file.is_open()) return;
    
    std::string line;
    while (std::getline(file, line)) {
        if (line.empty() || line[0] == '#' || line[0] == '[') continue;
        
        auto eq_pos = line.find('=');
        if (eq_pos == std::string::npos) continue;
        
        std::string key = line.substr(0, eq_pos);
        std::string value = line.substr(eq_pos + 1);
        
        key.erase(0, key.find_first_not_of(" \t"));
        key.erase(key.find_last_not_of(" \t") + 1);
        value.erase(0, value.find_first_not_of(" \t"));
        value.erase(value.find_last_not_of(" \t") + 1);
        
        if (key == "max_cache_size_mb") {
            config_.max_cache_size_bytes = std::stoull(value) * 1024 * 1024;
        } else if (key == "cache_enabled") {
            config_.cache_enabled = (value == "true" || value == "1");
            enabled_ = config_.cache_enabled;
        }
    }
}

void QueryCache::enforce_cache_limit() {
    if (!enabled_) return;
    
    size_t current_size = get_cache_size();
    if (current_size <= config_.max_cache_size_bytes) return;
    
    auto queries_by_time = get_queries_by_last_used();
    
    std::sort(queries_by_time.begin(), queries_by_time.end(),
              [](const auto& a, const auto& b) { return a.second < b.second; });
    
    for (const auto& [query_id, _] : queries_by_time) {
        if (current_size <= config_.max_cache_size_bytes) break;
        
        std::string file_path = get_query_file_path(query_id);
        if (fs::exists(file_path)) {
            size_t file_size = fs::file_size(file_path);
            remove_from_inverted_index(query_id);
            delete_query_result(query_id);
            current_size -= file_size;
        }
    }
    save_inverted_index();
}

std::string QueryCache::get_query_file_path(const std::string& query_id) const {
    return cache_dir_ + "/" + query_id + ".qcache";
}

bool QueryCache::load_query_result(const std::string& query_id, CachedQueryResult& result) const {
    std::string path = get_query_file_path(query_id);
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) return false;
    
    result.query_id = query_id;
    
    file.read(reinterpret_cast<char*>(&result.created_date), sizeof(result.created_date));
    file.read(reinterpret_cast<char*>(&result.last_used_date), sizeof(result.last_used_date));
    file.read(reinterpret_cast<char*>(&result.min_key), sizeof(result.min_key));
    file.read(reinterpret_cast<char*>(&result.max_key), sizeof(result.max_key));
    file.read(reinterpret_cast<char*>(&result.max_k), sizeof(result.max_k));
    
    uint32_t vec_size;
    file.read(reinterpret_cast<char*>(&vec_size), sizeof(vec_size));
    result.input_vector.resize(vec_size);
    file.read(reinterpret_cast<char*>(result.input_vector.data()), vec_size * sizeof(float));
    
    uint32_t num_neighbors;
    file.read(reinterpret_cast<char*>(&num_neighbors), sizeof(num_neighbors));
    result.neighbors.resize(num_neighbors);
    
    for (uint32_t i = 0; i < num_neighbors; i++) {
        uint32_t neighbor_vec_size;
        file.read(reinterpret_cast<char*>(&neighbor_vec_size), sizeof(neighbor_vec_size));
        result.neighbors[i].vector.resize(neighbor_vec_size);
        file.read(reinterpret_cast<char*>(result.neighbors[i].vector.data()), neighbor_vec_size * sizeof(float));
        file.read(reinterpret_cast<char*>(&result.neighbors[i].key), sizeof(int));
        file.read(reinterpret_cast<char*>(&result.neighbors[i].distance), sizeof(double));
    }
    
    return file.good();
}

void QueryCache::save_query_result(const CachedQueryResult& result) {
    std::string path = get_query_file_path(result.query_id);
    std::ofstream file(path, std::ios::binary);
    if (!file.is_open()) return;
    
    file.write(reinterpret_cast<const char*>(&result.created_date), sizeof(result.created_date));
    file.write(reinterpret_cast<const char*>(&result.last_used_date), sizeof(result.last_used_date));
    file.write(reinterpret_cast<const char*>(&result.min_key), sizeof(result.min_key));
    file.write(reinterpret_cast<const char*>(&result.max_key), sizeof(result.max_key));
    file.write(reinterpret_cast<const char*>(&result.max_k), sizeof(result.max_k));
    
    uint32_t vec_size = static_cast<uint32_t>(result.input_vector.size());
    file.write(reinterpret_cast<const char*>(&vec_size), sizeof(vec_size));
    file.write(reinterpret_cast<const char*>(result.input_vector.data()), vec_size * sizeof(float));
    
    uint32_t num_neighbors = static_cast<uint32_t>(result.neighbors.size());
    file.write(reinterpret_cast<const char*>(&num_neighbors), sizeof(num_neighbors));
    
    for (const auto& neighbor : result.neighbors) {
        uint32_t neighbor_vec_size = static_cast<uint32_t>(neighbor.vector.size());
        file.write(reinterpret_cast<const char*>(&neighbor_vec_size), sizeof(neighbor_vec_size));
        file.write(reinterpret_cast<const char*>(neighbor.vector.data()), neighbor_vec_size * sizeof(float));
        file.write(reinterpret_cast<const char*>(&neighbor.key), sizeof(int));
        file.write(reinterpret_cast<const char*>(&neighbor.distance), sizeof(double));
    }
}

void QueryCache::delete_query_result(const std::string& query_id) {
    std::string path = get_query_file_path(query_id);
    fs::remove(path);
}

void QueryCache::add_to_inverted_index(const std::string& query_id, int min_key, int max_key) {
    // Store only range bounds - O(1) instead of O(N) where N = range size
    query_ranges_[query_id] = {min_key, max_key};
    
    // Add to interval tree for efficient lookups
    insert_interval(interval_root_, min_key, max_key, query_id);
}

void QueryCache::remove_from_inverted_index(const std::string& query_id) {
    // Remove from interval tree
    remove_interval(interval_root_, query_id);
    
    query_ranges_.erase(query_id);
}

void QueryCache::load_inverted_index() {
    std::ifstream file(inverted_index_path_, std::ios::binary);
    if (!file.is_open()) return;
    
    query_ranges_.clear();
    interval_root_.reset();  // Clear interval tree
    
    uint32_t num_queries;
    file.read(reinterpret_cast<char*>(&num_queries), sizeof(num_queries));
    
    for (uint32_t i = 0; i < num_queries; i++) {
        uint32_t id_len;
        file.read(reinterpret_cast<char*>(&id_len), sizeof(id_len));
        std::string query_id(id_len, '\0');
        file.read(&query_id[0], id_len);
        
        int min_key, max_key;
        file.read(reinterpret_cast<char*>(&min_key), sizeof(min_key));
        file.read(reinterpret_cast<char*>(&max_key), sizeof(max_key));
        
        query_ranges_[query_id] = {min_key, max_key};
        // Rebuild interval tree
        insert_interval(interval_root_, min_key, max_key, query_id);
    }
}

void QueryCache::save_inverted_index() {
    std::ofstream file(inverted_index_path_, std::ios::binary);
    if (!file.is_open()) return;
    
    uint32_t num_queries = static_cast<uint32_t>(query_ranges_.size());
    file.write(reinterpret_cast<const char*>(&num_queries), sizeof(num_queries));
    
    for (const auto& [query_id, range] : query_ranges_) {
        uint32_t id_len = static_cast<uint32_t>(query_id.size());
        file.write(reinterpret_cast<const char*>(&id_len), sizeof(id_len));
        file.write(query_id.data(), id_len);
        
        file.write(reinterpret_cast<const char*>(&range.min_key), sizeof(range.min_key));
        file.write(reinterpret_cast<const char*>(&range.max_key), sizeof(range.max_key));
    }
    
    // Note: Interval tree is rebuilt from query_ranges_ on load, so no need to persist it separately
}

size_t QueryCache::get_cache_size() const {
    size_t total = 0;
    for (const auto& entry : fs::directory_iterator(cache_dir_)) {
        if (entry.is_regular_file() && entry.path().extension() == ".qcache") {
            total += entry.file_size();
        }
    }
    return total;
}

std::vector<std::pair<std::string, std::time_t>> QueryCache::get_queries_by_last_used() const {
    std::vector<std::pair<std::string, std::time_t>> result;
    
    for (const auto& [query_id, _] : query_ranges_) {
        CachedQueryResult cached;
        if (load_query_result(query_id, cached)) {
            result.push_back({query_id, cached.last_used_date});
        }
    }
    
    return result;
}

// Public API for B+ tree operations: efficiently find all queries containing a key
std::vector<std::string> QueryCache::get_queries_containing_key(int key) const {
    std::vector<std::string> result;
    if (!enabled_) return result;
    
    find_overlapping_intervals(interval_root_.get(), key, result);
    return result;
}

// Interval tree implementation for efficient range queries
void QueryCache::insert_interval(std::unique_ptr<IntervalNode>& node, int start, int end, const std::string& query_id) {
    if (!node) {
        node = std::make_unique<IntervalNode>(start, end, query_id);
        return;
    }
    
    // Update max_end for this subtree
    if (end > node->max_end) {
        node->max_end = end;
    }
    
    // Insert based on start value (BST property)
    if (start < node->start) {
        insert_interval(node->left, start, end, query_id);
    } else {
        insert_interval(node->right, start, end, query_id);
    }
    
    // Update max_end after insertion
    update_max_end(node.get());
}

void QueryCache::remove_interval(std::unique_ptr<IntervalNode>& node, const std::string& query_id) {
    if (!node) return;
    
    if (node->query_id == query_id) {
        // Found the node to remove
        if (!node->left && !node->right) {
            // Leaf node
            node.reset();
        } else if (!node->left) {
            // Only right child
            node = std::move(node->right);
        } else if (!node->right) {
            // Only left child
            node = std::move(node->left);
        } else {
            // Two children - find inorder successor
            IntervalNode* successor = node->right.get();
            while (successor->left) {
                successor = successor->left.get();
            }
            
            // Replace current node's data with successor's data
            node->start = successor->start;
            node->end = successor->end;
            node->query_id = successor->query_id;
            
            // Remove successor
            remove_interval(node->right, successor->query_id);
        }
    } else {
        // Recursively search in subtrees
        remove_interval(node->left, query_id);
        remove_interval(node->right, query_id);
    }
    
    // Update max_end after removal
    if (node) {
        update_max_end(node.get());
    }
}

void QueryCache::find_overlapping_intervals(const IntervalNode* node, int key, std::vector<std::string>& result) const {
    if (!node) return;
    
    // Check if current interval contains the key
    if (key >= node->start && key <= node->end) {
        result.push_back(node->query_id);
    }
    
    // If left subtree exists and its max_end >= key, search left
    if (node->left && node->left->max_end >= key) {
        find_overlapping_intervals(node->left.get(), key, result);
    }
    
    // If current node's start <= key, search right subtree
    if (node->start <= key && node->right) {
        find_overlapping_intervals(node->right.get(), key, result);
    }
}

void QueryCache::find_overlapping_range(const IntervalNode* node, int min_key, int max_key, std::vector<std::string>& result) const {
    if (!node) return;
    
    // Check if current interval overlaps with [min_key, max_key]
    // Two intervals [a,b] and [c,d] overlap iff a <= d && c <= b
    if (node->start <= max_key && min_key <= node->end) {
        result.push_back(node->query_id);
    }
    
    // If left subtree exists and its max_end >= min_key, there might be overlapping intervals
    if (node->left && node->left->max_end >= min_key) {
        find_overlapping_range(node->left.get(), min_key, max_key, result);
    }
    
    // If current node's start <= max_key, search right subtree
    // (intervals in right subtree have start >= node->start, so they could still overlap)
    if (node->start <= max_key && node->right) {
        find_overlapping_range(node->right.get(), min_key, max_key, result);
    }
}

void QueryCache::update_max_end(IntervalNode* node) {
    if (!node) return;
    
    node->max_end = node->end;
    if (node->left && node->left->max_end > node->max_end) {
        node->max_end = node->left->max_end;
    }
    if (node->right && node->right->max_end > node->max_end) {
        node->max_end = node->right->max_end;
    }
}

// ============================================================================
// SIMILARITY METRICS
// ============================================================================

double QueryCache::compute_vector_cosine_similarity(const std::vector<float>& v1, const std::vector<float>& v2) {
    // Cosine Similarity: cos(θ) = (A · B) / (||A|| * ||B||)
    // Returns value in range [-1, 1], where:
    //   1.0  = identical direction (most similar)
    //   0.0  = orthogonal (unrelated)
    //  -1.0  = opposite direction (most dissimilar)
    // We normalize to [0, 1] for threshold comparison: (cos + 1) / 2
    
    if (v1.empty() || v2.empty()) return 0.0;
    if (v1.size() != v2.size()) return 0.0;
    
    double dot_product = 0.0;
    double norm_v1 = 0.0;
    double norm_v2 = 0.0;
    
    for (size_t i = 0; i < v1.size(); i++) {
        dot_product += static_cast<double>(v1[i]) * static_cast<double>(v2[i]);
        norm_v1 += static_cast<double>(v1[i]) * static_cast<double>(v1[i]);
        norm_v2 += static_cast<double>(v2[i]) * static_cast<double>(v2[i]);
    }
    
    norm_v1 = std::sqrt(norm_v1);
    norm_v2 = std::sqrt(norm_v2);
    
    if (norm_v1 < 1e-10 || norm_v2 < 1e-10) return 0.0;
    
    double cosine = dot_product / (norm_v1 * norm_v2);
    
    // Clamp to [-1, 1] to handle floating point errors
    cosine = std::max(-1.0, std::min(1.0, cosine));
    
    // Normalize to [0, 1]: (cosine + 1) / 2
    // This makes 1.0 = identical, 0.5 = orthogonal, 0.0 = opposite
    return (cosine + 1.0) / 2.0;
}

double QueryCache::compute_range_iou(int min1, int max1, int min2, int max2) {
    // Range IoU (Intersection over Union) for 1D intervals
    // Also known as Jaccard Index for intervals
    // Returns value in range [0, 1], where:
    //   1.0 = identical ranges
    //   0.0 = no overlap
    //
    // Formula: IoU = |intersection| / |union|
    //   intersection = max(0, min(max1, max2) - max(min1, min2) + 1)
    //   union = max(max1, max2) - min(min1, min2) + 1
    
    // Calculate intersection
    int intersection_start = std::max(min1, min2);
    int intersection_end = std::min(max1, max2);
    int intersection_size = std::max(0, intersection_end - intersection_start + 1);
    
    if (intersection_size == 0) return 0.0;
    
    // Calculate union
    int union_start = std::min(min1, min2);
    int union_end = std::max(max1, max2);
    int union_size = union_end - union_start + 1;
    
    if (union_size <= 0) return 0.0;
    
    return static_cast<double>(intersection_size) / static_cast<double>(union_size);
}

SimilarCacheMatch QueryCache::find_similar_cached_result(
    const std::vector<float>& query_vector,
    int min_key, int max_key, int k,
    const SimilarityThresholds& thresholds) const {
    
    SimilarCacheMatch best_match;
    if (!enabled_) return best_match;
    
    // First check for exact match (fast path)
    std::string exact_hash = compute_query_hash(query_vector, min_key, max_key);
    if (has_cached_result(exact_hash, k)) {
        CachedQueryResult cached;
        if (load_query_result(exact_hash, cached)) {
            best_match.found = true;
            best_match.query_id = exact_hash;
            best_match.vector_similarity = 1.0;
            best_match.range_similarity = 1.0;
            best_match.result = cached;
            // Trim to k neighbors
            if (static_cast<int>(best_match.result.neighbors.size()) > k) {
                best_match.result.neighbors.resize(k);
            }
            return best_match;
        }
    }
    
    // If thresholds require exact match, return not found
    if (thresholds.vector_similarity_threshold >= 1.0 && 
        thresholds.range_similarity_threshold >= 1.0) {
        return best_match;
    }
    
    // Use interval tree to efficiently find cached queries that overlap with the requested range
    // This is O(log N + M) where M is the number of overlapping intervals, instead of O(N) linear scan
    std::vector<std::string> candidate_queries;
    find_overlapping_range(interval_root_.get(), min_key, max_key, candidate_queries);
    
    // Search only through overlapping cached queries
    double best_combined_score = 0.0;
    
    for (const std::string& query_id : candidate_queries) {
        auto range_it = query_ranges_.find(query_id);
        if (range_it == query_ranges_.end()) continue;
        const QueryRange& range = range_it->second;
        
        // Compute range similarity (we know they overlap, but need IoU score)
        double range_sim = compute_range_iou(min_key, max_key, range.min_key, range.max_key);
        if (range_sim < thresholds.range_similarity_threshold) {
            continue;  // Range IoU doesn't meet threshold
        }
        
        // Load the cached result to compare vectors
        CachedQueryResult cached;
        if (!load_query_result(query_id, cached)) continue;
        if (static_cast<int>(cached.neighbors.size()) < k) continue;
        
        // Compute vector similarity
        double vec_sim = compute_vector_cosine_similarity(query_vector, cached.input_vector);
        if (vec_sim < thresholds.vector_similarity_threshold) {
            continue;  // Vector doesn't meet threshold
        }
        
        // Both thresholds met - check if this is the best match
        // Use geometric mean of similarities as combined score
        double combined_score = std::sqrt(vec_sim * range_sim);
        
        if (combined_score > best_combined_score) {
            best_combined_score = combined_score;
            best_match.found = true;
            best_match.query_id = query_id;
            best_match.vector_similarity = vec_sim;
            best_match.range_similarity = range_sim;
            best_match.result = cached;
        }
    }
    
    // Trim result to k neighbors if found
    if (best_match.found && static_cast<int>(best_match.result.neighbors.size()) > k) {
        best_match.result.neighbors.resize(k);
    }
    
    return best_match;
}
