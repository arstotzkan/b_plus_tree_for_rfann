#include "query_cache.h"
#include <filesystem>
#include <sstream>
#include <iomanip>
#include <algorithm>
#include <fstream>
#include <cmath>

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
                                            int min_key, int max_key, int k) const {
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
    hash ^= static_cast<uint64_t>(k);
    hash *= prime;
    
    return uint64_to_hex(hash);
}

bool QueryCache::has_cached_result(const std::string& query_id) const {
    if (!enabled_) return false;
    return query_to_keys_.find(query_id) != query_to_keys_.end();
}

CachedQueryResult QueryCache::get_cached_result(const std::string& query_id) {
    CachedQueryResult result;
    if (!enabled_) return result;
    
    if (load_query_result(query_id, result)) {
        result.last_used_date = std::time(nullptr);
        save_query_result(result);
    }
    return result;
}

void QueryCache::store_result(const std::string& query_id,
                              const std::vector<float>& input_vector,
                              int min_key, int max_key, int k,
                              const std::vector<std::pair<std::vector<float>, int>>& results) {
    if (!enabled_) return;
    
    CachedQueryResult cached;
    cached.query_id = query_id;
    cached.created_date = std::time(nullptr);
    cached.last_used_date = cached.created_date;
    cached.input_vector = input_vector;
    cached.min_key = min_key;
    cached.max_key = max_key;
    cached.k_neighbors = k;
    cached.output_objects = results;
    
    save_query_result(cached);
    add_to_inverted_index(query_id, min_key, max_key);
    save_inverted_index();
    
    enforce_cache_limit();
}

void QueryCache::invalidate_for_key(int key) {
    if (!enabled_) return;
    
    auto it = key_to_queries_.find(key);
    if (it == key_to_queries_.end()) return;
    
    std::vector<std::string> queries_to_remove(it->second.begin(), it->second.end());
    for (const auto& query_id : queries_to_remove) {
        remove_from_inverted_index(query_id);
        delete_query_result(query_id);
    }
    save_inverted_index();
}

void QueryCache::invalidate_if_affected(int key, const std::vector<float>& new_vector,
                                         std::function<double(const std::vector<float>&, const std::vector<float>&)> distance_fn) {
    if (!enabled_) return;
    
    auto it = key_to_queries_.find(key);
    if (it == key_to_queries_.end()) return;
    
    std::vector<std::string> queries_to_check(it->second.begin(), it->second.end());
    
    for (const auto& query_id : queries_to_check) {
        CachedQueryResult result;
        if (!load_query_result(query_id, result)) continue;
        if (result.output_objects.empty()) continue;
        
        double new_dist = distance_fn(result.input_vector, new_vector);
        
        double farthest_dist = 0.0;
        for (const auto& obj : result.output_objects) {
            double dist = distance_fn(result.input_vector, obj.first);
            if (dist > farthest_dist) {
                farthest_dist = dist;
            }
        }
        
        if (new_dist < farthest_dist) {
            remove_from_inverted_index(query_id);
            delete_query_result(query_id);
        }
    }
    save_inverted_index();
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
    file.read(reinterpret_cast<char*>(&result.k_neighbors), sizeof(result.k_neighbors));
    
    uint32_t vec_size;
    file.read(reinterpret_cast<char*>(&vec_size), sizeof(vec_size));
    result.input_vector.resize(vec_size);
    file.read(reinterpret_cast<char*>(result.input_vector.data()), vec_size * sizeof(float));
    
    uint32_t num_results;
    file.read(reinterpret_cast<char*>(&num_results), sizeof(num_results));
    result.output_objects.resize(num_results);
    
    for (uint32_t i = 0; i < num_results; i++) {
        uint32_t obj_vec_size;
        file.read(reinterpret_cast<char*>(&obj_vec_size), sizeof(obj_vec_size));
        result.output_objects[i].first.resize(obj_vec_size);
        file.read(reinterpret_cast<char*>(result.output_objects[i].first.data()), obj_vec_size * sizeof(float));
        file.read(reinterpret_cast<char*>(&result.output_objects[i].second), sizeof(int));
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
    file.write(reinterpret_cast<const char*>(&result.k_neighbors), sizeof(result.k_neighbors));
    
    uint32_t vec_size = static_cast<uint32_t>(result.input_vector.size());
    file.write(reinterpret_cast<const char*>(&vec_size), sizeof(vec_size));
    file.write(reinterpret_cast<const char*>(result.input_vector.data()), vec_size * sizeof(float));
    
    uint32_t num_results = static_cast<uint32_t>(result.output_objects.size());
    file.write(reinterpret_cast<const char*>(&num_results), sizeof(num_results));
    
    for (const auto& obj : result.output_objects) {
        uint32_t obj_vec_size = static_cast<uint32_t>(obj.first.size());
        file.write(reinterpret_cast<const char*>(&obj_vec_size), sizeof(obj_vec_size));
        file.write(reinterpret_cast<const char*>(obj.first.data()), obj_vec_size * sizeof(float));
        file.write(reinterpret_cast<const char*>(&obj.second), sizeof(int));
    }
}

void QueryCache::delete_query_result(const std::string& query_id) {
    std::string path = get_query_file_path(query_id);
    fs::remove(path);
}

void QueryCache::add_to_inverted_index(const std::string& query_id, int min_key, int max_key) {
    std::unordered_set<int> keys;
    for (int k = min_key; k <= max_key; k++) {
        keys.insert(k);
        key_to_queries_[k].insert(query_id);
    }
    query_to_keys_[query_id] = keys;
}

void QueryCache::remove_from_inverted_index(const std::string& query_id) {
    auto it = query_to_keys_.find(query_id);
    if (it == query_to_keys_.end()) return;
    
    for (int key : it->second) {
        auto kit = key_to_queries_.find(key);
        if (kit != key_to_queries_.end()) {
            kit->second.erase(query_id);
            if (kit->second.empty()) {
                key_to_queries_.erase(kit);
            }
        }
    }
    query_to_keys_.erase(it);
}

void QueryCache::load_inverted_index() {
    std::ifstream file(inverted_index_path_, std::ios::binary);
    if (!file.is_open()) return;
    
    key_to_queries_.clear();
    query_to_keys_.clear();
    
    uint32_t num_queries;
    file.read(reinterpret_cast<char*>(&num_queries), sizeof(num_queries));
    
    for (uint32_t i = 0; i < num_queries; i++) {
        uint32_t id_len;
        file.read(reinterpret_cast<char*>(&id_len), sizeof(id_len));
        std::string query_id(id_len, '\0');
        file.read(&query_id[0], id_len);
        
        uint32_t num_keys;
        file.read(reinterpret_cast<char*>(&num_keys), sizeof(num_keys));
        
        std::unordered_set<int> keys;
        for (uint32_t j = 0; j < num_keys; j++) {
            int key;
            file.read(reinterpret_cast<char*>(&key), sizeof(key));
            keys.insert(key);
            key_to_queries_[key].insert(query_id);
        }
        query_to_keys_[query_id] = keys;
    }
}

void QueryCache::save_inverted_index() {
    std::ofstream file(inverted_index_path_, std::ios::binary);
    if (!file.is_open()) return;
    
    uint32_t num_queries = static_cast<uint32_t>(query_to_keys_.size());
    file.write(reinterpret_cast<const char*>(&num_queries), sizeof(num_queries));
    
    for (const auto& [query_id, keys] : query_to_keys_) {
        uint32_t id_len = static_cast<uint32_t>(query_id.size());
        file.write(reinterpret_cast<const char*>(&id_len), sizeof(id_len));
        file.write(query_id.data(), id_len);
        
        uint32_t num_keys = static_cast<uint32_t>(keys.size());
        file.write(reinterpret_cast<const char*>(&num_keys), sizeof(num_keys));
        
        for (int key : keys) {
            file.write(reinterpret_cast<const char*>(&key), sizeof(key));
        }
    }
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
    
    for (const auto& [query_id, _] : query_to_keys_) {
        CachedQueryResult cached;
        if (load_query_result(query_id, cached)) {
            result.push_back({query_id, cached.last_used_date});
        }
    }
    
    return result;
}
