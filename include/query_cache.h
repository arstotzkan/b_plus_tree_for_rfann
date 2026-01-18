#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <cstdint>
#include <fstream>
#include <ctime>
#include <functional>
#include "DataObject.h"

// Cached neighbor with distance for sorted storage
struct CachedNeighbor {
    std::vector<float> vector;
    int key;
    double distance;  // Distance from query vector
};

struct CachedQueryResult {
    std::string query_id;
    std::time_t created_date;
    std::time_t last_used_date;
    std::vector<float> input_vector;
    int min_key;
    int max_key;
    int max_k;  // Maximum K stored (highest K ever requested for this query)
    std::vector<CachedNeighbor> neighbors;  // Sorted by distance (nearest first)
};

struct CacheConfig {
    size_t max_cache_size_bytes = 100 * 1024 * 1024;
    bool cache_enabled = true;
};

// Similarity thresholds for approximate cache matching
struct SimilarityThresholds {
    double vector_similarity_threshold = 1.0;  // 1.0 = exact match only, 0.0 = any vector
    double range_similarity_threshold = 1.0;   // 1.0 = exact match only, 0.0 = any range
    
    // Default: exact match only (backward compatible)
    SimilarityThresholds() = default;
    SimilarityThresholds(double vec_thresh, double range_thresh)
        : vector_similarity_threshold(vec_thresh), range_similarity_threshold(range_thresh) {}
};

// Result of similarity-based cache lookup
struct SimilarCacheMatch {
    bool found = false;
    std::string query_id;
    double vector_similarity = 0.0;  // Cosine similarity [0, 1]
    double range_similarity = 0.0;   // Range IoU [0, 1]
    CachedQueryResult result;
};

class QueryCache {
public:
    explicit QueryCache(const std::string& index_dir, bool enabled = true);
    ~QueryCache();

    void set_enabled(bool enabled);
    bool is_enabled() const;

    // Compute hash from vector/min/max only (K is not part of the hash)
    std::string compute_query_hash(const std::vector<float>& query_vector, int min_key, int max_key) const;

    // Check if cache has result with at least k neighbors
    bool has_cached_result(const std::string& query_id, int k) const;

    // Get cached result, returns only first k neighbors
    CachedQueryResult get_cached_result(const std::string& query_id, int k);

    // Store result - updates existing cache if new K is higher
    // If used_similar_query_id is non-empty, skip caching and update that query's last_used instead
    void store_result(const std::string& query_id,
                      const std::vector<float>& input_vector,
                      int min_key, int max_key, int k,
                      const std::vector<CachedNeighbor>& results,
                      const std::string& used_similar_query_id = "");
    
    // Similarity-based cache lookup
    // Returns the best matching cached query if both similarities exceed thresholds
    SimilarCacheMatch find_similar_cached_result(
        const std::vector<float>& query_vector,
        int min_key, int max_key, int k,
        const SimilarityThresholds& thresholds) const;
    
    // Similarity metrics (public for testing/inspection)
    static double compute_vector_cosine_similarity(const std::vector<float>& v1, const std::vector<float>& v2);
    static double compute_range_iou(int min1, int max1, int min2, int max2);

    // Update cache when a new DataObject is inserted into the tree
    // Returns number of caches updated
    int update_for_inserted_object(int key, const std::vector<float>& vector,
                                   std::function<double(const std::vector<float>&, const std::vector<float>&)> distance_fn);

    // Update cache when a DataObject is deleted from the tree
    // Returns number of caches updated
    int update_for_deleted_object(int key, const std::vector<float>& vector);

    void invalidate_for_key(int key);

    // Public access for cache inspection utilities
    bool load_query_result(const std::string& query_id, CachedQueryResult& result) const;

    void load_config(const std::string& config_path);
    void enforce_cache_limit();
    
    // Public API for B+ tree operations: efficiently find all queries containing a key
    std::vector<std::string> get_queries_containing_key(int key) const;

    std::string get_index_dir() const { return index_dir_; }
    std::string get_cache_dir() const { return cache_dir_; }

private:
    std::string index_dir_;
    std::string cache_dir_;
    std::string inverted_index_path_;
    bool enabled_;
    CacheConfig config_;

    // Store range bounds instead of individual keys to avoid O(N) memory per query
    struct QueryRange {
        int min_key;
        int max_key;
    };
    std::unordered_map<std::string, QueryRange> query_ranges_;  // query_id -> range
    
    // Interval tree for efficient range lookups: O(log N) instead of O(N)
    struct IntervalNode {
        int start, end;           // Range bounds
        int max_end;              // Maximum end value in subtree (for pruning)
        std::string query_id;     // Associated query ID
        std::unique_ptr<IntervalNode> left, right;
        
        IntervalNode(int s, int e, const std::string& id) 
            : start(s), end(e), max_end(e), query_id(id) {}
    };
    
    std::unique_ptr<IntervalNode> interval_root_;  // Root of interval tree

    void ensure_directories();
    void load_inverted_index();
    void save_inverted_index();

    std::string get_query_file_path(const std::string& query_id) const;
    void save_query_result(const CachedQueryResult& result);
    void delete_query_result(const std::string& query_id);

    void add_to_inverted_index(const std::string& query_id, int min_key, int max_key);
    void remove_from_inverted_index(const std::string& query_id);
    
    // Interval tree operations for efficient range queries
    void insert_interval(std::unique_ptr<IntervalNode>& node, int start, int end, const std::string& query_id);
    void remove_interval(std::unique_ptr<IntervalNode>& node, const std::string& query_id);
    void find_overlapping_intervals(const IntervalNode* node, int key, std::vector<std::string>& result) const;
    void update_max_end(IntervalNode* node);
    void save_interval_tree(std::ofstream& file, const IntervalNode* node) const;
    void load_interval_tree(std::ifstream& file, std::unique_ptr<IntervalNode>& node);

    size_t get_cache_size() const;
    std::vector<std::pair<std::string, std::time_t>> get_queries_by_last_used() const;
};
