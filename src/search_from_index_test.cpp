#include "bplustree_disk.h"
#include "DataObject.h"
#include "index_directory.h"
#include "query_cache.h"
#include "logger.h"
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cstdlib>
#include <cmath>
#include <algorithm>
#include <sstream>
#include <chrono>
#include <set>
#include <cstdint>
#include <nlohmann/json.hpp>

// Calculate Euclidean distance between two vectors
double calculate_distance(const std::vector<float>& v1, const std::vector<float>& v2) {
    double sum = 0.0;
    size_t min_size = std::min(v1.size(), v2.size());
    for (size_t i = 0; i < min_size; i++) {
        double diff = static_cast<double>(v1[i]) - static_cast<double>(v2[i]);
        sum += diff * diff;
    }
    return std::sqrt(sum);
}

// Parse comma-separated vector string into vector<float>
std::vector<float> parse_vector(const std::string& str) {
    std::vector<float> result;
    std::stringstream ss(str);
    std::string item;
    while (std::getline(ss, item, ',')) {
        result.push_back(static_cast<float>(std::atof(item.c_str())));
    }
    return result;
}

// Load queries from fvecs file
std::vector<std::vector<float>> load_fvecs_queries(const std::string& path) {
    std::vector<std::vector<float>> queries;
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open query file: " << path << std::endl;
        return queries;
    }

    int32_t dimension;
    while (file.read(reinterpret_cast<char*>(&dimension), sizeof(int32_t))) {
        std::vector<float> vec(dimension);
        file.read(reinterpret_cast<char*>(vec.data()), dimension * sizeof(float));
        if (file) {
            queries.push_back(vec);
        }
    }
    return queries;
}

// Load groundtruth from ivecs file
std::vector<std::vector<int32_t>> load_ivecs_groundtruth(const std::string& path) {
    std::vector<std::vector<int32_t>> groundtruth;
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open groundtruth file: " << path << std::endl;
        return groundtruth;
    }

    int32_t count;
    while (file.read(reinterpret_cast<char*>(&count), sizeof(int32_t))) {
        std::vector<int32_t> neighbors(count);
        file.read(reinterpret_cast<char*>(neighbors.data()), count * sizeof(int32_t));
        if (file) {
            groundtruth.push_back(neighbors);
        }
    }
    return groundtruth;
}

// Calculate recall@K
double calculate_recall(const std::vector<int>& retrieved, const std::vector<int32_t>& groundtruth, int k) {
    if (groundtruth.empty() || k <= 0) return 0.0;
    
    std::set<int> gt_set(groundtruth.begin(), groundtruth.begin() + std::min(k, static_cast<int>(groundtruth.size())));
    int hits = 0;
    for (int i = 0; i < std::min(k, static_cast<int>(retrieved.size())); i++) {
        if (gt_set.count(retrieved[i]) > 0) {
            hits++;
        }
    }
    return static_cast<double>(hits) / std::min(k, static_cast<int>(gt_set.size()));
}

// Load sorted labels saved during RFANN index build
std::vector<int> load_sorted_labels(const std::string& index_dir) {
    std::vector<int> labels;
    std::string path = index_dir + "/sorted_labels.bin";
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) return labels;
    int32_t count;
    if (!in.read(reinterpret_cast<char*>(&count), sizeof(int32_t))) return labels;
    labels.resize(count);
    in.read(reinterpret_cast<char*>(labels.data()), count * sizeof(int32_t));
    return labels;
}

// Read qrange JSON: [left0, right0, left1, right1, ...]
std::vector<std::pair<int, int>> read_qrange_json(const std::string& path, int num_queries) {
    std::ifstream in(path);
    if (!in.is_open()) {
        std::cerr << "Error: Cannot open qrange file: " << path << std::endl;
        return {};
    }
    nlohmann::json j;
    in >> j;
    in.close();
    std::vector<int> raw = j.get<std::vector<int>>();
    int limit = num_queries * 2;
    if (static_cast<int>(raw.size()) > limit) raw.resize(limit);
    std::vector<std::pair<int, int>> ranges;
    for (size_t i = 0; i + 1 < raw.size(); i += 2) {
        ranges.push_back({raw[i], raw[i + 1]});
    }
    return ranges;
}

// Binary search: first position where sorted_labels[pos] >= attr_min
int attr_to_pos_lower(const std::vector<int>& sorted_labels, int attr_min) {
    auto it = std::lower_bound(sorted_labels.begin(), sorted_labels.end(), attr_min);
    if (it == sorted_labels.end()) return -1;
    return static_cast<int>(std::distance(sorted_labels.begin(), it));
}

// Binary search: last position where sorted_labels[pos] <= attr_max
int attr_to_pos_upper(const std::vector<int>& sorted_labels, int attr_max) {
    auto it = std::upper_bound(sorted_labels.begin(), sorted_labels.end(), attr_max);
    if (it == sorted_labels.begin()) return -1;
    --it;
    return static_cast<int>(std::distance(sorted_labels.begin(), it));
}

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " --index <index_dir> [options]" << std::endl;
    std::cout << std::endl;
    std::cout << "Flags:" << std::endl;
    std::cout << "  --index, -i      Path to the index directory (required)" << std::endl;
    std::cout << "  --queries, -q    Path to query vectors file (.fvecs format)" << std::endl;
    std::cout << "  --groundtruth    Path to groundtruth file (.ivecs format)" << std::endl;
    std::cout << "  --qrange-path         Path to query range JSON file for RFANN (optional)" << std::endl;
    std::cout << "                   Format: [left0, right0, left1, right1, ...] attribute pairs" << std::endl;
    std::cout << "                   Requires sorted_labels.bin in the index dir (built with --label-path)" << std::endl;
    std::cout << "  --num-queries    Number of queries to run (default: all)" << std::endl;
    std::cout << "  --no-cache       Disable query caching" << std::endl;
    std::cout << "  --parallel       Enable parallel KNN search (auto-detects optimal thread count)" << std::endl;
    std::cout << "  --threads        Number of threads for parallel search (0 = auto, default)" << std::endl;
    std::cout << "  --memory-index   Load entire index into memory before searching (faster for multiple queries)" << std::endl;
    std::cout << "  --vec-sim        Vector similarity threshold for cache matching [0.0-1.0] (default: 1.0 = exact)" << std::endl;
    std::cout << "  --range-sim      Range similarity threshold for cache matching [0.0-1.0] (default: 1.0 = exact)" << std::endl;
    std::cout << std::endl;
    std::cout << "Examples:" << std::endl;
    std::cout << "  Batch RFANN test:" << std::endl;
    std::cout << "    " << program_name << " --index data/sift_index --queries data/dataset/siftsmall_query.fvecs \\" << std::endl;
    std::cout << "      --groundtruth data/dataset/siftsmall_groundtruth.ivecs" << std::endl;
    std::cout << "  Parallel RFANN test:" << std::endl;
    std::cout << "    " << program_name << " --index data/sift_index --queries data/dataset/siftsmall_query.fvecs \\" << std::endl;
    std::cout << "      --groundtruth data/dataset/siftsmall_groundtruth.ivecs --parallel" << std::endl;
}

int main(int argc, char* argv[]) {
    std::string index_dir;
    std::string queries_path;
    std::string groundtruth_path;
    std::string qrange_path;
    int num_queries = -1;
    bool has_index = false;
    bool has_queries = false;
    bool has_groundtruth = false;
    bool has_qrange = false;
    bool cache_enabled = true;
    bool use_parallel = false;
    int num_threads = 0;  // 0 = auto-detect
    bool use_memory_index = false;
    double vec_sim_threshold = 1.0;   // Default: exact match only
    double range_sim_threshold = 1.0; // Default: exact match only

    // Parse command line flags
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if ((arg == "--index" || arg == "-i") && i + 1 < argc) {
            index_dir = argv[++i];
            has_index = true;
        } else if ((arg == "--queries" || arg == "-q") && i + 1 < argc) {
            queries_path = argv[++i];
            has_queries = true;
        } else if (arg == "--groundtruth" && i + 1 < argc) {
            groundtruth_path = argv[++i];
            has_groundtruth = true;
        } else if (arg == "--qrange-path" && i + 1 < argc) {
            qrange_path = argv[++i];
            has_qrange = true;
        } else if (arg == "--num-queries" && i + 1 < argc) {
            num_queries = std::atoi(argv[++i]);
        } else if (arg == "--no-cache") {
            cache_enabled = false;
        } else if (arg == "--parallel") {
            use_parallel = true;
        } else if (arg == "--threads" && i + 1 < argc) {
            num_threads = std::atoi(argv[++i]);
            use_parallel = true;  // Implicitly enable parallel if threads specified
        } else if (arg == "--memory-index") {
            use_memory_index = true;
        } else if (arg == "--vec-sim" && i + 1 < argc) {
            vec_sim_threshold = std::atof(argv[++i]);
            if (vec_sim_threshold < 0.0 || vec_sim_threshold > 1.0) {
                std::cerr << "Error: --vec-sim must be between 0.0 and 1.0" << std::endl;
                return 1;
            }
        } else if (arg == "--range-sim" && i + 1 < argc) {
            range_sim_threshold = std::atof(argv[++i]);
            if (range_sim_threshold < 0.0 || range_sim_threshold > 1.0) {
                std::cerr << "Error: --range-sim must be between 0.0 and 1.0" << std::endl;
                return 1;
            }
        } else if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        }
    }

    if (!has_index) {
        std::cerr << "Error: Missing required --index flag" << std::endl;
        print_usage(argv[0]);
        return 1;
    }

    IndexDirectory idx_dir(index_dir);
    if (!idx_dir.index_exists()) {
        std::cerr << "Error: Index file not found: " << idx_dir.get_index_file_path() << std::endl;
        return 1;
    }

    // Check index cache configuration - this overrides command line --no-cache
    bool index_cache_enabled = idx_dir.read_cache_config();
    if (cache_enabled && !index_cache_enabled) {
        std::cout << "Note: Index was created with --no-cache, disabling cache for this benchmark." << std::endl;
        cache_enabled = false;
    }

    std::cout << "=== RFANN B+ Tree Benchmark ===" << std::endl;
    std::cout << "Index directory: " << index_dir << std::endl;
    std::cout << "Index file: " << idx_dir.get_index_file_path() << std::endl;
    std::cout << "Cache: " << (cache_enabled ? "enabled" : "disabled") << std::endl;
    if (!index_cache_enabled) {
        std::cout << "Cache disabled by index configuration (--no-cache used during build)" << std::endl;
    }

    // Initialize logging
    Logger::init(index_dir, "search_test");
    Logger::set_log_level(LogLevel::DEBUG);

    DiskBPlusTree dataTree(idx_dir.get_index_file_path());
    QueryCache cache(idx_dir.get_base_dir(), cache_enabled);
    
    if (cache_enabled) {
        cache.load_config(idx_dir.get_config_file_path());
    }

    // Load index into memory if requested
    if (use_memory_index) {
        std::cout << "Loading index into memory..." << std::endl;
        auto load_start = std::chrono::high_resolution_clock::now();
        dataTree.loadIntoMemory();
        auto load_end = std::chrono::high_resolution_clock::now();
        auto load_duration = std::chrono::duration_cast<std::chrono::milliseconds>(load_end - load_start);
        std::cout << "Index loaded into memory in " << load_duration.count() << " ms" << std::endl;
    }

    // Log test configuration
    std::ostringstream config_log;
    config_log << "Search test configuration | Cache: " << (cache_enabled ? "enabled" : "disabled")
               << " | Parallel: " << (use_parallel ? "enabled" : "disabled")
               << " | Memory Index: " << (use_memory_index ? "enabled" : "disabled");
    if (use_parallel) config_log << " | Threads: " << num_threads;
    if (has_queries) config_log << " | Query file provided: yes";
    Logger::log_config(config_log.str());

    auto [min_key, max_key] = dataTree.get_key_range();
    if (max_key < min_key) {
        std::cerr << "Error: Tree appears empty" << std::endl;
        return 1;
    }
    std::cout << "Parallel: " << (use_parallel ? "enabled" : "disabled") << std::endl;
    std::cout << "Memory Index: " << (use_memory_index ? "enabled" : "disabled") << std::endl;
    std::cout << "Range filter (from tree): [" << min_key << ", " << max_key << "]" << std::endl;

    // Load per-query ranges if --qrange provided
    std::vector<std::pair<int, int>> query_pos_ranges;
    bool use_per_query_range = false;
    if (has_qrange) {
        std::vector<int> sorted_labels = load_sorted_labels(index_dir);
        if (sorted_labels.empty()) {
            std::cerr << "Error: Cannot load sorted_labels.bin from index directory. Was the index built with --label-path?" << std::endl;
            return 1;
        }
        std::cout << "Loaded " << sorted_labels.size() << " sorted labels" << std::endl;

        // We don't know query count yet, read a large number and trim later
        int qrange_limit = (num_queries > 0) ? num_queries : 1000000;
        std::vector<std::pair<int, int>> attr_ranges = read_qrange_json(qrange_path, qrange_limit);
        std::cout << "Loaded " << attr_ranges.size() << " query ranges from: " << qrange_path << std::endl;

        // Convert attribute ranges to position ranges
        for (auto& r : attr_ranges) {
            int pos_lo = attr_to_pos_lower(sorted_labels, r.first);
            int pos_hi = attr_to_pos_upper(sorted_labels, r.second);
            query_pos_ranges.push_back({pos_lo, pos_hi});
        }
        std::cout << "Converted attribute ranges to position ranges" << std::endl;
        for (size_t ex = 0; ex < std::min(static_cast<size_t>(3), query_pos_ranges.size()); ex++) {
            std::cout << "  Query " << ex << ": attr [" << attr_ranges[ex].first << ", " << attr_ranges[ex].second
                      << "] -> pos [" << query_pos_ranges[ex].first << ", " << query_pos_ranges[ex].second << "]" << std::endl;
        }
        use_per_query_range = true;
    }

    // If no queries file provided, run simple tests
    if (!has_queries) {
        std::cout << std::endl << "No query file provided. Running simple tests..." << std::endl;
        
        // Simple range search test
        int test_min = min_key;
        int test_max = max_key;
        
        auto start = std::chrono::high_resolution_clock::now();
        std::vector<DataObject*> results = dataTree.search_range(test_min, test_max, use_memory_index);
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        
        std::cout << "Range search [" << test_min << ", " << test_max << "]: " << results.size() << " results in " << duration.count() << " us" << std::endl;
        
        for (DataObject* obj : results) {
            delete obj;
        }
        return 0;
    }

    // Load queries
    std::cout << "Loading queries from: " << queries_path << std::endl;
    std::vector<std::vector<float>> queries = load_fvecs_queries(queries_path);
    if (queries.empty()) {
        std::cerr << "Error: No queries loaded" << std::endl;
        return 1;
    }
    std::cout << "Loaded " << queries.size() << " queries (dimension: " << queries[0].size() << ")" << std::endl;

    // Load groundtruth if provided
    std::vector<std::vector<int32_t>> groundtruth;
    int k_neighbors = -1;
    if (has_groundtruth) {
        std::cout << "Loading groundtruth from: " << groundtruth_path << std::endl;
        groundtruth = load_ivecs_groundtruth(groundtruth_path);
        std::cout << "Loaded " << groundtruth.size() << " groundtruth entries" << std::endl;

        if (groundtruth.empty() || groundtruth[0].empty()) {
            std::cerr << "Error: Groundtruth file has no neighbors" << std::endl;
            return 1;
        }
        k_neighbors = static_cast<int>(groundtruth[0].size());
        for (size_t i = 1; i < groundtruth.size(); i++) {
            if (groundtruth[i].size() != groundtruth[0].size()) {
                std::cerr << "Error: Groundtruth file has inconsistent K at row " << i
                          << " (expected " << groundtruth[0].size() << ", got " << groundtruth[i].size() << ")" << std::endl;
                return 1;
            }
        }
    }

    if (has_groundtruth) {
        std::cout << "K (from groundtruth): " << k_neighbors << std::endl;
    }
    std::cout << std::endl;

    // Limit number of queries if specified
    int queries_to_run = (num_queries > 0 && num_queries < static_cast<int>(queries.size())) 
                         ? num_queries : static_cast<int>(queries.size());

    // Run batch queries
    std::cout << "=== Running " << queries_to_run << " RFANN Queries ===" << std::endl;
    if (use_per_query_range) {
        std::cout << "Configuration: K=" << k_neighbors << " | Per-query ranges from qrange file" << std::endl;
    } else {
        std::cout << "Configuration: K=" << k_neighbors << " | Range=[" << min_key << "," << max_key << "]" << std::endl;
    }
    std::cout << "Parallel: " << (use_parallel ? "enabled" : "disabled");
    if (use_parallel) std::cout << " | Threads: " << (num_threads > 0 ? std::to_string(num_threads) : "auto-detect");
    std::cout << std::endl;
    std::cout << "Memory Index: " << (use_memory_index ? "enabled" : "disabled") << std::endl << std::endl;
    
    double total_recall = 0.0;
    long long total_range_time = 0;
    long long total_knn_time = 0;
    int valid_queries = 0;
    int cache_hits = 0;

    for (int q = 0; q < queries_to_run; q++) {
        const std::vector<float>& query_vec = queries[q];
        std::vector<int> retrieved;

        // Determine range for this query
        int q_min = min_key;
        int q_max = max_key;
        if (use_per_query_range && q < static_cast<int>(query_pos_ranges.size())) {
            q_min = query_pos_ranges[q].first;
            q_max = query_pos_ranges[q].second;
            if (q_min < 0 || q_max < 0 || q_min > q_max) {
                // Skip queries with empty position ranges
                if ((q + 1) % 10 == 0 || q == queries_to_run - 1)
                    std::cout << "\rProgress: " << (q + 1) << "/" << queries_to_run << " queries" << std::flush;
                continue;
            }
        }

        std::string query_hash = cache.compute_query_hash(query_vec, q_min, q_max);
        std::string used_similar_query_id;  // Track if we used a similar query's results
        bool cache_hit = false;
        
        if (cache_enabled) {
            SimilarityThresholds thresholds(vec_sim_threshold, range_sim_threshold);
            auto cache_start = std::chrono::high_resolution_clock::now();
            SimilarCacheMatch match = cache.find_similar_cached_result(
                query_vec, q_min, q_max, k_neighbors, thresholds);
            auto cache_end = std::chrono::high_resolution_clock::now();
            long long cache_duration = std::chrono::duration_cast<std::chrono::microseconds>(cache_end - cache_start).count();
            
            if (match.found) {
                cache_hit = true;
                used_similar_query_id = match.query_id;
                total_range_time += cache_duration;
                
                for (const auto& neighbor : match.result.neighbors) {
                    retrieved.push_back(neighbor.key);
                }
                cache_hits++;
                
                // Log cache hit
                std::ostringstream cache_log;
                if (match.vector_similarity >= 1.0 && match.range_similarity >= 1.0) {
                    cache_log << "Query #" << (q + 1) << " | CACHE HIT (exact) | Results: " << match.result.neighbors.size();
                } else {
                    cache_log << "Query #" << (q + 1) << " | CACHE HIT (similar: vec=" << (match.vector_similarity * 100) 
                              << "%, range=" << (match.range_similarity * 100) << "%) | Results: " << match.result.neighbors.size();
                }
                Logger::log_query("KNN_CACHE", cache_log.str(), cache_duration / 1000.0, match.result.neighbors.size());
            }
        }
        
        if (!cache_hit) {
            // Optimized KNN search (parallel or single-threaded)
            auto knn_start = std::chrono::high_resolution_clock::now();
            
            std::ostringstream progress_log;
            progress_log << "Query #" << (q + 1) << " | Starting KNN search (K=" << k_neighbors << ", Range=[" << q_min << "," << q_max << "])";
            Logger::log_query("KNN_PROGRESS", progress_log.str(), 0.0, 0);
            
            std::vector<DataObject*> knn_results;
            if (use_parallel) {
                knn_results = dataTree.search_knn_parallel(query_vec, q_min, q_max, k_neighbors, num_threads, use_memory_index);
            } else {
                knn_results = dataTree.search_knn_optimized(query_vec, q_min, q_max, k_neighbors, use_memory_index);
            }
            auto knn_end = std::chrono::high_resolution_clock::now();
            auto query_duration = std::chrono::duration_cast<std::chrono::microseconds>(knn_end - knn_start).count();
            total_range_time += query_duration;

            // Log individual query performance with detailed breakdown
            std::ostringstream query_params;
            query_params << "Query #" << (q + 1) << " | K=" << k_neighbors << " | Range=[" << q_min << "," << q_max << "] | Results: " << knn_results.size() << " | Time: " << (query_duration / 1000.0) << " ms";
            Logger::log_query("KNN", query_params.str(), query_duration / 1000.0, knn_results.size());

            if (knn_results.empty()) {
                std::ostringstream empty_log;
                empty_log << "Query #" << (q + 1) << " | No results found in range [" << q_min << "," << q_max << "]";
                Logger::log_query("KNN_PROGRESS", empty_log.str(), 0.0, 0);
                continue;
            }

            // Results are already sorted by distance, prepare cache data
            auto cache_prep_start = std::chrono::high_resolution_clock::now();
            std::vector<CachedNeighbor> results_for_cache;
            for (size_t i = 0; i < knn_results.size(); i++) {
                int key = knn_results[i]->is_int_value() ? 
                    knn_results[i]->get_int_value() : 
                    static_cast<int>(knn_results[i]->get_float_value());
                retrieved.push_back(key);
                
                CachedNeighbor neighbor;
                neighbor.vector = knn_results[i]->get_vector();
                neighbor.key = key;
                neighbor.distance = calculate_distance(query_vec, neighbor.vector);
                results_for_cache.push_back(neighbor);
            }
            auto cache_prep_end = std::chrono::high_resolution_clock::now();
            auto cache_prep_duration = std::chrono::duration_cast<std::chrono::microseconds>(cache_prep_end - cache_prep_start).count();

            // Store in cache (pass used_similar_query_id to skip caching if we used similar results)
            auto cache_store_start = std::chrono::high_resolution_clock::now();
            if (cache_enabled && !results_for_cache.empty()) {
                cache.store_result(query_hash, query_vec, q_min, q_max, k_neighbors, results_for_cache, used_similar_query_id);
            }
            auto cache_store_end = std::chrono::high_resolution_clock::now();
            auto cache_store_duration = std::chrono::duration_cast<std::chrono::microseconds>(cache_store_end - cache_store_start).count();

            // Log post-processing times
            std::ostringstream postproc_log;
            postproc_log << "Query #" << (q + 1) << " | Post-processing: prep=" << (cache_prep_duration / 1000.0) << "ms, cache_store=" << (cache_store_duration / 1000.0) << "ms";
            Logger::log_query("KNN_PROGRESS", postproc_log.str(), 0.0, 0);

            // Clean up
            for (DataObject* obj : knn_results) {
                delete obj;
            }
        }

        // Calculate recall if groundtruth available
        if (has_groundtruth && q < static_cast<int>(groundtruth.size())) {
            double recall = calculate_recall(retrieved, groundtruth[q], k_neighbors);
            total_recall += recall;
            valid_queries++;
        }

        // Progress
        if ((q + 1) % 10 == 0 || q == queries_to_run - 1) {
            std::cout << "\rProgress: " << (q + 1) << "/" << queries_to_run << " queries" << std::flush;
        }
    }
    std::cout << std::endl;

    // Print results
    std::cout << std::endl << "=== Benchmark Results ===" << std::endl;
    std::cout << "Total queries: " << queries_to_run << std::endl;
    std::cout << "Cache hits: " << cache_hits << std::endl;
    std::cout << "Tree searches: " << (queries_to_run - cache_hits) << std::endl;
    if (cache_enabled && queries_to_run > 0) {
        double cache_hit_rate = (double)cache_hits / queries_to_run * 100.0;
        std::cout << "Cache hit rate: " << std::fixed << std::setprecision(1) << cache_hit_rate << "%" << std::endl;
    }
    std::cout << "Average range search time: " << (total_range_time / queries_to_run) << " us" << std::endl;
    std::cout << "Average KNN time: " << (total_knn_time / queries_to_run) << " us" << std::endl;
    std::cout << "Average total query time: " << (total_range_time / queries_to_run) << " us" << std::endl;
    std::cout << "Total time: " << (total_range_time / 1000.0) << " ms" << std::endl;
    std::cout << "Queries per second: " << (queries_to_run * 1000000.0 / (total_range_time + total_knn_time)) << std::endl;

    // Log batch performance summary
    std::ostringstream batch_summary;
    batch_summary << "Batch test completed | Total queries: " << queries_to_run
                  << " | Cache hits: " << cache_hits << " (" << (cache_hits * 100.0 / queries_to_run) << "%)"
                  << " | Avg query time: " << (total_range_time / queries_to_run) << " μs"
                  << " | QPS: " << (queries_to_run * 1000000.0 / (total_range_time + total_knn_time));
    Logger::log_performance("BATCH_TEST", total_range_time / 1000.0, batch_summary.str());

    if (has_groundtruth && valid_queries > 0) {
        std::cout << std::endl << "=== Recall ===" << std::endl;
        std::cout << "Recall@" << k_neighbors << ": " << (total_recall / valid_queries * 100.0) << "%" << std::endl;
    }

    return 0;
}
