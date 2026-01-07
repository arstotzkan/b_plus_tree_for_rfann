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

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " --index <index_dir> [options]" << std::endl;
    std::cout << std::endl;
    std::cout << "Flags:" << std::endl;
    std::cout << "  --index, -i      Path to the index directory (required)" << std::endl;
    std::cout << "  --queries, -q    Path to query vectors file (.fvecs format)" << std::endl;
    std::cout << "  --groundtruth    Path to groundtruth file (.ivecs format)" << std::endl;
    std::cout << "  --num-queries    Number of queries to run (default: all)" << std::endl;
    std::cout << "  --no-cache       Disable query caching" << std::endl;
    std::cout << "  --parallel       Enable parallel KNN search (auto-detects optimal thread count)" << std::endl;
    std::cout << "  --threads        Number of threads for parallel search (0 = auto, default)" << std::endl;
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
    int num_queries = -1;
    bool has_index = false;
    bool has_queries = false;
    bool has_groundtruth = false;
    bool cache_enabled = true;
    bool use_parallel = false;
    int num_threads = 0;  // 0 = auto-detect

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
        } else if (arg == "--num-queries" && i + 1 < argc) {
            num_queries = std::atoi(argv[++i]);
        } else if (arg == "--no-cache") {
            cache_enabled = false;
        } else if (arg == "--parallel") {
            use_parallel = true;
        } else if (arg == "--threads" && i + 1 < argc) {
            num_threads = std::atoi(argv[++i]);
            use_parallel = true;  // Implicitly enable parallel if threads specified
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

    std::cout << "=== RFANN B+ Tree Benchmark ===" << std::endl;
    std::cout << "Index directory: " << index_dir << std::endl;
    std::cout << "Index file: " << idx_dir.get_index_file_path() << std::endl;
    std::cout << "Cache: " << (cache_enabled ? "enabled" : "disabled") << std::endl;

    // Initialize logging
    Logger::init(index_dir, "search_test");
    Logger::set_log_level(LogLevel::DEBUG);

    DiskBPlusTree dataTree(idx_dir.get_index_file_path());
    QueryCache cache(idx_dir.get_base_dir(), cache_enabled);
    
    if (cache_enabled) {
        cache.load_config(idx_dir.get_config_file_path());
    }

    // Log test configuration
    std::ostringstream config_log;
    config_log << "Search test configuration | Cache: " << (cache_enabled ? "enabled" : "disabled")
               << " | Parallel: " << (use_parallel ? "enabled" : "disabled");
    if (use_parallel) config_log << " | Threads: " << num_threads;
    if (has_queries) config_log << " | Query file provided: yes";
    Logger::log_config(config_log.str());

    auto [min_key, max_key] = dataTree.get_key_range();
    if (max_key < min_key) {
        std::cerr << "Error: Tree appears empty" << std::endl;
        return 1;
    }
    std::cout << "Parallel: " << (use_parallel ? "enabled" : "disabled") << std::endl;
    std::cout << "Range filter (from tree): [" << min_key << ", " << max_key << "]" << std::endl;

    // If no queries file provided, run simple tests
    if (!has_queries) {
        std::cout << std::endl << "No query file provided. Running simple tests..." << std::endl;
        
        // Simple range search test
        int test_min = min_key;
        int test_max = max_key;
        
        auto start = std::chrono::high_resolution_clock::now();
        std::vector<DataObject*> results = dataTree.search_range(test_min, test_max);
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
    
    double total_recall = 0.0;
    long long total_range_time = 0;
    long long total_knn_time = 0;
    int valid_queries = 0;
    int cache_hits = 0;

    for (int q = 0; q < queries_to_run; q++) {
        const std::vector<float>& query_vec = queries[q];
        std::vector<int> retrieved;
        
        std::string query_hash = cache.compute_query_hash(query_vec, min_key, max_key, k_neighbors);
        
        if (cache_enabled && cache.has_cached_result(query_hash)) {
            auto cache_start = std::chrono::high_resolution_clock::now();
            CachedQueryResult cached = cache.get_cached_result(query_hash);
            auto cache_end = std::chrono::high_resolution_clock::now();
            total_range_time += std::chrono::duration_cast<std::chrono::microseconds>(cache_end - cache_start).count();
            
            for (const auto& obj : cached.output_objects) {
                retrieved.push_back(obj.second);
            }
            cache_hits++;
        } else {
            // Optimized KNN search (parallel or single-threaded)
            auto knn_start = std::chrono::high_resolution_clock::now();
            std::vector<DataObject*> knn_results;
            if (use_parallel) {
                knn_results = dataTree.search_knn_parallel(query_vec, min_key, max_key, k_neighbors, num_threads);
            } else {
                knn_results = dataTree.search_knn_optimized(query_vec, min_key, max_key, k_neighbors);
            }
            auto knn_end = std::chrono::high_resolution_clock::now();
            auto query_duration = std::chrono::duration_cast<std::chrono::microseconds>(knn_end - knn_start).count();
            total_range_time += query_duration;

            // Log individual query performance
            std::ostringstream query_params;
            query_params << "Query #" << (q + 1) << " | K=" << k_neighbors << " | Range=[" << min_key << "," << max_key << "]";
            Logger::log_query("KNN", query_params.str(), query_duration / 1000.0, knn_results.size());

            if (knn_results.empty()) {
                continue;
            }

            // Results are already sorted by distance, prepare cache data
            std::vector<std::pair<std::vector<float>, int>> results_for_cache;
            for (size_t i = 0; i < knn_results.size(); i++) {
                int key = knn_results[i]->is_int_value() ? 
                    knn_results[i]->get_int_value() : 
                    static_cast<int>(knn_results[i]->get_float_value());
                retrieved.push_back(key);
                results_for_cache.push_back({knn_results[i]->get_vector(), key});
            }

            // Store in cache
            if (cache_enabled && !results_for_cache.empty()) {
                cache.store_result(query_hash, query_vec, min_key, max_key, k_neighbors, results_for_cache);
            }

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
    if (cache_enabled) {
        std::cout << "Cache hits: " << cache_hits << " (" << (cache_hits * 100.0 / queries_to_run) << "%)" << std::endl;
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
