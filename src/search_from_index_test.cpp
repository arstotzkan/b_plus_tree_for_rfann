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
#include <thread>
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
        std::cerr << "Error: Cannot open query file: " << path << "\n";
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
        std::cerr << "Error: Cannot open groundtruth file: " << path << "\n";
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

// Read qrange JSON: [left0, right0, left1, right1, ...]
std::vector<std::pair<int, int>> read_qrange_json(const std::string& path, int num_queries) {
    std::ifstream in(path);
    if (!in.is_open()) {
        std::cerr << "Error: Cannot open qrange file: " << path << "\n";
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

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " --index <index_dir> [options]" << "\n";
    std::cout << "\n";
    std::cout << "Flags:" << "\n";
    std::cout << "  --index, -i      Path to the index directory (required)" << "\n";
    std::cout << "  --queries, -q    Path to query vectors file (.fvecs format)" << "\n";
    std::cout << "  --groundtruth    Path to groundtruth file (.ivecs format)" << "\n";
    std::cout << "  --qrange-path         Path to query range JSON file for RFANN (optional)" << "\n";
    std::cout << "                   Format: [left0, right0, left1, right1, ...] attribute pairs" << "\n";
    std::cout << "                   Used directly as B+ tree key ranges (index must be built with --label-path)" << "\n";
    std::cout << "  --num-queries    Number of queries to run (default: all)" << "\n";
    std::cout << "  --no-cache       Disable query caching" << "\n";
    std::cout << "  --parallel       Enable parallel multi-query execution (requires --memory-index)" << "\n";
    std::cout << "  --threads        Number of concurrent queries for --parallel (0 = auto, default)" << "\n";
    std::cout << "  --memory-index   Load entire index into memory before searching (faster for multiple queries)" << "\n";
    std::cout << "  --vec-sim        Vector similarity threshold for cache matching [0.0-1.0] (default: 1.0 = exact)" << "\n";
    std::cout << "  --range-sim      Range similarity threshold for cache matching [0.0-1.0] (default: 1.0 = exact)" << "\n";
    std::cout << "\n";
    std::cout << "Examples:" << "\n";
    std::cout << "  Batch RFANN test:" << "\n";
    std::cout << "    " << program_name << " --index data/sift_index --queries data/dataset/siftsmall_query.fvecs \\" << "\n";
    std::cout << "      --groundtruth data/dataset/siftsmall_groundtruth.ivecs" << "\n";
    std::cout << "  Parallel RFANN test:" << "\n";
    std::cout << "    " << program_name << " --index data/sift_index --queries data/dataset/siftsmall_query.fvecs \\" << "\n";
    std::cout << "      --groundtruth data/dataset/siftsmall_groundtruth.ivecs --parallel" << "\n";
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
                std::cerr << "Error: --vec-sim must be between 0.0 and 1.0" << "\n";
                return 1;
            }
        } else if (arg == "--range-sim" && i + 1 < argc) {
            range_sim_threshold = std::atof(argv[++i]);
            if (range_sim_threshold < 0.0 || range_sim_threshold > 1.0) {
                std::cerr << "Error: --range-sim must be between 0.0 and 1.0" << "\n";
                return 1;
            }
        } else if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        }
    }

    if (!has_index) {
        std::cerr << "Error: Missing required --index flag" << "\n";
        print_usage(argv[0]);
        return 1;
    }

    IndexDirectory idx_dir(index_dir);
    if (!idx_dir.index_exists()) {
        std::cerr << "Error: Index file not found: " << idx_dir.get_index_file_path() << "\n";
        return 1;
    }

    // Check index cache configuration - this overrides command line --no-cache
    bool index_cache_enabled = idx_dir.read_cache_config();
    if (cache_enabled && !index_cache_enabled) {
        std::cout << "Note: Index was created with --no-cache, disabling cache for this benchmark." << "\n";
        cache_enabled = false;
    }

    std::cout << "=== RFANN B+ Tree Benchmark ===" << "\n";
    std::cout << "Index directory: " << index_dir << "\n";
    std::cout << "Index file: " << idx_dir.get_index_file_path() << "\n";
    std::cout << "Cache: " << (cache_enabled ? "enabled" : "disabled") << "\n";
    if (!index_cache_enabled) {
        std::cout << "Cache disabled by index configuration (--no-cache used during build)" << "\n";
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
        std::cout << "Loading index into memory..." << "\n";
        auto load_start = std::chrono::high_resolution_clock::now();
        dataTree.loadIntoMemory();
        auto load_end = std::chrono::high_resolution_clock::now();
        auto load_duration = std::chrono::duration_cast<std::chrono::milliseconds>(load_end - load_start);
        std::cout << "Index loaded into memory in " << load_duration.count() << " ms" << "\n";
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
        std::cerr << "Error: Tree appears empty" << "\n";
        return 1;
    }
    std::cout << "Parallel: " << (use_parallel ? "enabled" : "disabled") << "\n";
    std::cout << "Memory Index: " << (use_memory_index ? "enabled" : "disabled") << "\n";
    std::cout << "Range filter (from tree): [" << min_key << ", " << max_key << "]" << "\n";

    // Load per-query attribute ranges if --qrange provided
    std::vector<std::pair<int, int>> query_ranges;
    bool use_per_query_range = false;
    if (has_qrange) {
        int qrange_limit = (num_queries > 0) ? num_queries : 1000000;
        query_ranges = read_qrange_json(qrange_path, qrange_limit);
        std::cout << "Loaded " << query_ranges.size() << " query ranges from: " << qrange_path << "\n";
        for (size_t ex = 0; ex < std::min(static_cast<size_t>(3), query_ranges.size()); ex++) {
            std::cout << "  Query " << ex << ": range [" << query_ranges[ex].first << ", " << query_ranges[ex].second << "]" << "\n";
        }
        use_per_query_range = true;
    }

    // If no queries file provided, run simple tests
    if (!has_queries) {
        std::cout << "\n" << "No query file provided. Running simple tests..." << "\n";
        
        // Simple range search test
        int test_min = min_key;
        int test_max = max_key;
        
        auto start = std::chrono::high_resolution_clock::now();
        std::vector<DataObject*> results = dataTree.search_range(test_min, test_max, use_memory_index);
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        
        std::cout << "Range search [" << test_min << ", " << test_max << "]: " << results.size() << " results in " << duration.count() << " us" << "\n";
        
        for (DataObject* obj : results) {
            delete obj;
        }
        return 0;
    }

    // Load queries
    std::cout << "Loading queries from: " << queries_path << "\n";
    std::vector<std::vector<float>> queries = load_fvecs_queries(queries_path);
    if (queries.empty()) {
        std::cerr << "Error: No queries loaded" << "\n";
        return 1;
    }
    std::cout << "Loaded " << queries.size() << " queries (dimension: " << queries[0].size() << ")" << "\n";

    // Load groundtruth if provided
    std::vector<std::vector<int32_t>> groundtruth;
    int k_neighbors = -1;
    if (has_groundtruth) {
        std::cout << "Loading groundtruth from: " << groundtruth_path << "\n";
        groundtruth = load_ivecs_groundtruth(groundtruth_path);
        std::cout << "Loaded " << groundtruth.size() << " groundtruth entries" << "\n";

        if (groundtruth.empty()) {
            std::cerr << "Error: Groundtruth file is empty" << "\n";
            return 1;
        }
        // Determine K from first non-empty groundtruth row
        for (size_t i = 0; i < groundtruth.size(); i++) {
            if (!groundtruth[i].empty()) {
                k_neighbors = static_cast<int>(groundtruth[i].size());
                break;
            }
        }
        if (k_neighbors <= 0) {
            std::cerr << "Error: All groundtruth rows are empty, cannot determine K" << "\n";
            return 1;
        }
    }

    if (has_groundtruth) {
        std::cout << "K (from groundtruth): " << k_neighbors << "\n";
    }
    std::cout << "\n";

    // Limit number of queries if specified
    int queries_to_run = (num_queries > 0 && num_queries < static_cast<int>(queries.size())) 
                         ? num_queries : static_cast<int>(queries.size());

    // Run batch queries
    std::cout << "=== Running " << queries_to_run << " RFANN Queries ===" << "\n";
    if (use_per_query_range) {
        std::cout << "Configuration: K=" << k_neighbors << " | Per-query ranges from qrange file" << "\n";
    } else {
        std::cout << "Configuration: K=" << k_neighbors << " | Range=[" << min_key << "," << max_key << "]" << "\n";
    }
    std::cout << "Parallel: " << (use_parallel ? "enabled" : "disabled");
    if (use_parallel) std::cout << " | Threads: " << (num_threads > 0 ? std::to_string(num_threads) : "auto-detect");
    std::cout << "\n";
    std::cout << "Memory Index: " << (use_memory_index ? "enabled" : "disabled") << "\n" << "\n";
    
    double total_recall = 0.0;
    long long total_query_time_sum = 0;  // Sum of individual query durations (for avg latency)
    int valid_queries = 0;
    int cache_hits = 0;

    // Determine parallelism
    int effective_threads = 1;
    if (use_parallel) {
        if (!use_memory_index) {
            std::cerr << "Warning: --parallel requires --memory-index for multi-query parallelism. Falling back to sequential." << "\n";
            use_parallel = false;
        } else {
            effective_threads = (num_threads > 0) ? num_threads : static_cast<int>(std::thread::hardware_concurrency());
            if (effective_threads <= 0) effective_threads = 4;
            std::cout << "Parallel query execution: " << effective_threads << " concurrent queries" << "\n";
        }
    }

    auto wall_start = std::chrono::high_resolution_clock::now();
    for (int batch_start = 0; batch_start < queries_to_run; batch_start += effective_threads) {
        int batch_end = std::min(batch_start + effective_threads, queries_to_run);
        int batch_count = batch_end - batch_start;

        // Per-query state for this batch
        struct QueryState {
            int q_min = 0, q_max = 0;
            bool skipped = false;
            bool cache_hit = false;
            std::string query_hash;
            std::string used_similar_query_id;
            std::vector<int> retrieved;
            std::vector<DataObject*> knn_results;
            long long search_duration_us = 0;
        };
        std::vector<QueryState> states(batch_count);

        // Phase 1: Range setup + cache check (sequential)
        for (int t = 0; t < batch_count; t++) {
            int q = batch_start + t;
            QueryState& st = states[t];
            st.q_min = min_key;
            st.q_max = max_key;
            if (use_per_query_range && q < static_cast<int>(query_ranges.size())) {
                st.q_min = query_ranges[q].first;
                st.q_max = query_ranges[q].second;
                if (st.q_min > st.q_max) {
                    st.skipped = true;
                    continue;
                }
            }

            st.query_hash = cache.compute_query_hash(queries[q], st.q_min, st.q_max);

            if (cache_enabled) {
                SimilarityThresholds thresholds(vec_sim_threshold, range_sim_threshold);
                auto cache_start = std::chrono::high_resolution_clock::now();
                SimilarCacheMatch match = cache.find_similar_cached_result(
                    queries[q], st.q_min, st.q_max, k_neighbors, thresholds);
                auto cache_end = std::chrono::high_resolution_clock::now();
                long long cache_duration = std::chrono::duration_cast<std::chrono::microseconds>(cache_end - cache_start).count();

                if (match.found) {
                    st.cache_hit = true;
                    st.used_similar_query_id = match.query_id;
                    total_query_time_sum += cache_duration;
                    for (const auto& neighbor : match.result.neighbors) {
                        st.retrieved.push_back(static_cast<int>(neighbor.original_id));
                    }
                    cache_hits++;
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
        }

        // Phase 2: Search (parallel or sequential)
        if (use_parallel) {
            std::vector<std::thread> threads;
            for (int t = 0; t < batch_count; t++) {
                if (states[t].skipped || states[t].cache_hit) continue;
                int q = batch_start + t;
                threads.emplace_back([&dataTree, &queries, &states, q, t, k_neighbors, use_memory_index]() {
                    auto start = std::chrono::high_resolution_clock::now();
                    states[t].knn_results = dataTree.search_knn_optimized(
                        queries[q], states[t].q_min, states[t].q_max,
                        k_neighbors, use_memory_index);
                    auto end = std::chrono::high_resolution_clock::now();
                    states[t].search_duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
                });
            }
            for (auto& th : threads) th.join();
        } else {
            for (int t = 0; t < batch_count; t++) {
                if (states[t].skipped || states[t].cache_hit) continue;
                int q = batch_start + t;
                auto start = std::chrono::high_resolution_clock::now();
                states[t].knn_results = dataTree.search_knn_optimized(
                    queries[q], states[t].q_min, states[t].q_max,
                    k_neighbors, use_memory_index);
                auto end = std::chrono::high_resolution_clock::now();
                states[t].search_duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
            }
        }

        // Phase 3: Post-process (sequential)
        for (int t = 0; t < batch_count; t++) {
            int q = batch_start + t;
            QueryState& st = states[t];
            if (st.skipped) continue;

            if (!st.cache_hit) {
                total_query_time_sum += st.search_duration_us;

                std::ostringstream query_params;
                query_params << "Query #" << (q + 1) << " | K=" << k_neighbors
                             << " | Range=[" << st.q_min << "," << st.q_max << "] | Results: "
                             << st.knn_results.size() << " | Time: " << (st.search_duration_us / 1000.0) << " ms";
                Logger::log_query("KNN", query_params.str(), st.search_duration_us / 1000.0, st.knn_results.size());

                if (!st.knn_results.empty()) {
                    std::vector<CachedNeighbor> results_for_cache;
                    for (size_t i = 0; i < st.knn_results.size(); i++) {
                        int key = st.knn_results[i]->is_int_value() ?
                            st.knn_results[i]->get_int_value() :
                            static_cast<int>(st.knn_results[i]->get_float_value());
                        st.retrieved.push_back(static_cast<int>(st.knn_results[i]->get_id()));
                        CachedNeighbor neighbor;
                        neighbor.vector = st.knn_results[i]->get_vector();
                        neighbor.key = key;
                        neighbor.original_id = st.knn_results[i]->get_id();
                        neighbor.distance = calculate_distance(queries[q], neighbor.vector);
                        results_for_cache.push_back(neighbor);
                    }
                    if (cache_enabled) {
                        cache.store_result(st.query_hash, queries[q], st.q_min, st.q_max,
                                           k_neighbors, results_for_cache, st.used_similar_query_id);
                    }
                }
                for (DataObject* obj : st.knn_results) {
                    delete obj;
                }
            }

            // Calculate recall
            if (has_groundtruth && q < static_cast<int>(groundtruth.size())) {
                if (groundtruth[q].empty()) {
                    // Empty groundtruth: correct only if search returned nothing
                    total_recall += st.retrieved.empty() ? 1.0 : 0.0;
                } else {
                    total_recall += calculate_recall(st.retrieved, groundtruth[q], k_neighbors);
                }
                valid_queries++;
            }
        }

        // Progress
        if (batch_end % 10 == 0 || batch_end >= queries_to_run) {
            std::cout << "\rProgress: " << batch_end << "/" << queries_to_run << " queries" << std::flush;
        }
    }
    std::cout << "\n";
    auto wall_end = std::chrono::high_resolution_clock::now();
    double wall_time_ms = std::chrono::duration_cast<std::chrono::microseconds>(wall_end - wall_start).count() / 1000.0;

    // Print results
    std::cout << "\n" << "=== Benchmark Results ===" << "\n";
    std::cout << "Total queries: " << queries_to_run << "\n";
    std::cout << "Cache hits: " << cache_hits << "\n";
    std::cout << "Tree searches: " << (queries_to_run - cache_hits) << "\n";
    if (cache_enabled && queries_to_run > 0) {
        double cache_hit_rate = (double)cache_hits / queries_to_run * 100.0;
        std::cout << "Cache hit rate: " << std::fixed << std::setprecision(1) << cache_hit_rate << "%" << "\n";
    }
    std::cout << "Average query latency: " << (total_query_time_sum / queries_to_run) << " us" << "\n";
    std::cout << "Total wall-clock time: " << wall_time_ms << " ms" << "\n";
    std::cout << "Queries per second: " << (queries_to_run * 1000.0 / wall_time_ms) << "\n";

    // Log batch performance summary
    std::ostringstream batch_summary;
    batch_summary << "Batch test completed | Total queries: " << queries_to_run
                  << " | Cache hits: " << cache_hits << " (" << (cache_hits * 100.0 / queries_to_run) << "%)"
                  << " | Avg latency: " << (total_query_time_sum / queries_to_run) << " μs"
                  << " | Wall time: " << wall_time_ms << " ms"
                  << " | QPS: " << (queries_to_run * 1000.0 / wall_time_ms);
    Logger::log_performance("BATCH_TEST", wall_time_ms, batch_summary.str());

    if (has_groundtruth && valid_queries > 0) {
        std::cout << "\n" << "=== Recall ===" << "\n";
        std::cout << "Recall@" << k_neighbors << ": " << (total_recall / valid_queries * 100.0) << "%" << "\n";
    }

    return 0;
}
