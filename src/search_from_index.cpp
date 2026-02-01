#include "bplustree_disk.h"
#include "DataObject.h"
#include "index_directory.h"
#include "query_cache.h"
#include "logger.h"
#include <iostream>
#include <string>
#include <vector>
#include <cstdlib>
#include <cmath>
#include <algorithm>
#include <sstream>
#include <chrono>

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

// Parse comma-separated vector string like "1.0,2.0,3.0" into vector<float>
std::vector<float> parse_vector(const std::string& str) {
    std::vector<float> result;
    std::stringstream ss(str);
    std::string item;
    while (std::getline(ss, item, ',')) {
        result.push_back(static_cast<float>(std::atof(item.c_str())));
    }
    return result;
}

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " --index <index_dir> [--min <value> --max <value> | --value <value>] [--vector <v1,v2,...>] [--K <count>]" << std::endl;
    std::cout << std::endl;
    std::cout << "Flags:" << std::endl;
    std::cout << "  --index, -i   Path to the index directory (required)" << std::endl;
    std::cout << "  --min         Minimum value for range search" << std::endl;
    std::cout << "  --max         Maximum value for range search" << std::endl;
    std::cout << "  --value, -v   Search for all objects with a specific value" << std::endl;
    std::cout << "  --vector      Query vector for KNN search (comma-separated, e.g., 1,2,3)" << std::endl;
    std::cout << "  --K, -k       Number of nearest neighbors to return (requires --vector)" << std::endl;
    std::cout << "  --limit       Maximum number of results to return (for memory efficiency)" << std::endl;
    std::cout << "  --no-cache    Disable query caching" << std::endl;
    std::cout << "  --parallel    Enable parallel KNN search (auto-detects optimal thread count)" << std::endl;
    std::cout << "  --threads     Number of threads for parallel search (0 = auto, default)" << std::endl;
    std::cout << "  --memory-index  Load entire index into memory before searching (faster for multiple queries)" << std::endl;
    std::cout << "  --vec-sim     Vector similarity threshold for cache matching [0.0-1.0] (default: 1.0 = exact)" << std::endl;
    std::cout << "  --range-sim   Range similarity threshold for cache matching [0.0-1.0] (default: 1.0 = exact)" << std::endl;
    std::cout << std::endl;
    std::cout << "Note: --value and --min/--max are mutually exclusive" << std::endl;
    std::cout << std::endl;
    std::cout << "Similarity thresholds allow using cached results from similar queries:" << std::endl;
    std::cout << "  --vec-sim 0.95   Accept cached results if query vectors are 95% similar (cosine)" << std::endl;
    std::cout << "  --range-sim 0.8  Accept cached results if ranges overlap by 80% (IoU)" << std::endl;
    std::cout << std::endl;
    std::cout << "Examples:" << std::endl;
    std::cout << "  Range search:      " << program_name << " --index data/my_index --min 20 --max 80" << std::endl;
    std::cout << "  Value search:      " << program_name << " --index data/my_index --value 42" << std::endl;
    std::cout << "  KNN in range:      " << program_name << " --index data/my_index --min 20 --max 80 --vector 10,20,30 --K 5" << std::endl;
    std::cout << "  KNN at value:      " << program_name << " --index data/my_index --value 42 --vector 10,20,30 --K 5" << std::endl;
    std::cout << "  Parallel KNN:      " << program_name << " --index data/my_index --min 0 --max 10000 --vector 1,2,3 --K 10 --parallel" << std::endl;
    std::cout << "  Similar cache:     " << program_name << " --index data/my_index --min 0 --max 100 --vector 1,2,3 --K 10 --vec-sim 0.95 --range-sim 0.8" << std::endl;
}

int main(int argc, char* argv[]) {
    std::string index_dir;
    int min_key = -1;
    int max_key = -1;
    int search_value = -1;
    std::vector<float> query_vector;
    bool use_parallel = false;
    int num_threads = 0;  // 0 = auto-detect
    int k_neighbors = -1;
    int result_limit = -1;  // -1 means no limit
    bool has_index = false;
    bool has_min = false;
    bool has_max = false;
    bool has_value = false;
    bool has_vector = false;
    bool has_k = false;
    bool cache_enabled = true;
    bool use_memory_index = false;
    double vec_sim_threshold = 1.0;   // Default: exact match only
    double range_sim_threshold = 1.0; // Default: exact match only

    // Parse command line flags
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if ((arg == "--index" || arg == "-i") && i + 1 < argc) {
            index_dir = argv[++i];
            has_index = true;
        } else if (arg == "--min" && i + 1 < argc) {
            min_key = std::atoi(argv[++i]);
            has_min = true;
        } else if (arg == "--max" && i + 1 < argc) {
            max_key = std::atoi(argv[++i]);
            has_max = true;
        } else if ((arg == "--value" || arg == "-v") && i + 1 < argc) {
            search_value = std::atoi(argv[++i]);
            has_value = true;
        } else if (arg == "--vector" && i + 1 < argc) {
            query_vector = parse_vector(argv[++i]);
            has_vector = true;
        } else if ((arg == "--K" || arg == "-k") && i + 1 < argc) {
            k_neighbors = std::atoi(argv[++i]);
            has_k = true;
        } else if (arg == "--limit" && i + 1 < argc) {
            result_limit = std::atoi(argv[++i]);
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

    // Must have either --value OR both --min and --max (but not both)
    bool has_range = has_min && has_max;
    if (!has_value && !has_range) {
        std::cerr << "Error: Must specify either --value or both --min and --max" << std::endl;
        print_usage(argv[0]);
        return 1;
    }

    if (has_value && (has_min || has_max)) {
        std::cerr << "Error: Cannot use --value together with --min or --max" << std::endl;
        print_usage(argv[0]);
        return 1;
    }

    if (has_range && min_key > max_key) {
        std::cerr << "Error: min value must be less than or equal to max value" << std::endl;
        return 1;
    }

    if (has_k && !has_vector) {
        std::cerr << "Error: --K requires --vector to be specified" << std::endl;
        print_usage(argv[0]);
        return 1;
    }

    if (has_k && k_neighbors <= 0) {
        std::cerr << "Error: K must be a positive integer" << std::endl;
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
        std::cout << "Note: Index was created with --no-cache, disabling cache for this query." << std::endl;
        cache_enabled = false;
    }

    // Initialize logging
    Logger::init(index_dir, "query");
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

    // Log query configuration
    std::ostringstream config_log;
    config_log << "Query configuration | Cache: " << (cache_enabled ? "enabled" : "disabled")
               << " | Parallel: " << (use_parallel ? "enabled" : "disabled")
               << " | Memory Index: " << (use_memory_index ? "enabled" : "disabled");
    if (use_parallel) config_log << " | Threads: " << num_threads;
    Logger::log_config(config_log.str());

    // Start timing
    auto query_start = std::chrono::high_resolution_clock::now();

    // Get results based on search type
    std::vector<DataObject*> results;
    
    if (has_value) {
        // Value search - get all objects with this value using range search with same min/max
        std::cout << "=== B+ Tree Value Search ===" << std::endl;
        std::cout << "Index directory: " << index_dir << std::endl;
        std::cout << "Cache: " << (cache_enabled ? "enabled" : "disabled") << std::endl;
        std::cout << "Parallel: " << (use_parallel ? "enabled" : "disabled") << std::endl;
        std::cout << "Memory Index: " << (use_memory_index ? "enabled" : "disabled") << std::endl;
        if (use_parallel) std::cout << "Threads: " << (num_threads > 0 ? std::to_string(num_threads) : "auto-detect") << std::endl;
        std::cout << "Search value: " << search_value << std::endl;
        std::cout << "Starting search..." << std::endl;
        
        if (has_vector && has_k) {
            // Use optimized KNN search for value queries with vector
            if (use_parallel) {
                results = dataTree.search_knn_parallel(query_vector, search_value, search_value, k_neighbors, num_threads, use_memory_index);
            } else {
                results = dataTree.search_knn_optimized(query_vector, search_value, search_value, k_neighbors, use_memory_index);
            }
        } else {
            // Regular value search without KNN
            results = dataTree.search_range(search_value, search_value, use_memory_index);
        }
    } else {
        // Range search
        std::cout << "=== B+ Tree Range Search ===" << std::endl;
        std::cout << "Index directory: " << index_dir << std::endl;
        std::cout << "Cache: " << (cache_enabled ? "enabled" : "disabled") << std::endl;
        std::cout << "Parallel: " << (use_parallel ? "enabled" : "disabled") << std::endl;
        std::cout << "Memory Index: " << (use_memory_index ? "enabled" : "disabled") << std::endl;
        if (use_parallel) std::cout << "Threads: " << (num_threads > 0 ? std::to_string(num_threads) : "auto-detect") << std::endl;
        std::cout << "Range: [" << min_key << ", " << max_key << "]" << std::endl;
        std::cout << "Starting search..." << std::endl;
        
        if (has_vector && has_k) {
            // Use optimized KNN search for range queries with vector
            if (use_parallel) {
                results = dataTree.search_knn_parallel(query_vector, min_key, max_key, k_neighbors, num_threads, use_memory_index);
            } else {
                results = dataTree.search_knn_optimized(query_vector, min_key, max_key, k_neighbors, use_memory_index);
            }
        } else {
            // Regular range search without KNN
            results = dataTree.search_range(min_key, max_key, use_memory_index);
        }
    }

    auto range_end = std::chrono::high_resolution_clock::now();
    auto range_duration = std::chrono::duration_cast<std::chrono::microseconds>(range_end - query_start);

    if (has_vector) {
        std::cout << "Query vector: [";
        for (size_t i = 0; i < query_vector.size(); i++) {
            std::cout << query_vector[i];
            if (i < query_vector.size() - 1) std::cout << ", ";
        }
        std::cout << "]" << std::endl;
    }
    if (has_k) {
        std::cout << "K nearest neighbors: " << k_neighbors << std::endl;
    }
    std::cout << std::endl;

    if (has_vector && has_k) {
        // Determine actual min/max for cache key
        int cache_min = has_value ? search_value : min_key;
        int cache_max = has_value ? search_value : max_key;
        
        // Check cache first using similarity-based lookup
        std::string query_hash = cache.compute_query_hash(query_vector, cache_min, cache_max);
        bool cache_hit = false;
        std::string used_similar_query_id;  // Track if we used a similar query's results
        
        if (cache_enabled) {
            SimilarityThresholds thresholds(vec_sim_threshold, range_sim_threshold);
            auto cache_start = std::chrono::high_resolution_clock::now();
            SimilarCacheMatch match = cache.find_similar_cached_result(
                query_vector, cache_min, cache_max, k_neighbors, thresholds);
            auto cache_end = std::chrono::high_resolution_clock::now();
            auto cache_duration = std::chrono::duration_cast<std::chrono::microseconds>(cache_end - cache_start);
            
            if (match.found) {
                cache_hit = true;
                used_similar_query_id = match.query_id;
                
                if (match.vector_similarity >= 1.0 && match.range_similarity >= 1.0) {
                    std::cout << "Cache HIT (exact)! Retrieved " << match.result.neighbors.size() << " cached results:" << std::endl;
                } else {
                    std::cout << "Cache HIT (similar)! Vector similarity: " << (match.vector_similarity * 100) << "%, Range IoU: " << (match.range_similarity * 100) << "%" << std::endl;
                    std::cout << "Retrieved " << match.result.neighbors.size() << " cached results:" << std::endl;
                }
                
                for (size_t i = 0; i < match.result.neighbors.size(); i++) {
                    std::cout << "  #" << (i+1) << " (dist=" << match.result.neighbors[i].distance << "): [";
                    const auto& vec = match.result.neighbors[i].vector;
                    for (size_t j = 0; j < vec.size(); j++) {
                        std::cout << vec[j];
                        if (j < vec.size() - 1) std::cout << ", ";
                    }
                    std::cout << "]  (" << match.result.neighbors[i].key << ")" << std::endl;
                }
                
                std::cout << std::endl << "Query execution time (from cache): " << cache_duration.count() << " us" << std::endl;
                
                // Clean up results that were fetched but not needed
                for (DataObject* obj : results) {
                    delete obj;
                }
            }
        }
        
        if (!cache_hit) {
            // Results are already sorted by distance from optimized KNN search
            std::cout << "Found and sorted " << results.size() << " nearest neighbors:" << std::endl;
            
            std::vector<CachedNeighbor> results_for_cache;
            for (size_t i = 0; i < results.size(); i++) {
                double dist = calculate_distance(query_vector, results[i]->get_vector());
                std::cout << "  #" << (i+1) << " (dist=" << dist << "): ";
                results[i]->print();
                
                CachedNeighbor neighbor;
                neighbor.vector = results[i]->get_vector();
                neighbor.key = results[i]->is_int_value() ? 
                    results[i]->get_int_value() : 
                    static_cast<int>(results[i]->get_float_value());
                neighbor.distance = dist;
                results_for_cache.push_back(neighbor);
            }

            // Store in cache (pass used_similar_query_id to skip caching if we used similar results)
            if (cache_enabled && !results_for_cache.empty()) {
                cache.store_result(query_hash, query_vector, cache_min, cache_max, k_neighbors, results_for_cache, used_similar_query_id);
                if (used_similar_query_id.empty()) {
                    std::cout << std::endl << "Results cached for future queries." << std::endl;
                }
            }

            // Clean up all results
            for (DataObject* obj : results) {
                delete obj;
            }

            auto knn_end = std::chrono::high_resolution_clock::now();
            auto total_duration = std::chrono::duration_cast<std::chrono::microseconds>(knn_end - query_start);
            std::cout << std::endl << "Query execution time:" << std::endl;
            std::cout << "  Optimized KNN search: " << range_duration.count() << " us" << std::endl;
            
            // Log query performance
            std::ostringstream query_params;
            query_params << "KNN search | K=" << k_neighbors << " | Range=[" << cache_min << "," << cache_max << "]";
            Logger::log_query("KNN", query_params.str(), total_duration.count() / 1000.0, results.size());
            std::cout << "  Total: " << total_duration.count() << " us" << std::endl;
        }
    } else {
        // Regular search output
        size_t display_count = results.size();
        if (result_limit > 0 && static_cast<size_t>(result_limit) < display_count) {
            display_count = static_cast<size_t>(result_limit);
        }
        std::cout << "Found " << results.size() << " objects";
        if (display_count < results.size()) {
            std::cout << " (showing first " << display_count << ")";
        }
        std::cout << ":" << std::endl;
        
        for (size_t i = 0; i < display_count; i++) {
            std::cout << "  #" << (i+1) << ": ";
            results[i]->print();
        }
        // Clean up all results
        for (size_t i = 0; i < results.size(); i++) {
            delete results[i];
        }

        std::cout << std::endl << "Query execution time: " << range_duration.count() << " us" << std::endl;
    }

    return 0;
}
