#include "bplustree_disk.h"
#include "DataObject.h"
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
    std::cout << "Usage: " << program_name << " --index <path> [options]" << std::endl;
    std::cout << std::endl;
    std::cout << "Flags:" << std::endl;
    std::cout << "  --index, -i      Path to the B+ tree index file (required)" << std::endl;
    std::cout << "  --queries, -q    Path to query vectors file (.fvecs format)" << std::endl;
    std::cout << "  --groundtruth    Path to groundtruth file (.ivecs format)" << std::endl;
    std::cout << "  --min            Minimum range for RFANN queries" << std::endl;
    std::cout << "  --max            Maximum range for RFANN queries" << std::endl;
    std::cout << "  --K, -k          Number of nearest neighbors (default: 10)" << std::endl;
    std::cout << "  --num-queries    Number of queries to run (default: all)" << std::endl;
    std::cout << std::endl;
    std::cout << "Examples:" << std::endl;
    std::cout << "  Batch RFANN test:" << std::endl;
    std::cout << "    " << program_name << " --index data/sift_index.bpt --queries data/dataset/siftsmall_query.fvecs \\" << std::endl;
    std::cout << "      --groundtruth data/dataset/siftsmall_groundtruth.ivecs --min 0 --max 1000 --K 10" << std::endl;
}

int main(int argc, char* argv[]) {
    std::string index_path;
    std::string queries_path;
    std::string groundtruth_path;
    int min_key = 0;
    int max_key = -1;
    int k_neighbors = 10;
    int num_queries = -1;
    bool has_index = false;
    bool has_queries = false;
    bool has_groundtruth = false;
    bool has_min = false;
    bool has_max = false;

    // Parse command line flags
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if ((arg == "--index" || arg == "-i") && i + 1 < argc) {
            index_path = argv[++i];
            has_index = true;
        } else if ((arg == "--queries" || arg == "-q") && i + 1 < argc) {
            queries_path = argv[++i];
            has_queries = true;
        } else if (arg == "--groundtruth" && i + 1 < argc) {
            groundtruth_path = argv[++i];
            has_groundtruth = true;
        } else if (arg == "--min" && i + 1 < argc) {
            min_key = std::atoi(argv[++i]);
            has_min = true;
        } else if (arg == "--max" && i + 1 < argc) {
            max_key = std::atoi(argv[++i]);
            has_max = true;
        } else if ((arg == "--K" || arg == "-k") && i + 1 < argc) {
            k_neighbors = std::atoi(argv[++i]);
        } else if (arg == "--num-queries" && i + 1 < argc) {
            num_queries = std::atoi(argv[++i]);
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

    std::cout << "=== RFANN B+ Tree Benchmark ===" << std::endl;
    std::cout << "Index path: " << index_path << std::endl;

    DiskBPlusTree dataTree(index_path);

    // If no queries file provided, run simple tests
    if (!has_queries) {
        std::cout << std::endl << "No query file provided. Running simple tests..." << std::endl;
        
        // Simple range search test
        int test_min = has_min ? min_key : 0;
        int test_max = has_max ? max_key : 100;
        
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
    if (has_groundtruth) {
        std::cout << "Loading groundtruth from: " << groundtruth_path << std::endl;
        groundtruth = load_ivecs_groundtruth(groundtruth_path);
        std::cout << "Loaded " << groundtruth.size() << " groundtruth entries" << std::endl;
    }

    // Set range
    if (!has_max) {
        max_key = 10000;  // Default max range
    }
    std::cout << "Range filter: [" << min_key << ", " << max_key << "]" << std::endl;
    std::cout << "K: " << k_neighbors << std::endl;
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

    for (int q = 0; q < queries_to_run; q++) {
        const std::vector<float>& query_vec = queries[q];

        // Range search
        auto range_start = std::chrono::high_resolution_clock::now();
        std::vector<DataObject*> range_results = dataTree.search_range(min_key, max_key);
        auto range_end = std::chrono::high_resolution_clock::now();
        total_range_time += std::chrono::duration_cast<std::chrono::microseconds>(range_end - range_start).count();

        if (range_results.empty()) {
            continue;
        }

        // KNN within range
        auto knn_start = std::chrono::high_resolution_clock::now();
        std::vector<std::pair<double, int>> distances;
        for (DataObject* obj : range_results) {
            double dist = calculate_distance(query_vec, obj->get_vector());
            int key = obj->is_int_value() ? obj->get_int_value() : static_cast<int>(obj->get_float_value());
            distances.push_back({dist, key});
        }

        std::sort(distances.begin(), distances.end(),
            [](const auto& a, const auto& b) { return a.first < b.first; });

        // Get top-K
        std::vector<int> retrieved;
        for (int i = 0; i < std::min(k_neighbors, static_cast<int>(distances.size())); i++) {
            retrieved.push_back(distances[i].second);
        }
        auto knn_end = std::chrono::high_resolution_clock::now();
        total_knn_time += std::chrono::duration_cast<std::chrono::microseconds>(knn_end - knn_start).count();

        // Calculate recall if groundtruth available
        if (has_groundtruth && q < static_cast<int>(groundtruth.size())) {
            double recall = calculate_recall(retrieved, groundtruth[q], k_neighbors);
            total_recall += recall;
            valid_queries++;
        }

        // Clean up
        for (DataObject* obj : range_results) {
            delete obj;
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
    std::cout << "Average range search time: " << (total_range_time / queries_to_run) << " us" << std::endl;
    std::cout << "Average KNN time: " << (total_knn_time / queries_to_run) << " us" << std::endl;
    std::cout << "Average total query time: " << ((total_range_time + total_knn_time) / queries_to_run) << " us" << std::endl;
    std::cout << "Total time: " << ((total_range_time + total_knn_time) / 1000.0) << " ms" << std::endl;
    std::cout << "Queries per second: " << (queries_to_run * 1000000.0 / (total_range_time + total_knn_time)) << std::endl;

    if (has_groundtruth && valid_queries > 0) {
        std::cout << std::endl << "=== Recall ===" << std::endl;
        std::cout << "Recall@" << k_neighbors << ": " << (total_recall / valid_queries * 100.0) << "%" << std::endl;
    }

    return 0;
}
