#include "bplustree_disk.h"
#include "bptree_config.h"
#include "DataObject.h"
#include "index_directory.h"
#include "logger.h"
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cstdint>
#include <chrono>
#include <numeric>
#include <algorithm>
#include <nlohmann/json.hpp>

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " --input <fvecs_file> --index <index_dir> [options]" << std::endl;
    std::cout << std::endl;
    std::cout << "Flags:" << std::endl;
    std::cout << "  --input, -i              Path to the input .fvecs file" << std::endl;
    std::cout << "  --index, -o              Path to the index directory (will contain index.bpt and .cache/)" << std::endl;
    std::cout << "  --order                  B+ tree order (default: auto-calculated based on vector dimension)" << std::endl;
    std::cout << "  --batch-size             Number of vectors to read and process per chunk (default: 10000)" << std::endl;
    std::cout << "  --max-cache-size         Maximum cache size in MB (default: 100)" << std::endl;
    std::cout << "  --label-path                  Path to label JSON file for RFANN mode (optional)" << std::endl;
    std::cout << "                           Format: [42, 17, 99, ...] one integer per vector" << std::endl;
    std::cout << "                           When set, vectors are sorted by attribute and the label" << std::endl;
    std::cout << "                           value is used as the B+ tree key for direct range search." << std::endl;
    std::cout << std::endl;
    std::cout << "B+ Tree Configuration:" << std::endl;
    std::cout << "  The index automatically detects vector dimension from the input file and" << std::endl;
    std::cout << "  calculates optimal page size. Use --order to override the default order." << std::endl;
    std::cout << std::endl;
    std::cout << "FVECS file format:" << std::endl;
    std::cout << "  Each vector: 4 bytes (dimension d as int32) + d*4 bytes (floats)" << std::endl;
    std::cout << std::endl;
    std::cout << "Examples:" << std::endl;
    std::cout << "  " << program_name << " --input data/siftsmall_base.fvecs --index data/sift_index" << std::endl;
    std::cout << std::endl;
}

int main(int argc, char* argv[]) {
    std::string input_path;
    std::string index_dir;
    int batch_size = 10000;
    int custom_order = 0;  // 0 = auto-calculate
    std::string label_path;
    bool has_input = false;
    bool has_index = false;
    bool has_label = false;
    // Model B: Vectors are always stored separately (no inline storage option)
    size_t max_cache_size_mb = 100;

    // Parse command line flags
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if ((arg == "--input" || arg == "-i") && i + 1 < argc) {
            input_path = argv[++i];
            has_input = true;
        } else if ((arg == "--index" || arg == "-o") && i + 1 < argc) {
            index_dir = argv[++i];
            has_index = true;
        } else if (arg == "--order" && i + 1 < argc) {
            custom_order = std::atoi(argv[++i]);
            if (custom_order < 2) custom_order = 2;
        } else if (arg == "--batch-size" && i + 1 < argc) {
            batch_size = std::atoi(argv[++i]);
            if (batch_size <= 0) batch_size = 10000;
        } else if (arg == "--max-cache-size" && i + 1 < argc) {
            max_cache_size_mb = std::stoull(argv[++i]);
            if (max_cache_size_mb == 0) max_cache_size_mb = 100;
        } else if (arg == "--label-path" && i + 1 < argc) {
            label_path = argv[++i];
            has_label = true;
        } else if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        }
    }

    if (!has_input || !has_index) {
        std::cerr << "Error: Missing required flags" << std::endl;
        print_usage(argv[0]);
        return 1;
    }

    // Setup index directory
    IndexDirectory idx_dir(index_dir);
    if (!idx_dir.ensure_exists()) {
        std::cerr << "Error: Cannot create index directory: " << index_dir << std::endl;
        return 1;
    }
    
    // Save cache configuration
    if (!idx_dir.save_cache_config(true, max_cache_size_mb)) {
        std::cerr << "Warning: Failed to save cache configuration" << std::endl;
    }

    // Open fvecs file to read dimension first
    std::ifstream file(input_path, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open input file: " << input_path << std::endl;
        return 1;
    }

    // Read first vector's dimension to configure B+ tree
    int32_t dimension;
    if (!file.read(reinterpret_cast<char*>(&dimension), sizeof(int32_t))) {
        std::cerr << "Error: Cannot read dimension from input file" << std::endl;
        return 1;
    }
    
    // Reset file to beginning
    file.seekg(0);

    // Configure B+ tree based on vector dimension
    uint32_t order = (custom_order > 0) ? static_cast<uint32_t>(custom_order) : 4;
    
    // For large vectors, we may need to reduce order to fit in reasonable page sizes
    // Auto-suggest order if not specified (Model B: vectors always stored separately)
    if (custom_order == 0) {
        order = BPTreeConfig::suggest_order(static_cast<uint32_t>(dimension), 16384);
        if (order < 2) order = 2;
    }
    
    BPTreeConfig config(order, static_cast<uint32_t>(dimension));
    config.page_size = config.calculate_min_page_size();
    
    // Initialize logging
    Logger::init(index_dir, "index_build");
    Logger::set_log_level(LogLevel::INFO);
    
    std::cout << "=== Building B+ Tree Index from FVECS File ===" << std::endl;
    std::cout << "Input file: " << input_path << std::endl;
    std::cout << "Index directory: " << index_dir << std::endl;
    std::cout << "Index file: " << idx_dir.get_index_file_path() << std::endl;
    std::cout << "Cache: enabled (max " << max_cache_size_mb << " MB)" << std::endl;
    std::cout << "Batch size: " << batch_size << " vectors" << std::endl;
    std::cout << std::endl;
    std::cout << "B+ Tree Configuration:" << std::endl;
    std::cout << "  Vector dimension: " << dimension << std::endl;
    std::cout << "  Order: " << config.order << std::endl;
    std::cout << "  Page size: " << config.page_size << " bytes" << std::endl;
    std::cout << "  Node size: " << config.calculate_node_size() << " bytes" << std::endl;
    std::cout << std::endl;

    // Log configuration details
    std::ostringstream config_log;
    config_log << "Building index from " << input_path << " | Vector dimension: " << dimension 
               << " | Order: " << config.order << " | Page size: " << config.page_size 
               << " | Node size: " << config.calculate_node_size() << " bytes";
    Logger::log_config(config_log.str());

    // Start timing
    auto start_time = std::chrono::high_resolution_clock::now();
    Logger::info("Starting index build process");

    // Create B+ tree with runtime configuration
    DiskBPlusTree dataTree(idx_dir.get_index_file_path(), config);

    int vector_count = 0;

    if (has_label) {
        // RFANN Mode: Load labels, read vectors in chunks, sort each chunk by label, insert
        std::cout << "RFANN Mode: Sorting vectors by label from " << label_path << std::endl;
        Logger::info("RFANN Mode: reading labels from " + label_path);

        // Read label JSON: [42, 17, 99, ...]
        std::vector<int> labels;
        {
            std::ifstream label_file(label_path);
            if (!label_file.is_open()) {
                std::cerr << "Error: Cannot open label file: " << label_path << std::endl;
                file.close();
                Logger::close();
                return 1;
            }
            nlohmann::json j;
            label_file >> j;
            label_file.close();
            labels = j.get<std::vector<int>>();
        }
        size_t total_labels = labels.size();
        std::cout << "Loaded " << total_labels << " labels" << std::endl;

        // Process vectors in chunks of batch_size
        size_t global_idx = 0;
        int chunk_num = 0;

        while (global_idx < total_labels) {
            size_t chunk_end = std::min(global_idx + static_cast<size_t>(batch_size), total_labels);
            size_t chunk_size = chunk_end - global_idx;
            chunk_num++;

            // Read chunk_size vectors from file
            std::vector<DataObject> objects;
            objects.reserve(chunk_size);

            for (size_t i = 0; i < chunk_size; i++) {
                int32_t current_dim;
                if (!file.read(reinterpret_cast<char*>(&current_dim), sizeof(int32_t))) {
                    std::cerr << "Error: Unexpected end of fvecs file at vector " << global_idx << std::endl;
                    file.close();
                    Logger::close();
                    return 1;
                }
                std::vector<float> vec(current_dim);
                if (!file.read(reinterpret_cast<char*>(vec.data()), current_dim * sizeof(float))) {
                    std::cerr << "Error: Failed to read vector " << global_idx << std::endl;
                    file.close();
                    Logger::close();
                    return 1;
                }
                objects.emplace_back(std::move(vec), labels[global_idx]);
                objects.back().set_id(static_cast<int32_t>(global_idx));
                global_idx++;
            }

            // Permutation-based sort: sort lightweight indices by label, then reorder DataObjects
            int N = static_cast<int>(objects.size());

            std::vector<int> keys(N);
            for (int i = 0; i < N; i++) {
                keys[i] = objects[i].is_int_value() ? objects[i].get_int_value()
                                                    : static_cast<int>(objects[i].get_float_value());
            }

            std::vector<int> perm(N);
            std::iota(perm.begin(), perm.end(), 0);
            std::sort(perm.begin(), perm.end(), [&keys](int a, int b) { return keys[a] < keys[b]; });

            // Apply permutation in-place (each DataObject moved at most once)
            std::vector<bool> placed(N, false);
            for (int i = 0; i < N; i++) {
                if (placed[i] || perm[i] == i) continue;
                DataObject temp = std::move(objects[i]);
                int j = i;
                while (perm[j] != i) {
                    objects[j] = std::move(objects[perm[j]]);
                    placed[j] = true;
                    j = perm[j];
                }
                objects[j] = std::move(temp);
                placed[j] = true;
            }

            // Insert sorted chunk into B+ tree
            for (int i = 0; i < N; i++) {
                try {
                    dataTree.insert_data_object(objects[i]);
                } catch (const std::exception& e) {
                    std::cerr << "ERROR inserting vector: " << e.what() << std::endl;
                    Logger::error("ERROR inserting vector: " + std::string(e.what()));
                    file.close();
                    Logger::close();
                    return 1;
                }
            }
            vector_count += N;

            auto current_time = std::chrono::high_resolution_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(current_time - start_time);
            std::cout << "Chunk " << chunk_num << ": inserted " << N
                      << " vectors (total: " << vector_count << ", " << elapsed.count() << " ms)" << std::endl;
            Logger::info("Chunk " + std::to_string(chunk_num) + ": inserted " + std::to_string(N) +
                         " vectors (total: " + std::to_string(vector_count) + ")");
        }
        file.close();
    } else {
        // Standard Mode: Read vectors in chunks, key = file order position (no sort needed)
        int chunk_num = 0;

        while (true) {
            std::vector<DataObject> objects;
            objects.reserve(batch_size);

            for (int i = 0; i < batch_size; i++) {
                int32_t current_dim;
                if (!file.read(reinterpret_cast<char*>(&current_dim), sizeof(int32_t))) {
                    break; // EOF
                }
                if (current_dim != dimension) {
                    std::cerr << "Warning: Inconsistent dimension at vector " << vector_count
                              << " (expected " << dimension << ", got " << current_dim << ")" << std::endl;
                }
                std::vector<float> vec(current_dim);
                if (!file.read(reinterpret_cast<char*>(vec.data()), current_dim * sizeof(float))) {
                    std::cerr << "Error: Failed to read vector " << vector_count << std::endl;
                    break;
                }
                objects.emplace_back(std::move(vec), vector_count);
                objects.back().set_id(static_cast<int32_t>(vector_count));
                vector_count++;
            }

            if (objects.empty()) break;
            chunk_num++;

            // Keys are sequential — no sort needed
            for (size_t i = 0; i < objects.size(); i++) {
                try {
                    dataTree.insert_data_object(objects[i]);
                } catch (const std::exception& e) {
                    std::cerr << "ERROR inserting vector " << objects[i].get_int_value() << ": " << e.what() << std::endl;
                    Logger::error("ERROR inserting vector: " + std::string(e.what()));
                    file.close();
                    Logger::close();
                    return 1;
                }
            }

            auto current_time = std::chrono::high_resolution_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(current_time - start_time);
            std::cout << "Chunk " << chunk_num << ": inserted " << objects.size()
                      << " vectors (total: " << vector_count << ", " << elapsed.count() << " ms)" << std::endl;
            Logger::info("Chunk " + std::to_string(chunk_num) + ": inserted " + std::to_string(objects.size()) +
                         " vectors (total: " + std::to_string(vector_count) + ")");
        }
        file.close();
    }
    Logger::info("Finished reading input file");

    // End timing
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << std::endl << "=== Index Build Complete ===" << std::endl;
    std::cout << "Total vectors inserted: " << vector_count << std::endl;
    std::cout << "Vector dimension: " << dimension << std::endl;
    std::cout << "Build time: " << duration.count() << " ms (" << (duration.count() / 1000.0) << " seconds)" << std::endl;
    
    // Log final summary
    double rate = vector_count / (duration.count() / 1000.0);
    std::ostringstream summary;
    summary << "Index build completed successfully | Total vectors: " << vector_count 
            << " | Dimension: " << dimension << " | Duration: " << duration.count() << " ms"
            << " | Rate: " << std::fixed << std::setprecision(2) << rate << " vectors/sec";
    Logger::info(summary.str());
    Logger::log_performance("Complete index build", duration.count(), 
        std::to_string(vector_count) + " vectors total");
    
    if (vector_count > 0) {
        std::cout << "Average insertion rate: " << (vector_count * 1000.0 / duration.count()) << " vectors/second" << std::endl;
    }

    Logger::close();
    return 0;
}
