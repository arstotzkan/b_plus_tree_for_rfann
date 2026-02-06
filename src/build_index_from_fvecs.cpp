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
    std::cout << "  --batch-size             Number of vectors to process in each batch (default: 10)" << std::endl;
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
    int batch_size = 10;  // Reduced default batch size for memory efficiency
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
        // RFANN Mode: Load labels, load all vectors, sort by label, insert by sorted position
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
        std::cout << "Loaded " << labels.size() << " labels" << std::endl;

        // Load all vectors into memory
        std::vector<std::vector<float>> all_vectors;
        int32_t current_dim;
        while (file.read(reinterpret_cast<char*>(&current_dim), sizeof(int32_t))) {
            std::vector<float> vec(current_dim);
            if (!file.read(reinterpret_cast<char*>(vec.data()), current_dim * sizeof(float))) break;
            all_vectors.push_back(std::move(vec));
        }
        file.close();
        std::cout << "Loaded " << all_vectors.size() << " vectors into memory" << std::endl;

        if (labels.size() != all_vectors.size()) {
            std::cerr << "Error: Label count (" << labels.size() << ") != vector count (" << all_vectors.size() << ")" << std::endl;
            Logger::close();
            return 1;
        }

        int N = static_cast<int>(all_vectors.size());

        // Sort permutation by label value
        std::vector<int> perm(N);
        std::iota(perm.begin(), perm.end(), 0);
        std::sort(perm.begin(), perm.end(), [&](int a, int b) { return labels[a] < labels[b]; });

        // Build DataObjects in sorted order and bulk load into B+ tree
        // Key = label value (not positional index), so the tree can be searched by attribute range directly
        std::cout << "Building DataObjects in sorted order for bulk load..." << std::endl;
        Logger::info("Building DataObjects in sorted order for bulk load");
        std::vector<DataObject> sorted_objects;
        sorted_objects.reserve(N);
        for (int i = 0; i < N; i++) {
            sorted_objects.emplace_back(std::move(all_vectors[perm[i]]), labels[perm[i]]);
        }
        all_vectors.clear();
        all_vectors.shrink_to_fit();

        std::cout << "Bulk loading " << N << " vectors into B+ tree..." << std::endl;
        Logger::info("Starting bulk load of " + std::to_string(N) + " vectors");
        try {
            dataTree.bulk_load(sorted_objects);
        } catch (const std::exception& e) {
            std::cerr << "ERROR during bulk load: " << e.what() << std::endl;
            Logger::error("ERROR during bulk load: " + std::string(e.what()));
            Logger::close();
            return 1;
        }
        vector_count = N;
    } else {
        // Standard Mode: Process vectors one by one, key = file order position
        int32_t current_dim;
        while (file.read(reinterpret_cast<char*>(&current_dim), sizeof(int32_t))) {
            if (current_dim != dimension) {
                std::cerr << "Warning: Inconsistent dimension at vector " << vector_count 
                          << " (expected " << dimension << ", got " << current_dim << ")" << std::endl;
            }
            
            std::vector<float> vec(current_dim);
            if (!file.read(reinterpret_cast<char*>(vec.data()), current_dim * sizeof(float))) {
                std::cerr << "Error: Failed to read vector " << vector_count << std::endl;
                break;
            }
            
            int key = vector_count;
            
            try {
                DataObject obj(vec, key);
                dataTree.insert_data_object(obj);
            } catch (const std::exception& e) {
                std::string error_msg = "ERROR inserting vector " + std::to_string(key) + ": " + e.what();
                std::cerr << error_msg << std::endl;
                Logger::error(error_msg);
                file.close();
                Logger::close();
                return 1;
            } catch (...) {
                std::string error_msg = "UNKNOWN ERROR inserting vector " + std::to_string(key);
                std::cerr << error_msg << std::endl;
                Logger::error(error_msg);
                file.close();
                Logger::close();
                return 1;
            }
            
            vector_count++;
            
            if (vector_count % 1000 == 0) {
                std::cout << "Progress: " << vector_count << " vectors inserted" << std::endl;
                Logger::info("Progress: " + std::to_string(vector_count) + " vectors inserted");
            }
            
            if (vector_count % 10000 == 0) {
                auto current_time = std::chrono::high_resolution_clock::now();
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(current_time - start_time);
                double rate = vector_count / (elapsed.count() / 1000.0);
                Logger::log_performance("Batch insertion", elapsed.count(), 
                    std::to_string(vector_count) + " vectors, " + std::to_string(rate) + " vectors/sec");
            }
            
            vec.clear();
            vec.shrink_to_fit();
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
