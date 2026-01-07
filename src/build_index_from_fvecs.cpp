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

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " --input <fvecs_file> --index <index_dir> [options]" << std::endl;
    std::cout << std::endl;
    std::cout << "Flags:" << std::endl;
    std::cout << "  --input, -i     Path to the input .fvecs file" << std::endl;
    std::cout << "  --index, -o     Path to the index directory (will contain index.bpt and .cache/)" << std::endl;
    std::cout << "  --order         B+ tree order (default: auto-calculated based on vector dimension)" << std::endl;
    std::cout << "  --batch-size    Number of vectors to process in each batch (default: 10)" << std::endl;
    std::cout << "  --no-cache      Disable cache creation" << std::endl;
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
    std::cout << "  " << program_name << " --input data/gist_base.fvecs --index data/gist_index --order 2" << std::endl;
}

int main(int argc, char* argv[]) {
    std::string input_path;
    std::string index_dir;
    int batch_size = 10;  // Reduced default batch size for memory efficiency
    int custom_order = 0;  // 0 = auto-calculate
    bool has_input = false;
    bool has_index = false;
    bool cache_enabled = true;

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
            if (batch_size <= 0) batch_size = 10;
        } else if (arg == "--no-cache") {
            cache_enabled = false;
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
    // Auto-suggest order if not specified
    if (custom_order == 0) {
        order = BPTreeConfig::suggest_order(static_cast<uint32_t>(dimension), 16384);
        if (order < 2) order = 2;
    }
    
    BPTreeConfig config(order, static_cast<uint32_t>(dimension));
    
    // Initialize logging
    Logger::init(index_dir, "index_build");
    Logger::set_log_level(LogLevel::INFO);
    
    std::cout << "=== Building B+ Tree Index from FVECS File ===" << std::endl;
    std::cout << "Input file: " << input_path << std::endl;
    std::cout << "Index directory: " << index_dir << std::endl;
    std::cout << "Index file: " << idx_dir.get_index_file_path() << std::endl;
    std::cout << "Cache: " << (cache_enabled ? "enabled" : "disabled") << std::endl;
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
    
    // Process vectors one by one for maximum memory efficiency
    int32_t current_dim;
    while (file.read(reinterpret_cast<char*>(&current_dim), sizeof(int32_t))) {
        if (current_dim != dimension) {
            std::cerr << "Warning: Inconsistent dimension at vector " << vector_count 
                      << " (expected " << dimension << ", got " << current_dim << ")" << std::endl;
        }
        
        // Read the vector data directly into a temporary vector
        std::vector<float> vec(current_dim);
        if (!file.read(reinterpret_cast<char*>(vec.data()), current_dim * sizeof(float))) {
            std::cerr << "Error: Failed to read vector " << vector_count << std::endl;
            break;
        }
        
        // Use vector index as the key (for RFANN, this represents the sorted attribute)
        int key = vector_count;
        
        // Insert immediately and clean up
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
        
        // Progress reporting every 1000 vectors
        if (vector_count % 1000 == 0) {
            std::cout << "Progress: " << vector_count << " vectors inserted" << std::endl;
            Logger::info("Progress: " + std::to_string(vector_count) + " vectors inserted");
        }
        
        // Detailed logging every 10000 vectors
        if (vector_count % 10000 == 0) {
            auto current_time = std::chrono::high_resolution_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(current_time - start_time);
            double rate = vector_count / (elapsed.count() / 1000.0);
            Logger::log_performance("Batch insertion", elapsed.count(), 
                std::to_string(vector_count) + " vectors, " + std::to_string(rate) + " vectors/sec");
        }
        
        // Clear vector to free memory immediately
        vec.clear();
        vec.shrink_to_fit();
    }

    file.close();
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
