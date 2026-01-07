#include "bplustree_disk.h"
#include "bptree_config.h"
#include "DataObject.h"
#include "index_directory.h"
#include "logger.h"
#include <iostream>
#include <random>
#include <ctime>
#include <cstdlib>
#include <string>
#include <chrono>

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " --index <index_dir> --size <count> [options]" << std::endl;
    std::cout << "Flags:" << std::endl;
    std::cout << "  --index, -i     Path to the index directory (will contain index.bpt and .cache/)" << std::endl;
    std::cout << "  --size, -s      Number of synthetic DataObjects to generate and insert" << std::endl;
    std::cout << "  --dimension, -d Vector dimension (default: 128)" << std::endl;
    std::cout << "  --order         B+ tree order (default: auto-calculated based on vector dimension)" << std::endl;
    std::cout << "  --no-cache      Disable cache creation" << std::endl;
    std::cout << std::endl;
    std::cout << "Examples:" << std::endl;
    std::cout << "  " << program_name << " --index data/my_index --size 1000" << std::endl;
    std::cout << "  " << program_name << " --index data/high_dim_index --size 5000 --dimension 960 --order 2" << std::endl;
}

int main(int argc, char* argv[]) {
    std::string index_dir;
    int data_size = 0;
    int vector_dimension = 128;  // Default dimension
    int custom_order = 0;  // 0 = auto-calculate
    bool has_index = false;
    bool has_size = false;
    bool cache_enabled = true;

    // Parse command line flags
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if ((arg == "--index" || arg == "-i") && i + 1 < argc) {
            index_dir = argv[++i];
            has_index = true;
        } else if ((arg == "--size" || arg == "-s") && i + 1 < argc) {
            data_size = std::atoi(argv[++i]);
            has_size = true;
        } else if ((arg == "--dimension" || arg == "-d") && i + 1 < argc) {
            vector_dimension = std::atoi(argv[++i]);
            if (vector_dimension < 1) vector_dimension = 128;
        } else if (arg == "--order" && i + 1 < argc) {
            custom_order = std::atoi(argv[++i]);
            if (custom_order < 2) custom_order = 2;
        } else if (arg == "--no-cache") {
            cache_enabled = false;
        } else if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        }
    }

    if (!has_index || !has_size) {
        std::cerr << "Error: Missing required flags" << std::endl;
        print_usage(argv[0]);
        return 1;
    }

    if (data_size <= 0) {
        std::cerr << "Error: size must be a positive integer" << std::endl;
        return 1;
    }

    // Setup index directory
    IndexDirectory idx_dir(index_dir);
    if (!idx_dir.ensure_exists()) {
        std::cerr << "Error: Cannot create index directory: " << index_dir << std::endl;
        return 1;
    }
    
    // Save cache configuration
    if (!idx_dir.save_cache_config(cache_enabled)) {
        std::cerr << "Warning: Failed to save cache configuration" << std::endl;
    }

    // Configure B+ tree based on vector dimension
    uint32_t order = (custom_order > 0) ? static_cast<uint32_t>(custom_order) : 4;
    
    // Auto-suggest order if not specified
    if (custom_order == 0) {
        order = BPTreeConfig::suggest_order(static_cast<uint32_t>(vector_dimension), 16384);
        if (order < 2) order = 2;
    }
    
    BPTreeConfig config(order, static_cast<uint32_t>(vector_dimension));

    // Initialize logging
    Logger::init(index_dir, "synthetic_build");
    Logger::set_log_level(LogLevel::INFO);

    // Initialize random number generator
    std::mt19937 rng(static_cast<unsigned int>(time(nullptr)));
    std::uniform_real_distribution<float> vector_dist(0.0f, 100.0f);
    std::uniform_int_distribution<int> value_dist(0, data_size * 2);  // Spread keys across range

    std::cout << "=== Building B+ Tree Index with Synthetic Data ===" << std::endl;
    std::cout << "Index directory: " << index_dir << std::endl;
    std::cout << "Index file: " << idx_dir.get_index_file_path() << std::endl;
    std::cout << "Cache: " << (cache_enabled ? "enabled" : "disabled") << std::endl;
    std::cout << "Data size: " << data_size << std::endl;
    std::cout << std::endl;
    std::cout << "B+ Tree Configuration:" << std::endl;
    std::cout << "  Vector dimension: " << vector_dimension << std::endl;
    std::cout << "  Order: " << config.order << std::endl;
    std::cout << "  Page size: " << config.page_size << " bytes" << std::endl;
    std::cout << "  Node size: " << config.calculate_node_size() << " bytes" << std::endl;
    std::cout << std::endl;

    // Start timing
    auto start_time = std::chrono::high_resolution_clock::now();

    // Create B+ tree with runtime configuration
    DiskBPlusTree dataTree(idx_dir.get_index_file_path(), config);

    // Create and insert DataObjects with random values
    for (int i = 1; i <= data_size; i++) {
        // Generate random vector with specified dimension
        std::vector<float> vec(vector_dimension);
        for (int j = 0; j < vector_dimension; j++) {
            vec[j] = vector_dist(rng);
        }
        int value = value_dist(rng);

        DataObject obj(vec, value);

        // Only print first few for readability
        if (i <= 10 || i % 1000 == 0) {
            std::cout << "Inserting DataObject " << i << " with value " << value;
            if (vector_dimension <= 10) {
                std::cout << ": [";
                for (int j = 0; j < vector_dimension; j++) {
                    std::cout << vec[j];
                    if (j < vector_dimension - 1) std::cout << ", ";
                }
                std::cout << "]";
            }
            std::cout << std::endl;
        }

        try {
            dataTree.insert_data_object(obj);
        } catch (const std::exception& e) {
            std::cout << "ERROR inserting DataObject " << i << ": " << e.what() << std::endl;
            break;
        } catch (...) {
            std::cout << "UNKNOWN ERROR inserting DataObject " << i << std::endl;
            break;
        }
    }

    // End timing
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << std::endl << "=== Index Build Complete ===" << std::endl;
    std::cout << "Total objects inserted: " << data_size << std::endl;
    std::cout << "Build time: " << duration.count() << " ms (" << (duration.count() / 1000.0) << " seconds)" << std::endl;
    std::cout << "B+ Tree structure:" << std::endl;
    dataTree.print_tree();

    return 0;
}
