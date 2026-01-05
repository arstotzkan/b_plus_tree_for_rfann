#include "bplustree_disk.h"
#include "DataObject.h"
#include "index_directory.h"
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
    std::cout << "  --batch-size    Number of vectors to process in each batch (default: 100)" << std::endl;
    std::cout << "  --no-cache      Disable cache creation" << std::endl;
    std::cout << std::endl;
    std::cout << "FVECS file format:" << std::endl;
    std::cout << "  Each vector: 4 bytes (dimension d as int32) + d*4 bytes (floats)" << std::endl;
    std::cout << std::endl;
    std::cout << "Example: " << program_name << " --input data/siftsmall_base.fvecs --index data/sift_index" << std::endl;
}

int main(int argc, char* argv[]) {
    std::string input_path;
    std::string index_dir;
    int batch_size = 10;  // Reduced default batch size for memory efficiency
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
        } else if (arg == "--batch-size" && i + 1 < argc) {
            batch_size = std::atoi(argv[++i]);
            if (batch_size <= 0) batch_size = 100;
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

    // Open fvecs file
    std::ifstream file(input_path, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open input file: " << input_path << std::endl;
        return 1;
    }

    std::cout << "=== Building B+ Tree Index from FVECS File ===" << std::endl;
    std::cout << "Input file: " << input_path << std::endl;
    std::cout << "Index directory: " << index_dir << std::endl;
    std::cout << "Index file: " << idx_dir.get_index_file_path() << std::endl;
    std::cout << "Cache: " << (cache_enabled ? "enabled" : "disabled") << std::endl;
    std::cout << "Batch size: " << batch_size << " vectors" << std::endl;
    std::cout << std::endl;

    // Start timing
    auto start_time = std::chrono::high_resolution_clock::now();

    DiskBPlusTree dataTree(idx_dir.get_index_file_path());

    int32_t dimension = -1;
    int vector_count = 0;
    int batch_count = 0;
    
    // Process vectors one by one for maximum memory efficiency
    int32_t current_dim;
    while (file.read(reinterpret_cast<char*>(&current_dim), sizeof(int32_t))) {
        if (dimension == -1) {
            dimension = current_dim;
            std::cout << "Vector dimension: " << dimension << std::endl;
        } else if (current_dim != dimension) {
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
            std::cerr << "ERROR inserting vector " << key << ": " << e.what() << std::endl;
            file.close();
            return 1;
        } catch (...) {
            std::cerr << "UNKNOWN ERROR inserting vector " << key << std::endl;
            file.close();
            return 1;
        }
        
        vector_count++;
        
        // Progress reporting every 100 vectors
        if (vector_count % 100 == 0) {
            std::cout << "Progress: " << vector_count << " vectors inserted" << std::endl;
        }
        
        // Clear vector to free memory immediately
        vec.clear();
        vec.shrink_to_fit();
    }

    file.close();

    // End timing
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << std::endl << "=== Index Build Complete ===" << std::endl;
    std::cout << "Total vectors inserted: " << vector_count << std::endl;
    std::cout << "Vector dimension: " << dimension << std::endl;
    std::cout << "Build time: " << duration.count() << " ms (" << (duration.count() / 1000.0) << " seconds)" << std::endl;
    
    if (vector_count > 0) {
        std::cout << "Average insertion rate: " << (vector_count * 1000.0 / duration.count()) << " vectors/second" << std::endl;
    }

    return 0;
}
