#include "bplustree_disk.h"
#include "DataObject.h"
#include "index_directory.h"
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cstdint>
#include <chrono>
#include <cstring>

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " --input <npy_file> --index <index_dir> [options]" << std::endl;
    std::cout << std::endl;
    std::cout << "Flags:" << std::endl;
    std::cout << "  --input, -i     Path to the input .npy file" << std::endl;
    std::cout << "  --index, -o     Path to the index directory (will contain index.bpt and .cache/)" << std::endl;
    std::cout << "  --batch-size    Number of vectors to process in each batch (default: 100)" << std::endl;
    std::cout << "  --no-cache      Disable cache creation" << std::endl;
    std::cout << std::endl;
    std::cout << "NPY file format:" << std::endl;
    std::cout << "  NumPy array format with float32 vectors" << std::endl;
    std::cout << std::endl;
    std::cout << "Example: " << program_name << " --input data/vectors.npy --index data/npy_index" << std::endl;
}

// Simple NPY header parser
bool parse_npy_header(std::ifstream& file, int& num_vectors, int& dimension) {
    char magic[6];
    file.read(magic, 6);
    if (magic[0] != '\x93' || magic[1] != 'N' || magic[2] != 'U' || 
        magic[3] != 'M' || magic[4] != 'P' || magic[5] != 'Y') {
        std::cerr << "Error: Invalid NPY magic number" << std::endl;
        return false;
    }

    uint8_t major, minor;
    file.read(reinterpret_cast<char*>(&major), 1);
    file.read(reinterpret_cast<char*>(&minor), 1);

    uint16_t header_len;
    if (major == 1) {
        file.read(reinterpret_cast<char*>(&header_len), 2);
    } else {
        uint32_t header_len32;
        file.read(reinterpret_cast<char*>(&header_len32), 4);
        header_len = static_cast<uint16_t>(header_len32);
    }

    std::string header(header_len, '\0');
    file.read(&header[0], header_len);

    // Parse shape from header (simple parsing)
    size_t shape_pos = header.find("'shape':");
    if (shape_pos == std::string::npos) {
        shape_pos = header.find("\"shape\":");
    }
    if (shape_pos == std::string::npos) {
        std::cerr << "Error: Cannot find shape in NPY header" << std::endl;
        return false;
    }

    size_t paren_start = header.find('(', shape_pos);
    size_t paren_end = header.find(')', paren_start);
    std::string shape_str = header.substr(paren_start + 1, paren_end - paren_start - 1);

    // Parse two integers from shape
    size_t comma_pos = shape_str.find(',');
    if (comma_pos == std::string::npos) {
        std::cerr << "Error: Expected 2D array in NPY file" << std::endl;
        return false;
    }

    num_vectors = std::stoi(shape_str.substr(0, comma_pos));
    std::string dim_str = shape_str.substr(comma_pos + 1);
    // Remove trailing comma if present
    size_t trailing_comma = dim_str.find(',');
    if (trailing_comma != std::string::npos) {
        dim_str = dim_str.substr(0, trailing_comma);
    }
    dimension = std::stoi(dim_str);

    return true;
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
        } else if (arg == "--no-cache") {
            cache_enabled = false;
        } else if (arg == "--batch-size" && i + 1 < argc) {
            batch_size = std::atoi(argv[++i]);
            if (batch_size <= 0) batch_size = 100;
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

    // Open npy file
    std::ifstream file(input_path, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open input file: " << input_path << std::endl;
        return 1;
    }

    int num_vectors, dimension;
    if (!parse_npy_header(file, num_vectors, dimension)) {
        return 1;
    }

    std::cout << "=== Building B+ Tree Index from NPY File ===" << std::endl;
    std::cout << "Input file: " << input_path << std::endl;
    std::cout << "Index directory: " << index_dir << std::endl;
    std::cout << "Index file: " << idx_dir.get_index_file_path() << std::endl;
    std::cout << "Cache: " << (cache_enabled ? "enabled" : "disabled") << std::endl;
    std::cout << "Number of vectors: " << num_vectors << std::endl;
    std::cout << "Dimension: " << dimension << std::endl;
    std::cout << "Batch size: " << batch_size << " vectors" << std::endl;
    std::cout << std::endl;

    // Start timing
    auto start_time = std::chrono::high_resolution_clock::now();

    DiskBPlusTree dataTree(idx_dir.get_index_file_path());

    int vector_count = 0;
    int batch_count = 0;
    
    // Process vectors in batches
    while (vector_count < num_vectors) {
        std::vector<std::pair<std::vector<float>, int>> batch;
        int current_batch_size = std::min(batch_size, num_vectors - vector_count);
        batch.reserve(current_batch_size);
        
        // Read a batch of vectors
        for (int i = 0; i < current_batch_size; i++) {
            std::vector<float> vec(dimension);
            if (!file.read(reinterpret_cast<char*>(vec.data()), dimension * sizeof(float))) {
                std::cerr << "Error: Failed to read vector " << vector_count << std::endl;
                break;
            }
            
            // Use vector index as the key
            int key = vector_count;
            batch.emplace_back(std::move(vec), key);
            vector_count++;
        }
        
        if (batch.empty()) {
            break; // No more vectors to process
        }
        
        // Insert batch into B+ tree
        for (auto& [vec, key] : batch) {
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
        }
        
        batch_count++;
        std::cout << "Progress: Batch " << batch_count << " complete (" << vector_count << "/" << num_vectors << " vectors total)" << std::endl;
        
        // Clear batch to free memory
        batch.clear();
        batch.shrink_to_fit();
    }

    file.close();

    // End timing
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << std::endl << "=== Index Build Complete ===" << std::endl;
    std::cout << "Total vectors inserted: " << vector_count << std::endl;
    std::cout << "Batches processed: " << batch_count << std::endl;
    std::cout << "Build time: " << duration.count() << " ms (" << (duration.count() / 1000.0) << " seconds)" << std::endl;
    
    if (vector_count > 0) {
        std::cout << "Average insertion rate: " << (vector_count * 1000.0 / duration.count()) << " vectors/second" << std::endl;
    }

    return 0;
}
