#include "bplustree_disk.h"
#include "DataObject.h"
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cstdint>

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " --input <binary_file> --index <index_path>" << std::endl;
    std::cout << "Flags:" << std::endl;
    std::cout << "  --input, -i   Path to the input binary file" << std::endl;
    std::cout << "  --index, -o   Path to the output B+ tree index file" << std::endl;
    std::cout << std::endl;
    std::cout << "Binary file format:" << std::endl;
    std::cout << "  - First 4 bytes: number of points (int32)" << std::endl;
    std::cout << "  - Next 4 bytes: dimension of data (int32)" << std::endl;
    std::cout << "  - Following n*d*sizeof(float) bytes: data points (float[])" << std::endl;
    std::cout << "  - Data points should be sorted in ascending order by attribute" << std::endl;
    std::cout << std::endl;
    std::cout << "Example: " << program_name << " --input data/vectors.bin --index data/my_index.bpt" << std::endl;
}

int main(int argc, char* argv[]) {
    std::string input_path;
    std::string index_path;
    bool has_input = false;
    bool has_index = false;

    // Parse command line flags
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if ((arg == "--input" || arg == "-i") && i + 1 < argc) {
            input_path = argv[++i];
            has_input = true;
        } else if ((arg == "--index" || arg == "-o") && i + 1 < argc) {
            index_path = argv[++i];
            has_index = true;
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

    // Open binary file
    std::ifstream file(input_path, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Error: Cannot open input file: " << input_path << std::endl;
        return 1;
    }

    // Read header: number of points and dimension
    int32_t num_points, dimension;
    file.read(reinterpret_cast<char*>(&num_points), sizeof(int32_t));
    file.read(reinterpret_cast<char*>(&dimension), sizeof(int32_t));

    if (!file) {
        std::cerr << "Error: Failed to read header from binary file" << std::endl;
        return 1;
    }

    std::cout << "=== Building B+ Tree Index from Binary File ===" << std::endl;
    std::cout << "Input file: " << input_path << std::endl;
    std::cout << "Index path: " << index_path << std::endl;
    std::cout << "Number of points: " << num_points << std::endl;
    std::cout << "Dimension: " << dimension << std::endl;
    std::cout << std::endl;

    DiskBPlusTree dataTree(index_path);

    // Read and insert each data point
    std::vector<float> point_data(dimension);
    
    for (int32_t i = 0; i < num_points; i++) {
        file.read(reinterpret_cast<char*>(point_data.data()), dimension * sizeof(float));
        
        if (!file) {
            std::cerr << "Error: Failed to read data point " << i << std::endl;
            break;
        }

        // Convert float vector to int vector for DataObject
        std::vector<int> int_vector(dimension);
        for (int j = 0; j < dimension; j++) {
            int_vector[j] = static_cast<int>(point_data[j]);
        }

        // Use the first element as the key (since data is sorted by attribute)
        int key = static_cast<int>(point_data[0]);

        DataObject obj(int_vector, key);

        try {
            dataTree.insert_data_object(obj);
        } catch (const std::exception& e) {
            std::cerr << "ERROR inserting point " << i << ": " << e.what() << std::endl;
            break;
        } catch (...) {
            std::cerr << "UNKNOWN ERROR inserting point " << i << std::endl;
            break;
        }

        if ((i + 1) % 1000 == 0) {
            std::cout << "Progress: " << (i + 1) << "/" << num_points << " points inserted" << std::endl;
        }
    }

    file.close();

    std::cout << std::endl << "=== Index Build Complete ===" << std::endl;

    return 0;
}
