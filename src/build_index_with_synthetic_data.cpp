#include "bplustree_disk.h"
#include "DataObject.h"
#include <iostream>
#include <random>
#include <ctime>
#include <cstdlib>
#include <string>
#include <chrono>

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " --index <path> --size <count>" << std::endl;
    std::cout << "Flags:" << std::endl;
    std::cout << "  --index, -i  Path to the B+ tree index file to create" << std::endl;
    std::cout << "  --size, -s   Number of synthetic DataObjects to generate and insert" << std::endl;
    std::cout << std::endl;
    std::cout << "Example: " << program_name << " --index data/my_index.bpt --size 1000" << std::endl;
}

int main(int argc, char* argv[]) {
    std::string index_path;
    int data_size = 0;
    bool has_index = false;
    bool has_size = false;

    // Parse command line flags
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if ((arg == "--index" || arg == "-i") && i + 1 < argc) {
            index_path = argv[++i];
            has_index = true;
        } else if ((arg == "--size" || arg == "-s") && i + 1 < argc) {
            data_size = std::atoi(argv[++i]);
            has_size = true;
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

    // Initialize random number generator
    std::mt19937 rng(static_cast<unsigned int>(time(nullptr)));
    std::uniform_int_distribution<int> vector_dist(0, 100);
    std::uniform_int_distribution<int> value_dist(0, 150);

    std::cout << "=== Building B+ Tree Index with Synthetic Data ===" << std::endl;
    std::cout << "Index path: " << index_path << std::endl;
    std::cout << "Data size: " << data_size << std::endl;
    std::cout << std::endl;

    // Start timing
    auto start_time = std::chrono::high_resolution_clock::now();

    DiskBPlusTree dataTree(index_path);

    // Create and insert DataObjects with random values
    for (int i = 1; i <= data_size; i++) {
        int arr[3];
        for (int j = 0; j < 3; j++) {
            arr[j] = vector_dist(rng);
        }
        int value = value_dist(rng);

        DataObject* obj = new DataObject(createDataObject(arr, value));

        std::cout << "Inserting DataObject " << i << " with value " << value << ": [";
        for (int j = 0; j < 3; j++) {
            std::cout << arr[j];
            if (j < 2) std::cout << ", ";
        }
        std::cout << "]" << std::endl;

        try {
            dataTree.insert_data_object(*obj);
        } catch (const std::exception& e) {
            std::cout << "ERROR inserting DataObject " << i << ": " << e.what() << std::endl;
            delete obj;
            break;
        } catch (...) {
            std::cout << "UNKNOWN ERROR inserting DataObject " << i << std::endl;
            delete obj;
            break;
        }

        delete obj;
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
