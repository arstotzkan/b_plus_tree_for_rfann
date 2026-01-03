#include "bplustree_disk.h"
#include "DataObject.h"
#include <iostream>
#include <random>
#include <ctime>
#include <cstdlib>

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " <index_path> <data_size>" << std::endl;
    std::cout << "  index_path: Path to the B+ tree index file (e.g., data/my_index.bpt)" << std::endl;
    std::cout << "  data_size:  Number of synthetic DataObjects to generate and insert" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        print_usage(argv[0]);
        return 1;
    }

    std::string index_path = argv[1];
    int data_size = std::atoi(argv[2]);

    if (data_size <= 0) {
        std::cerr << "Error: data_size must be a positive integer" << std::endl;
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

    std::cout << std::endl << "=== Index Build Complete ===" << std::endl;
    std::cout << "B+ Tree structure:" << std::endl;
    dataTree.print_tree();

    return 0;
}
