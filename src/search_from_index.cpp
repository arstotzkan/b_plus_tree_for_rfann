#include "bplustree_disk.h"
#include "DataObject.h"
#include <iostream>
#include <string>
#include <vector>

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " <index_path>" << std::endl;
    std::cout << "  index_path: Path to the B+ tree index file (e.g., data/my_index.bpt)" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        print_usage(argv[0]);
        return 1;
    }

    std::string index_path = argv[1];

    std::cout << "=== B+ Tree Index Search ===" << std::endl;
    std::cout << "Index path: " << index_path << std::endl;
    std::cout << std::endl;

    DiskBPlusTree dataTree(index_path);

    // Search for specific keys
    std::cout << "=== Searching DataObjects ===" << std::endl;

    // Test search with int keys
    int search_keys[] = {42, 3, 100, 999};
    for (int key : search_keys) {
        DataObject* found = dataTree.search_data_object(key);
        if (found) {
            std::cout << "Found DataObject with key " << key << ": ";
            found->print();
            delete found;
        } else {
            std::cout << "DataObject with key " << key << " not found" << std::endl;
        }
    }

    // Test search with float key
    std::cout << std::endl << "=== Testing Float Key Search ===" << std::endl;
    DataObject* found_float = dataTree.search_data_object(3.14f);
    if (found_float) {
        std::cout << "Found with float key 3.14: ";
        found_float->print();
        delete found_float;
    } else {
        std::cout << "Not found with float key 3.14" << std::endl;
    }

    // Test range searches
    std::cout << std::endl << "=== Testing Range Search (20 to 80) ===" << std::endl;
    std::vector<DataObject*> range_results = dataTree.search_range(20, 80);
    std::cout << "Found " << range_results.size() << " objects in range [20, 80]:" << std::endl;
    for (size_t i = 0; i < range_results.size(); i++) {
        std::cout << "  Object " << (i+1) << ": ";
        range_results[i]->print();
        delete range_results[i];
    }

    std::cout << std::endl << "=== Testing Range Search (90 to 105) ===" << std::endl;
    std::vector<DataObject*> range_results2 = dataTree.search_range(90, 105);
    std::cout << "Found " << range_results2.size() << " objects in range [90, 105]:" << std::endl;
    for (size_t i = 0; i < range_results2.size(); i++) {
        std::cout << "  Object " << (i+1) << ": ";
        range_results2[i]->print();
        delete range_results2[i];
    }

    std::cout << std::endl << "=== Testing Range Search (300 to 500) ===" << std::endl;
    std::vector<DataObject*> high_range_results = dataTree.search_range(300, 500);
    std::cout << "Found " << high_range_results.size() << " objects in range [300, 500]" << std::endl;

    // Print tree structure
    std::cout << std::endl << "=== B+ Tree Structure ===" << std::endl;
    dataTree.print_tree();

    return 0;
}
