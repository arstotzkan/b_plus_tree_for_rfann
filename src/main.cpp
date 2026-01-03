#include "bplustree_disk.h"
#include "DataObject.h"
#include <iostream>
#include <random>
#include <ctime>

int main() {
    // Initialize random number generator
    std::mt19937 rng(static_cast<unsigned int>(time(nullptr)));
    std::uniform_int_distribution<int> vector_dist(0, 100);
    std::uniform_int_distribution<int> value_dist(0, 150);
    
    // Test DataObject B+ tree (new functionality)
    std::cout << "=== Testing DataObject B+ Tree ===" << std::endl;
    DiskBPlusTree dataTree("data/test_data_index2.bpt");

    // Create and insert DataObject with random values
    for (int i = 1; i <= 1000; i++) {
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
            std::cout << "Successfully inserted DataObject " << i << std::endl;
        } catch (const std::exception& e) {
            std::cout << "ERROR inserting DataObject " << i << ": " << e.what() << std::endl;
            delete obj;
            break;
        } catch (...) {
            std::cout << "UNKNOWN ERROR inserting DataObject " << i << std::endl;
            delete obj;
            break;
        }
        
        delete obj; // Explicitly delete after insertion to prevent memory leaks
        
        // Add progress indicator every 10 insertions
        if (i % 10 == 0) {
            std::cout << "Progress: " << i << " objects inserted" << std::endl;
        }
    }

    std::cout << std::endl << "=== Searching DataObjects ===" << std::endl;
    
    // Search for DataObjects by their numeric values
    std::vector<int> searchVec1 = {0}; // Dummy vector for search
    DataObject searchObj1(searchVec1, 42);
    DataObject* found1 = dataTree.search_data_object(searchObj1);
    if (found1) {
        std::cout << "Found DataObject with key 42:" << std::endl;
        found1->print();
        delete found1;
    } else {
        std::cout << "DataObject with key 42 not found" << std::endl;
    }

    std::vector<int> searchVec2 = {0}; // Dummy vector for search
    DataObject searchObj2(searchVec2, 3);
    DataObject* found2 = dataTree.search_data_object(searchObj2);
    if (found2) {
        std::cout << "Found DataObject with key 3 (approx from 3.14):" << std::endl;
        found2->print();
        delete found2;
    } else {
        std::cout << "DataObject with key 3 not found" << std::endl;
    }

    std::vector<int> searchVec3 = {0}; // Dummy vector for search
    DataObject searchObj3(searchVec3, 100);
    DataObject* found3 = dataTree.search_data_object(searchObj3);
    if (found3) {
        std::cout << "Found DataObject with key 100:" << std::endl;
        found3->print();
        delete found3;
    } else {
        std::cout << "DataObject with key 100 not found" << std::endl;
    }

    std::vector<int> searchVec4 = {0}; // Dummy vector for search
    DataObject searchObj4(searchVec4, 999);
    DataObject* found4 = dataTree.search_data_object(searchObj4);
    if (found4) {
        std::cout << "Found DataObject with key 999:" << std::endl;
        found4->print();
        delete found4;
    } else {
        std::cout << "DataObject with key 999 not found (as expected)" << std::endl;
    }

    std::cout << std::endl << "=== Testing New Search Methods ===" << std::endl;
    
    // Test search with int key
    DataObject* found_int = dataTree.search_data_object(42);
    if (found_int) {
        std::cout << "Found with int key 42:" << std::endl;
        found_int->print();
        delete found_int;
    } else {
        std::cout << "Not found with int key 42" << std::endl;
    }
    
    // Test search with float key
    DataObject* found_float = dataTree.search_data_object(3.14f);
    if (found_float) {
        std::cout << "Found with float key 3.14:" << std::endl;
        found_float->print();
        delete found_float;
    } else {
        std::cout << "Not found with float key 3.14" << std::endl;
    }
    
    // Test range search
    std::cout << std::endl << "=== Testing Range Search (20 to 80) ===" << std::endl;
    std::vector<DataObject*> range_results = dataTree.search_range(20, 80);
    std::cout << "Found " << range_results.size() << " objects in range [20, 80]:" << std::endl;
    for (size_t i = 0; i < range_results.size(); i++) {
        std::cout << "  Object " << (i+1) << ": ";
        range_results[i]->print();
        delete range_results[i];
    }
    
    std::cout << std::endl << "=== Testing Range Search (90 to 105) ===" << std::endl;
    std::vector<DataObject*> empty_results = dataTree.search_range(90, 105);
    std::cout << "Found " << empty_results.size() << " objects in range [90, 105]" << std::endl;

    for (size_t i = 0; i < empty_results.size(); i++) {
        std::cout << "  Object " << (i+1) << ": ";
        empty_results[i]->print();
        delete empty_results[i];
    }

    // Test range search with no results
    std::cout << std::endl << "=== Testing Range Search (300 to 500) ===" << std::endl;
    std::vector<DataObject*> high_range_results = dataTree.search_range(300, 500);
    std::cout << "Found " << high_range_results.size() << " objects in range [300, 500]" << std::endl;

    std::cout << std::endl << "DataObject B+ Tree structure:" << std::endl;
    dataTree.print_tree();
    
    return 0;
}
