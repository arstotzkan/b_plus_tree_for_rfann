#include "bplustree_disk.h"
#include "DataObject.h"
#include <iostream>

int main() {
    // Test DataObject B+ tree (new functionality)
    std::cout << "=== Testing DataObject B+ Tree ===" << std::endl;
    DiskBPlusTree dataTree("data/test_data_index2.bpt");

    // Create and insert DataObject with int values
    DataObject obj1(3, 42);
    obj1.set_vector_element(0, 10);
    obj1.set_vector_element(1, 20);
    obj1.set_vector_element(2, 30);
    
    std::cout << "Inserting DataObject 1:" << std::endl;
    obj1.print();
    std::cout << "About to call insert_data_object..." << std::endl;
    dataTree.insert_data_object(obj1);
    std::cout << "Successfully inserted DataObject 1" << std::endl;

    // Create and insert DataObject with float value
    DataObject obj2(3, 3.14f);
    obj2.set_vector_element(0, 100);
    obj2.set_vector_element(1, 300);
    obj2.set_vector_element(2, 300);
    
    std::cout << "Inserting DataObject 2:" << std::endl;
    obj2.print();
    dataTree.insert_data_object(obj2);

    // Create and insert more DataObjects
    DataObject obj3(3, 100);
    obj3.set_vector_element(0, 1);
    obj3.set_vector_element(1, 2);
    obj3.set_vector_element(2, 3);
    
    std::cout << "Inserting DataObject 3:" << std::endl;
    obj3.print();
    dataTree.insert_data_object(obj3);

    DataObject obj4(3, 100);
    obj4.set_vector_element(0, 99);
    obj4.set_vector_element(1, 88);
    obj4.set_vector_element(2, 77);
    
    std::cout << "Inserting DataObject 4:" << std::endl;
    obj4.print();
    dataTree.insert_data_object(obj4);

    std::cout << std::endl << "=== Searching DataObjects ===" << std::endl;
    
    // Search for DataObjects by their numeric values
    DataObject searchObj1(1, 42);
    DataObject* found1 = dataTree.search_data_object(searchObj1);
    if (found1) {
        std::cout << "Found DataObject with key 42:" << std::endl;
        found1->print();
        delete found1;
    } else {
        std::cout << "DataObject with key 42 not found" << std::endl;
    }

    DataObject searchObj2(1, 3);
    DataObject* found2 = dataTree.search_data_object(searchObj2);
    if (found2) {
        std::cout << "Found DataObject with key 3 (approx from 3.14):" << std::endl;
        found2->print();
        delete found2;
    } else {
        std::cout << "DataObject with key 3 not found" << std::endl;
    }

    DataObject searchObj3(1, 100);
    DataObject* found3 = dataTree.search_data_object(searchObj3);
    if (found3) {
        std::cout << "Found DataObject with key 100:" << std::endl;
        found3->print();
        delete found3;
    } else {
        std::cout << "DataObject with key 100 not found" << std::endl;
    }

    DataObject searchObj4(1, 999);
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
