#include "bplustree_disk.h"
#include "DataObject.h"
#include <iostream>

int main() {
    // Test DataObject B+ tree (new functionality)
    std::cout << "=== Testing DataObject B+ Tree ===" << std::endl;
    DiskBPlusTree dataTree("data/test_data_index.bpt");

    // Create and insert DataObject with int values
    DataObject obj1(5, 42);
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
    obj2.set_vector_element(1, 200);
    obj2.set_vector_element(2, 300);
    
    std::cout << "Inserting DataObject 2:" << std::endl;
    obj2.print();
    dataTree.insert_data_object(obj2);

    // Create and insert more DataObjects
    DataObject obj3(4, 100);
    obj3.set_vector_element(0, 1);
    obj3.set_vector_element(1, 2);
    obj3.set_vector_element(2, 3);
    
    std::cout << "Inserting DataObject 3:" << std::endl;
    obj3.print();
    dataTree.insert_data_object(obj3);

    DataObject obj4(2, 25);
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

    std::cout << std::endl << "DataObject B+ Tree structure:" << std::endl;
    dataTree.print_tree();
    
    return 0;
}
