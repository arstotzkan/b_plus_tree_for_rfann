#include "bplustree_disk.h"
#include "DataObject.h"
#include <iostream>

int main() {
    DiskBPlusTree<int> tree("data/test_index.bpt");

    for (int i = 1; i <= 10; i++)
        tree.insert(i * -4);

    std::cout << "Search -4: " << tree.search(-4) << "\n";
    std::cout << "Search 55: " << tree.search(55) << "\n";
    std::cout << "Search -8: " << tree.search(-8) << "\n";
    std::cout << "Search 0: " << tree.search(0) << "\n";

    tree.print_tree();
    std::cout << std::endl;
    std::cout << "Deleting key -8..." << std::endl;
    bool deleted = tree.delete_key(-8);
    std::cout << "Delete successful: " << deleted << std::endl;
    std::cout << "Search -8: " << tree.search(-8) << std::endl;
    std::cout << std::endl;
    std::cout << "Tree structure after deletion:" << std::endl;
    tree.print_tree();
    std::cout << std::endl;
    std::cout << "Deleting key -15..." << std::endl;
    deleted = tree.delete_key(-15);
    std::cout << "Delete successful: " << deleted << std::endl;
    std::cout << "Search -15: " << tree.search(-15) << std::endl;
    std::cout << std::endl;
    std::cout << "Tree structure after second deletion:" << std::endl;
    tree.print_tree();
    
    // Test DataObject class
    std::cout << std::endl << "=== Testing DataObject ===" << std::endl;
    
    // Create DataObject with int value
    DataObject obj1(5, 42);
    obj1.set_vector_element(0, 10);
    obj1.set_vector_element(1, 20);
    obj1.set_vector_element(2, 30);
    obj1.set_vector_element(3, 40);
    obj1.set_vector_element(4, 50);
    
    std::cout << "DataObject 1 (int value):" << std::endl;
    obj1.print();
    
    // Create DataObject with float value
    DataObject obj2(3, 3.14f);
    obj2.set_vector_element(0, 100);
    obj2.set_vector_element(1, 200);
    obj2.set_vector_element(2, 300);
    
    std::cout << std::endl << "DataObject 2 (float value):" << std::endl;
    obj2.print();
    
    // Test copy constructor
    DataObject obj3 = obj1;
    obj3.set_int_value(99);
    obj3.set_vector_element(0, 999);
    
    std::cout << std::endl << "DataObject 3 (copy of obj1, modified):" << std::endl;
    obj3.print();
    
    std::cout << std::endl << "Original obj1 after copy:" << std::endl;
    obj1.print();
    
    return 0;
}
