#pragma once
#include "node.h"
#include "page_manager.h"
#include "DataObject.h"
#include <iostream>
#include <vector>
#include <string>

class DiskBPlusTree {
public:
    explicit DiskBPlusTree(const std::string& filename);
    void insert_data_object(const DataObject& obj);
    DataObject* search_data_object(const DataObject& obj);
    bool search(const DataObject& obj);
    void print_tree();

private:
    PageManager pm;
    
    void read(uint32_t pid, BPlusNode& node);
    void write(uint32_t pid, const BPlusNode& node);
    void splitLeaf(uint32_t leafPid, BPlusNode& leaf, int& promotedKey, uint32_t& newLeafPid);
    void print_tree_recursive(uint32_t pid, int level);
    int get_min_keys();
};
