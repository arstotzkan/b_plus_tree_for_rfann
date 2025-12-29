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
    DataObject* search_data_object(int key);
    DataObject* search_data_object(float key);
    std::vector<DataObject*> search_range(int min_key, int max_key);
    std::vector<DataObject*> search_range(float min_key, float max_key);
    bool search(const DataObject& obj);
    void print_tree();

private:
    PageManager pm;
    
    void read(uint32_t pid, BPlusNode& node);
    void write(uint32_t pid, const BPlusNode& node);
    void splitLeaf(uint32_t leafPid, BPlusNode& leaf, int& promotedKey, uint32_t& newLeafPid);
    void print_tree_recursive(uint32_t pid, int level);
    void collect_range_data(uint32_t leafPid, int min_key, int max_key, std::vector<DataObject*>& results);
    int get_min_keys();
};
