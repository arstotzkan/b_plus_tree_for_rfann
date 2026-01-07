#pragma once
#include "node.h"
#include "page_manager.h"
#include "bptree_config.h"
#include "DataObject.h"
#include <iostream>
#include <utility>
#include <vector>
#include <string>
#include <queue>
#include <cmath>
#include <thread>
#include <mutex>
#include <atomic>
#include <functional>
#include <memory>

class DiskBPlusTree {
public:
    // Constructor for opening existing index
    explicit DiskBPlusTree(const std::string& filename);
    
    // Constructor for creating new index with specified config
    DiskBPlusTree(const std::string& filename, const BPTreeConfig& config);
    
    void insert_data_object(const DataObject& obj);
    bool delete_data_object(const DataObject& obj);  // Delete specific DataObject (matches key + vector)
    bool delete_data_object(int key);                 // Delete first entry with this key
    bool delete_data_object(float key);               // Delete first entry with this key
    DataObject* search_data_object(const DataObject& obj);
    DataObject* search_data_object(int key);
    DataObject* search_data_object(float key);
    std::vector<DataObject*> search_range(int min_key, int max_key);
    std::vector<DataObject*> search_range(float min_key, float max_key);
    std::vector<DataObject*> search_knn_optimized(const std::vector<float>& query_vector, int min_key, int max_key, int k);
    std::vector<DataObject*> search_knn_parallel(const std::vector<float>& query_vector, int min_key, int max_key, int k, int num_threads = 0);
    bool search(const DataObject& obj);
    void print_tree();
    std::pair<int, int> get_key_range();
    
    // Access configuration
    const BPTreeConfig& getConfig() const { return pm->getConfig(); }
    uint32_t getOrder() const { return pm->getOrder(); }
    uint32_t getMaxVectorSize() const { return pm->getMaxVectorSize(); }

private:
    std::unique_ptr<PageManager> pm;
    
    void read(uint32_t pid, BPlusNode& node);
    void write(uint32_t pid, const BPlusNode& node);
    void splitLeaf(uint32_t leafPid, BPlusNode& leaf, int& promotedKey, uint32_t& newLeafPid);
    void print_tree_recursive(uint32_t pid, int level);
    void collect_range_data(uint32_t leafPid, int min_key, int max_key, std::vector<DataObject*>& results);
    int get_min_keys();
    
    // Helper to create initialized node
    BPlusNode createNode() const;
    
    // Deletion helper methods
    bool deleteKey(int key);
    bool deleteDataObject(int key, const std::vector<float>& vector);  // Delete matching key+vector
    bool borrowFromLeftSibling(BPlusNode& parent, int childIdx, BPlusNode& node, uint32_t nodePid);
    bool borrowFromRightSibling(BPlusNode& parent, int childIdx, BPlusNode& node, uint32_t nodePid);
    void mergeWithLeftSibling(BPlusNode& parent, int childIdx, BPlusNode& node, uint32_t nodePid);
    void mergeWithRightSibling(BPlusNode& parent, int childIdx, BPlusNode& node, uint32_t nodePid);
    bool vectorsMatch(const std::vector<float>& v1, const BPlusNode& node, int idx, uint32_t maxVecSize);
};
