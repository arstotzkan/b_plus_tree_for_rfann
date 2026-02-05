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
#include <unordered_map>

class DiskBPlusTree {
public:
    // Constructor for opening existing index
    explicit DiskBPlusTree(const std::string& filename);
    
    // Constructor for creating new index with specified config
    DiskBPlusTree(const std::string& filename, const BPTreeConfig& config);
    
    void insert_data_object(const DataObject& obj);
    
    // Bulk load: efficiently build tree from sorted data (for initial index creation)
    // Data is sorted by key, leaves are filled to fill_factor capacity, tree is built bottom-up
    // fill_factor: 0.5 to 1.0, default 0.7 (70% full leaves)
    void bulk_load(std::vector<DataObject>& objects, float fill_factor = 0.7f);
    bool delete_data_object(const DataObject& obj);  // Delete specific DataObject (matches key + vector)
    bool delete_data_object(int key);                 // Delete first entry with this key
    bool delete_data_object(float key);               // Delete first entry with this key
    DataObject* search_data_object(const DataObject& obj, bool use_memory_index = false);
    DataObject* search_data_object(int key, bool use_memory_index = false);
    DataObject* search_data_object(float key, bool use_memory_index = false);
    std::vector<DataObject*> search_range(int min_key, int max_key, bool use_memory_index = false);
    std::vector<DataObject*> search_range(float min_key, float max_key, bool use_memory_index = false);
    std::vector<DataObject*> search_knn_optimized(const std::vector<float>& query_vector, int min_key, int max_key, int k, bool use_memory_index = false);
    std::vector<DataObject*> search_knn_parallel(const std::vector<float>& query_vector, int min_key, int max_key, int k, int num_threads = 0, bool use_memory_index = false);
    bool search(const DataObject& obj, bool use_memory_index = false);
    void print_tree();
    std::pair<int, int> get_key_range();
    
    // In-memory index loading
    // max_memory_mb: 0 = load all, >0 = limit total memory usage (nodes + vectors)
    bool loadIntoMemory(size_t max_memory_mb = 0);
    void clearMemoryIndex();
    bool isMemoryIndexLoaded() const { return memory_index_loaded_; }
    size_t estimateTotalMemoryMB() const;
    
    // Access configuration
    const BPTreeConfig& getConfig() const { return pm->getConfig(); }
    uint32_t getOrder() const { return pm->getOrder(); }
    uint32_t getMaxVectorSize() const { return pm->getMaxVectorSize(); }

private:
    std::unique_ptr<PageManager> pm;
    
    // In-memory index cache
    std::unordered_map<uint32_t, BPlusNode> memory_index_;
    bool memory_index_loaded_ = false;
    
    void read(uint32_t pid, BPlusNode& node);
    void readFromMemory(uint32_t pid, BPlusNode& node) const;
    const BPlusNode* getNodeFromMemory(uint32_t pid) const;
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
