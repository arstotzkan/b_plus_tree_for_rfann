#pragma once
#include "node.h"
#include "page_manager.h"

template <typename T>
class DiskBPlusTree {
public:
    explicit DiskBPlusTree(const std::string& filename);
    void insert(const T& key);
    bool search(const T& key);
    bool delete_key(const T& key);
    void print_tree();

private:
    PageManager pm;
    void read(uint32_t pid, BPlusNode<T>& node);
    void write(uint32_t pid, const BPlusNode<T>& node);
    void splitLeaf(uint32_t leafPid, BPlusNode<T>& leaf, T& promotedKey, uint32_t& newLeafPid);
    void print_tree_recursive(uint32_t pid, int level);
    bool delete_from_leaf(uint32_t leafPid, const T& key);
    bool borrow_or_merge(uint32_t parentPid, int childIndex, uint32_t childPid);
    void redistribute_keys(uint32_t leftPid, uint32_t rightPid, uint32_t parentPid, int keyIndex);
    void merge_nodes(uint32_t leftPid, uint32_t rightPid, uint32_t parentPid, int keyIndex);
    int get_min_keys();
};
