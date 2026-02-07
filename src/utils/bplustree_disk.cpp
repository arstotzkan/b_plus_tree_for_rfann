#include "bplustree_disk.h"
#include "DataObject.h"
#include "logger.h"
#include <iostream>
#include <queue>
#include <vector>
#include <algorithm>
#include <thread>
#include <mutex>
#include <atomic>
#include <cmath>
#include <chrono>
#include <numeric>

DiskBPlusTree::DiskBPlusTree(const std::string& filename)
    : pm(std::make_unique<PageManager>(filename)) {}

DiskBPlusTree::DiskBPlusTree(const std::string& filename, const BPTreeConfig& config)
    : pm(std::make_unique<PageManager>(filename, config)) {}

void DiskBPlusTree::read(uint32_t pid, BPlusNode& node) {
    pm->readNode(pid, node);
}

void DiskBPlusTree::write(uint32_t pid, const BPlusNode& node) {
    pm->writeNode(pid, node);
}

size_t DiskBPlusTree::estimateTotalMemoryMB() const {
    size_t node_mb = pm->estimateNodeMemoryMB();
    size_t vector_mb = 0;
    if (pm->getVectorStore()) {
        vector_mb = pm->getVectorStore()->estimateMemoryUsageMB();
    }
    return node_mb + vector_mb;
}

bool DiskBPlusTree::loadIntoMemory(size_t max_memory_mb) {
    memory_index_.clear();
    memory_index_loaded_ = false;
    
    uint32_t rootPid = pm->getRoot();
    if (rootPid == INVALID_PAGE) {
        memory_index_loaded_ = true;
        return true;
    }
    
    // Estimate total memory needed
    size_t total_estimated = estimateTotalMemoryMB();
    std::cout << "Total estimated memory: " << total_estimated << " MB" << std::endl;
    
    // Split memory budget between nodes and vectors
    size_t node_memory_mb = 0;
    size_t vector_memory_mb = 0;
    
    if (max_memory_mb > 0) {
        size_t node_mb = pm->estimateNodeMemoryMB();
        size_t vector_mb = pm->getVectorStore() ? pm->getVectorStore()->estimateMemoryUsageMB() : 0;
        
        // Allocate proportionally based on estimated sizes
        if (node_mb + vector_mb > 0) {
            node_memory_mb = (max_memory_mb * node_mb) / (node_mb + vector_mb);
            vector_memory_mb = max_memory_mb - node_memory_mb;
        } else {
            node_memory_mb = max_memory_mb;
        }
        
        std::cout << "Memory budget: " << max_memory_mb << " MB (nodes: " << node_memory_mb << " MB, vectors: " << vector_memory_mb << " MB)" << std::endl;
    }
    
    // Use bulk sequential loading
    pm->loadAllNodes(memory_index_, node_memory_mb);
    
    memory_index_loaded_ = true;
    
    // Load vectors into memory
    if (pm->getVectorStore()) {
        pm->getVectorStore()->loadAllVectorsIntoMemory(vector_memory_mb);
    }
    
    return true;
}

void DiskBPlusTree::clearMemoryIndex() {
    memory_index_.clear();
    memory_index_loaded_ = false;
    
    // Clear vector cache
    if (pm->getVectorStore()) {
        pm->getVectorStore()->clearMemoryCache();
    }
}

void DiskBPlusTree::readFromMemory(uint32_t pid, BPlusNode& node) const {
    auto it = memory_index_.find(pid);
    if (it != memory_index_.end()) {
        node = it->second;
    } else {
        // Fallback to disk read if not found in memory (shouldn't happen if properly loaded)
        pm->readNode(pid, node);
    }
}

const BPlusNode* DiskBPlusTree::getNodeFromMemory(uint32_t pid) const {
    auto it = memory_index_.find(pid);
    if (it != memory_index_.end()) {
        return &(it->second);
    }
    return nullptr;
}

BPlusNode DiskBPlusTree::createNode() const {
    BPlusNode node;
    node.init(pm->getOrder(), pm->getMaxVectorSize());
    return node;
}

void DiskBPlusTree::splitLeaf(uint32_t leafPid, BPlusNode& leaf, int& promotedKey, uint32_t& newLeafPid) {
    BPlusNode newLeaf = createNode();
    newLeaf.isLeaf = true;

    int mid = leaf.keyCount / 2;
    newLeaf.keyCount = leaf.keyCount - mid;

    // Model B: Copy unique keys with their vector list references
    for (int i = 0; i < newLeaf.keyCount; i++) {
        newLeaf.keys[i] = leaf.keys[mid + i];
        newLeaf.vector_list_ids[i] = leaf.vector_list_ids[mid + i];
        newLeaf.vector_counts[i] = leaf.vector_counts[mid + i];
    }

    leaf.keyCount = mid;

    newLeaf.next = leaf.next;
    newLeafPid = pm->allocatePage();
    leaf.next = newLeafPid;

    promotedKey = newLeaf.keys[0];

    write(leafPid, leaf);
    write(newLeafPid, newLeaf);
}

void DiskBPlusTree::insert_data_object(const DataObject& obj) {
    int key;
    if (obj.is_int_value()) {
        key = obj.get_int_value();
    } else {
        key = static_cast<int>(obj.get_float_value());
    }
    
    uint32_t rootPid = pm->getRoot();
    uint32_t order = pm->getOrder();
    const std::vector<float>& vec = obj.get_vector();
    uint32_t vec_size = static_cast<uint32_t>(vec.size());
    int32_t orig_id = obj.get_id();

    if (rootPid == INVALID_PAGE) {
        // Create first leaf node as root
        BPlusNode root = createNode();
        root.isLeaf = true;
        root.keyCount = 1;
        root.keys[0] = key;
        root.next = INVALID_PAGE;
        
        // Store vector and get its ID
        uint64_t vector_id = pm->getVectorStore()->storeVector(vec, vec_size, orig_id);
        root.vector_list_ids[0] = vector_id;
        root.vector_counts[0] = 1;

        uint32_t pid = pm->allocatePage();
        write(pid, root);
        pm->setRoot(pid);
        return;
    }

    // Find the leaf node where key should be inserted
    std::vector<uint32_t> path;
    std::vector<int> pathIndex;
    uint32_t pid = rootPid;
    BPlusNode node;
    
    while (true) {
        read(pid, node);
        path.push_back(pid);
        
        int i = 0;
        while (i < node.keyCount && key > node.keys[i]) {
            i++;
        }
        pathIndex.push_back(i);
        
        if (node.isLeaf) {
            break;
        }
        
        pid = node.children[i];
    }

    // Model B: Check if key already exists in this leaf
    int existingIdx = -1;
    for (int i = 0; i < node.keyCount; i++) {
        if (node.keys[i] == key) {
            existingIdx = i;
            break;
        }
    }
    
    if (existingIdx >= 0) {
        // Key exists - append vector to the existing list
        uint64_t old_first_id = node.vector_list_ids[existingIdx];
        uint64_t new_first_id = pm->getVectorStore()->appendVectorToList(old_first_id, vec, vec_size, orig_id);
        node.vector_list_ids[existingIdx] = new_first_id;
        node.vector_counts[existingIdx]++;
        write(pid, node);
        return;
    }

    // Key doesn't exist - insert new unique key
    // Shift existing entries right to make room
    int i = node.keyCount - 1;
    while (i >= 0 && node.keys[i] > key) {
        node.keys[i + 1] = node.keys[i];
        node.vector_list_ids[i + 1] = node.vector_list_ids[i];
        node.vector_counts[i + 1] = node.vector_counts[i];
        i--;
    }
    
    // Insert new key with its first vector
    uint64_t vector_id = pm->getVectorStore()->storeVector(vec, vec_size, orig_id);
    node.keys[i + 1] = key;
    node.vector_list_ids[i + 1] = vector_id;
    node.vector_counts[i + 1] = 1;
    node.keyCount++;

    // If leaf doesn't overflow, just write and return
    if (node.keyCount < static_cast<int>(order)) {
        write(pid, node);
        return;
    }

    // Leaf overflows - need to split
    int promoted;
    uint32_t newNodePid;
    splitLeaf(pid, node, promoted, newNodePid);

    // Propagate split up the tree
    uint32_t childPid = newNodePid;
    int promotedKey = promoted;
    
    for (int level = static_cast<int>(path.size()) - 2; level >= 0; level--) {
        uint32_t parentPid = path[level];
        BPlusNode parent;
        read(parentPid, parent);

        // Insert promoted key and new child pointer into parent
        int j = parent.keyCount - 1;
        while (j >= 0 && parent.keys[j] > promotedKey) {
            parent.keys[j + 1] = parent.keys[j];
            parent.children[j + 2] = parent.children[j + 1];
            j--;
        }

        parent.keys[j + 1] = promotedKey;
        parent.children[j + 2] = childPid;
        parent.keyCount++;

        // If parent doesn't overflow, write and return
        if (parent.keyCount < static_cast<int>(order)) {
            write(parentPid, parent);
            return;
        }

        // Parent overflows - split internal node
        BPlusNode newInternal = createNode();
        newInternal.isLeaf = false;
        
        int mid = parent.keyCount / 2;
        promotedKey = parent.keys[mid];
        
        newInternal.keyCount = parent.keyCount - mid - 1;
        for (int k = 0; k < newInternal.keyCount; k++) {
            newInternal.keys[k] = parent.keys[mid + 1 + k];
        }
        
        for (int k = 0; k <= newInternal.keyCount; k++) {
            newInternal.children[k] = parent.children[mid + 1 + k];
        }
        
        parent.keyCount = mid;

        uint32_t newInternalPid = pm->allocatePage();
        write(parentPid, parent);
        write(newInternalPid, newInternal);
        
        childPid = newInternalPid;
    }

    // Root needs to split
    BPlusNode newRoot = createNode();
    newRoot.isLeaf = false;
    newRoot.keyCount = 1;
    newRoot.keys[0] = promotedKey;
    newRoot.children[0] = rootPid;
    newRoot.children[1] = childPid;

    uint32_t newRootPid = pm->allocatePage();
    write(newRootPid, newRoot);
    pm->setRoot(newRootPid);
}

void DiskBPlusTree::bulk_load(std::vector<DataObject>& objects, float fill_factor) {
    if (objects.empty()) {
        return;
    }
    
    // Validate fill_factor
    if (fill_factor < 0.5f) fill_factor = 0.5f;
    if (fill_factor > 1.0f) fill_factor = 1.0f;
    
    uint32_t order = pm->getOrder();
    int keys_per_leaf = static_cast<int>(order * fill_factor);
    if (keys_per_leaf < 1) keys_per_leaf = 1;
    if (keys_per_leaf >= static_cast<int>(order)) keys_per_leaf = order - 1;
    
    std::cout << "Bulk loading " << objects.size() << " objects with fill_factor=" 
              << fill_factor << " (keys_per_leaf=" << keys_per_leaf << ")" << std::endl;
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    // Pre-reserve VectorStore metadata to avoid rehashing during bulk insert
    pm->getVectorStore()->reserveMetadata(objects.size());
    
    // Step 1: Extract keys once into a flat array (avoids variant dispatch per comparison)
    size_t N = objects.size();
    std::vector<int> keys(N);
    for (size_t i = 0; i < N; i++) {
        keys[i] = objects[i].is_int_value() ? objects[i].get_int_value() 
                                            : static_cast<int>(objects[i].get_float_value());
    }
    
    // Step 1b: Permutation-based sort — sort lightweight int indices, then reorder DataObjects
    std::vector<int> perm(N);
    std::iota(perm.begin(), perm.end(), 0);
    std::sort(perm.begin(), perm.end(), [&keys](int a, int b) { return keys[a] < keys[b]; });
    
    // Reorder keys array to sorted order (needed for grouping below)
    {
        std::vector<int> sorted_keys(N);
        for (size_t i = 0; i < N; i++) sorted_keys[i] = keys[perm[i]];
        keys = std::move(sorted_keys);
    }
    
    // Apply permutation in-place using cycle-following (each DataObject moved at most once)
    {
        std::vector<bool> placed(N, false);
        for (size_t i = 0; i < N; i++) {
            if (placed[i] || perm[i] == static_cast<int>(i)) continue;
            DataObject temp = std::move(objects[i]);
            size_t j = i;
            while (perm[j] != static_cast<int>(i)) {
                objects[j] = std::move(objects[perm[j]]);
                placed[j] = true;
                j = perm[j];
            }
            objects[j] = std::move(temp);
            placed[j] = true;
        }
    }
    perm.clear();
    perm.shrink_to_fit();
    
    // Step 2: Group objects by key using pre-extracted sorted keys (no variant dispatch)
    struct KeyGroup {
        int key;
        std::vector<const DataObject*> objects;
    };
    std::vector<KeyGroup> key_groups;
    
    int current_key = keys[0];
    key_groups.push_back({current_key, {&objects[0]}});
    
    for (size_t i = 1; i < N; i++) {
        if (keys[i] == current_key) {
            key_groups.back().objects.push_back(&objects[i]);
        } else {
            current_key = keys[i];
            key_groups.push_back({current_key, {&objects[i]}});
        }
    }
    keys.clear();
    keys.shrink_to_fit();
    
    std::cout << "  Grouped into " << key_groups.size() << " unique keys" << std::endl;
    
    // Step 3: Build leaf nodes
    // Keep previous leaf in memory to avoid re-reading from disk for next-pointer linking
    std::vector<uint32_t> leaf_pids;
    std::vector<int> leaf_first_keys;
    BPlusNode prev_leaf;
    uint32_t prev_leaf_pid = INVALID_PAGE;
    
    size_t group_idx = 0;
    while (group_idx < key_groups.size()) {
        BPlusNode leaf = createNode();
        leaf.isLeaf = true;
        leaf.keyCount = 0;
        
        // Fill this leaf with keys up to keys_per_leaf
        while (leaf.keyCount < keys_per_leaf && group_idx < key_groups.size()) {
            const KeyGroup& group = key_groups[group_idx];
            
            // Store all vectors for this key
            uint64_t first_vector_id = 0;
            uint32_t vector_count = 0;
            
            for (const DataObject* obj : group.objects) {
                const std::vector<float>& vec = obj->get_vector();
                uint32_t vec_size = static_cast<uint32_t>(vec.size());
                int32_t orig_id = obj->get_id();
                
                if (first_vector_id == 0) {
                    first_vector_id = pm->getVectorStore()->storeVector(vec, vec_size, orig_id);
                } else {
                    first_vector_id = pm->getVectorStore()->appendVectorToList(first_vector_id, vec, vec_size, orig_id);
                }
                vector_count++;
            }
            
            leaf.keys[leaf.keyCount] = group.key;
            leaf.vector_list_ids[leaf.keyCount] = first_vector_id;
            leaf.vector_counts[leaf.keyCount] = vector_count;
            leaf.keyCount++;
            group_idx++;
        }
        
        uint32_t leaf_pid = pm->allocatePageDeferred();
        leaf_pids.push_back(leaf_pid);
        leaf_first_keys.push_back(leaf.keys[0]);
        
        // Link previous leaf to this one (using in-memory copy, no disk re-read)
        if (prev_leaf_pid != INVALID_PAGE) {
            prev_leaf.next = leaf_pid;
            write(prev_leaf_pid, prev_leaf);
        }
        
        leaf.next = INVALID_PAGE;
        prev_leaf = leaf;
        prev_leaf_pid = leaf_pid;
    }
    
    // Write the last leaf (its next pointer is already INVALID_PAGE)
    if (prev_leaf_pid != INVALID_PAGE) {
        write(prev_leaf_pid, prev_leaf);
    }
    
    std::cout << "  Created " << leaf_pids.size() << " leaf nodes" << std::endl;
    
    // Step 4: Build internal nodes bottom-up
    if (leaf_pids.size() == 1) {
        pm->setRootDeferred(leaf_pids[0]);
    } else {
        // Build internal node levels
        std::vector<uint32_t> current_level_pids = leaf_pids;
        std::vector<int> current_level_keys = leaf_first_keys;
        
        int keys_per_internal = static_cast<int>(order * fill_factor);
        if (keys_per_internal < 1) keys_per_internal = 1;
        if (keys_per_internal >= static_cast<int>(order)) keys_per_internal = order - 1;
        
        while (current_level_pids.size() > 1) {
            std::vector<uint32_t> next_level_pids;
            std::vector<int> next_level_keys;
            
            size_t child_idx = 0;
            while (child_idx < current_level_pids.size()) {
                BPlusNode internal = createNode();
                internal.isLeaf = false;
                internal.keyCount = 0;
                
                // First child pointer
                internal.children[0] = current_level_pids[child_idx];
                int first_key = current_level_keys[child_idx];
                child_idx++;
                
                // Add keys and child pointers
                while (internal.keyCount < keys_per_internal && child_idx < current_level_pids.size()) {
                    internal.keys[internal.keyCount] = current_level_keys[child_idx];
                    internal.children[internal.keyCount + 1] = current_level_pids[child_idx];
                    internal.keyCount++;
                    child_idx++;
                }
                
                uint32_t internal_pid = pm->allocatePageDeferred();
                next_level_pids.push_back(internal_pid);
                next_level_keys.push_back(first_key);
                write(internal_pid, internal);
            }
            
            std::cout << "  Created " << next_level_pids.size() << " internal nodes at level" << std::endl;
            current_level_pids = std::move(next_level_pids);
            current_level_keys = std::move(next_level_keys);
        }
        
        pm->setRootDeferred(current_level_pids[0]);
    }
    
    // Single header save + vector store flush at the end (instead of per-allocation)
    pm->getVectorStore()->flush();
    pm->saveHeader();
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    std::cout << "Bulk load completed in " << duration.count() << " ms" << std::endl;
    std::cout << "  Root page: " << pm->getRoot() << std::endl;
}

DataObject* DiskBPlusTree::search_data_object(const DataObject& obj, bool use_memory_index) {
    int key;
    if (obj.is_int_value()) {
        key = obj.get_int_value();
    } else {
        key = static_cast<int>(obj.get_float_value());
    }
    
    uint32_t pid = pm->getRoot();
    if (pid == INVALID_PAGE) return nullptr;
    
    BPlusNode diskNode;
    const BPlusNode* nodePtr = nullptr;
    
    while (true) {
        if (use_memory_index && memory_index_loaded_) {
            nodePtr = getNodeFromMemory(pid);
        } else {
            read(pid, diskNode);
            nodePtr = &diskNode;
        }
        
        int i = 0;
        while (i < nodePtr->keyCount && key > nodePtr->keys[i]) i++;
        
        if (nodePtr->isLeaf) break;
        pid = nodePtr->children[i];
    }
    
    // Model B: Find unique key and retrieve first vector from its list
    for (int i = 0; i < nodePtr->keyCount; i++) {
        if (nodePtr->keys[i] == key) {
            std::vector<float> vec;
            uint32_t actual_size;
            int32_t original_id;
            pm->getVectorStore()->retrieveVector(nodePtr->vector_list_ids[i], vec, actual_size, original_id);
            DataObject* result = new DataObject(vec, key);
            result->set_id(original_id);
            return result;
        }
    }
    
    return nullptr;
}

DataObject* DiskBPlusTree::search_data_object(int key, bool use_memory_index) {
    uint32_t pid = pm->getRoot();
    if (pid == INVALID_PAGE) return nullptr;
    
    BPlusNode diskNode;
    const BPlusNode* nodePtr = nullptr;
    
    // Navigate to the leaf that should contain the key
    while (true) {
        if (use_memory_index && memory_index_loaded_) {
            nodePtr = getNodeFromMemory(pid);
        } else {
            read(pid, diskNode);
            nodePtr = &diskNode;
        }
        
        int i = 0;
        while (i < nodePtr->keyCount && key > nodePtr->keys[i]) i++;
        
        if (nodePtr->isLeaf) break;
        pid = nodePtr->children[i];
    }
    
    // Model B: Keys are unique per leaf, search for exact match
    for (int i = 0; i < nodePtr->keyCount; i++) {
        if (nodePtr->keys[i] == key) {
            std::vector<float> vec;
            uint32_t actual_size;
            int32_t original_id;
            pm->getVectorStore()->retrieveVector(nodePtr->vector_list_ids[i], vec, actual_size, original_id);
            DataObject* result = new DataObject(vec, key);
            result->set_id(original_id);
            return result;
        }
        if (nodePtr->keys[i] > key) {
            return nullptr;
        }
    }
    
    return nullptr;
}

DataObject* DiskBPlusTree::search_data_object(float key, bool use_memory_index) {
    return search_data_object(static_cast<int>(key), use_memory_index);
}

bool DiskBPlusTree::delete_data_object(const DataObject& obj) {
    int key;
    if (obj.is_int_value()) {
        key = obj.get_int_value();
    } else {
        key = static_cast<int>(obj.get_float_value());
    }
    return deleteDataObject(key, obj.get_vector());
}

bool DiskBPlusTree::delete_data_object(int key) {
    return deleteKey(key);
}

bool DiskBPlusTree::delete_data_object(float key) {
    return deleteKey(static_cast<int>(key));
}

bool DiskBPlusTree::deleteDataObject(int key, const std::vector<float>& vector) {
    uint32_t rootPid = pm->getRoot();
    if (rootPid == INVALID_PAGE) {
        return false;
    }
    
    uint32_t order = pm->getOrder();
    int minKeys = (order - 1) / 2;
    
    // Find the leaf node containing the key
    std::vector<uint32_t> path;
    std::vector<int> pathIndex;
    uint32_t pid = rootPid;
    BPlusNode node;
    
    while (true) {
        read(pid, node);
        path.push_back(pid);
        
        int i = 0;
        while (i < node.keyCount && key > node.keys[i]) {
            i++;
        }
        pathIndex.push_back(i);
        
        if (node.isLeaf) {
            break;
        }
        pid = node.children[i];
    }
    
    // Model B: Find the unique key
    int keyIndex = -1;
    for (int i = 0; i < node.keyCount; i++) {
        if (node.keys[i] == key) {
            keyIndex = i;
            break;
        }
        if (node.keys[i] > key) {
            return false;  // Key not found
        }
    }
    
    if (keyIndex < 0) {
        return false;  // Key not found
    }
    
    // Remove the specific vector from the list
    uint32_t new_count;
    uint64_t new_first_id = pm->getVectorStore()->removeVectorFromList(
        node.vector_list_ids[keyIndex], 
        node.vector_counts[keyIndex],
        vector, 
        new_count
    );
    
    if (new_count == node.vector_counts[keyIndex]) {
        return false;  // Vector not found in list
    }
    
    if (new_count > 0) {
        // List still has vectors, just update the reference
        node.vector_list_ids[keyIndex] = new_first_id;
        node.vector_counts[keyIndex] = new_count;
        write(pid, node);
        return true;
    }
    
    // List is now empty - remove the entire key from the leaf
    // Remove the entry by shifting elements left
    for (int i = keyIndex; i < node.keyCount - 1; i++) {
        node.keys[i] = node.keys[i + 1];
        node.vector_list_ids[i] = node.vector_list_ids[i + 1];
        node.vector_counts[i] = node.vector_counts[i + 1];
    }
    node.keyCount--;
    
    // If this is the root leaf node
    if (path.size() == 1) {
        if (node.keyCount == 0) {
            pm->setRoot(INVALID_PAGE);
        } else {
            write(pid, node);
        }
        return true;
    }
    
    // Update ancestor separator keys if needed
    if (node.keyCount > 0) {
        int replacementKey = node.keys[0];
        for (int level = static_cast<int>(path.size()) - 2; level >= 0; level--) {
            BPlusNode ancestor;
            read(path[level], ancestor);
            bool modified = false;
            
            for (int k = 0; k < ancestor.keyCount; k++) {
                if (ancestor.keys[k] == key) {
                    ancestor.keys[k] = replacementKey;
                    modified = true;
                }
            }
            
            if (modified) {
                write(path[level], ancestor);
            }
        }
    }
    
    // If leaf has enough keys, just write and return
    if (node.keyCount >= minKeys) {
        write(pid, node);
        return true;
    }
    
    // Leaf underflows - handle borrowing or merging
    write(pid, node);
    
    for (int level = static_cast<int>(path.size()) - 1; level > 0; level--) {
        uint32_t currentPid = path[level];
        uint32_t parentPid = path[level - 1];
        int childIdx = pathIndex[level - 1];
        
        BPlusNode current;
        read(currentPid, current);
        
        if (current.keyCount >= minKeys) {
            break;
        }
        
        BPlusNode parent;
        read(parentPid, parent);
        
        if (childIdx > 0) {
            if (borrowFromLeftSibling(parent, childIdx, current, currentPid)) {
                write(parentPid, parent);
                return true;
            }
        }
        
        if (childIdx < parent.keyCount) {
            if (borrowFromRightSibling(parent, childIdx, current, currentPid)) {
                write(parentPid, parent);
                return true;
            }
        }
        
        if (childIdx > 0) {
            mergeWithLeftSibling(parent, childIdx, current, currentPid);
        } else {
            mergeWithRightSibling(parent, childIdx, current, currentPid);
        }
        
        write(parentPid, parent);
        
        if (parentPid == rootPid && parent.keyCount == 0) {
            pm->setRoot(parent.children[0]);
            break;
        }
    }
    
    return true;
}

bool DiskBPlusTree::deleteKey(int key) {
    uint32_t rootPid = pm->getRoot();
    if (rootPid == INVALID_PAGE) {
        return false;
    }
    
    uint32_t order = pm->getOrder();
    int minKeys = (order - 1) / 2;
    
    // Find the leaf node containing the key
    std::vector<uint32_t> path;
    std::vector<int> pathIndex;
    uint32_t pid = rootPid;
    BPlusNode node;
    
    while (true) {
        read(pid, node);
        path.push_back(pid);
        
        int i = 0;
        while (i < node.keyCount && key > node.keys[i]) {
            i++;
        }
        pathIndex.push_back(i);
        
        if (node.isLeaf) {
            break;
        }
        pid = node.children[i];
    }
    
    // Model B: Find the unique key
    int keyIndex = -1;
    for (int i = 0; i < node.keyCount; i++) {
        if (node.keys[i] == key) {
            keyIndex = i;
            break;
        }
        if (node.keys[i] > key) {
            break;
        }
    }
    
    if (keyIndex == -1) {
        return false;
    }
    
    // Remove the key by shifting elements left
    for (int i = keyIndex; i < node.keyCount - 1; i++) {
        node.keys[i] = node.keys[i + 1];
        node.vector_list_ids[i] = node.vector_list_ids[i + 1];
        node.vector_counts[i] = node.vector_counts[i + 1];
    }
    node.keyCount--;
    
    // If this is the root leaf node, just write it back
    if (path.size() == 1) {
        // If root becomes empty, tree becomes empty
        if (node.keyCount == 0) {
            pm->setRoot(INVALID_PAGE);
        } else {
            write(pid, node);
        }
        return true;
    }
    
    // Update all ancestor separator keys that match the deleted key
    // This is necessary because B+ tree separators are copies of leaf keys
    if (node.keyCount > 0) {
        int replacementKey = node.keys[0]; // New first key of the leaf after deletion
        for (int level = static_cast<int>(path.size()) - 2; level >= 0; level--) {
            BPlusNode ancestor;
            read(path[level], ancestor);
            bool modified = false;
            
            // Check all keys in this ancestor node
            for (int k = 0; k < ancestor.keyCount; k++) {
                if (ancestor.keys[k] == key) {
                    ancestor.keys[k] = replacementKey;
                    modified = true;
                }
            }
            
            if (modified) {
                write(path[level], ancestor);
            }
        }
    }
    
    // If leaf has enough keys, just write and return
    if (node.keyCount >= minKeys) {
        write(pid, node);
        return true;
    }
    
    // Leaf underflows - need to borrow or merge
    write(pid, node);
    
    // Handle underflow by borrowing or merging, propagating up if needed
    for (int level = static_cast<int>(path.size()) - 1; level > 0; level--) {
        uint32_t currentPid = path[level];
        uint32_t parentPid = path[level - 1];
        int childIdx = pathIndex[level - 1];
        
        BPlusNode current;
        read(currentPid, current);
        
        // Check if current node is underflowing
        if (current.keyCount >= minKeys) {
            break; // No underflow, we're done
        }
        
        BPlusNode parent;
        read(parentPid, parent);
        
        // Try to borrow from left sibling
        if (childIdx > 0) {
            if (borrowFromLeftSibling(parent, childIdx, current, currentPid)) {
                write(parentPid, parent);
                return true;
            }
        }
        
        // Try to borrow from right sibling
        if (childIdx < parent.keyCount) {
            if (borrowFromRightSibling(parent, childIdx, current, currentPid)) {
                write(parentPid, parent);
                return true;
            }
        }
        
        // Must merge - prefer merging with left sibling
        if (childIdx > 0) {
            mergeWithLeftSibling(parent, childIdx, current, currentPid);
        } else {
            mergeWithRightSibling(parent, childIdx, current, currentPid);
        }
        
        write(parentPid, parent);
        
        // If parent is root and now has 0 keys, make the merged child the new root
        if (parentPid == rootPid && parent.keyCount == 0) {
            pm->setRoot(parent.children[0]);
            break;
        }
    }
    
    return true;
}

bool DiskBPlusTree::borrowFromLeftSibling(BPlusNode& parent, int childIdx, BPlusNode& node, uint32_t nodePid) {
    uint32_t leftPid = parent.children[childIdx - 1];
    BPlusNode leftSibling;
    read(leftPid, leftSibling);
    
    int minKeys = (pm->getOrder() - 1) / 2;
    
    if (leftSibling.keyCount <= minKeys) {
        return false;
    }
    
    if (node.isLeaf) {
        // Shift all keys in current node right
        for (int i = node.keyCount; i > 0; i--) {
            node.keys[i] = node.keys[i - 1];
            node.vector_list_ids[i] = node.vector_list_ids[i - 1];
            node.vector_counts[i] = node.vector_counts[i - 1];
        }
        
        // Move last key from left sibling to current node
        int lastIdx = leftSibling.keyCount - 1;
        node.keys[0] = leftSibling.keys[lastIdx];
        node.vector_list_ids[0] = leftSibling.vector_list_ids[lastIdx];
        node.vector_counts[0] = leftSibling.vector_counts[lastIdx];
        node.keyCount++;
        leftSibling.keyCount--;
        
        parent.keys[childIdx - 1] = node.keys[0];
    } else {
        // Internal node borrowing
        for (int i = node.keyCount; i > 0; i--) {
            node.keys[i] = node.keys[i - 1];
        }
        for (int i = node.keyCount + 1; i > 0; i--) {
            node.children[i] = node.children[i - 1];
        }
        
        node.keys[0] = parent.keys[childIdx - 1];
        node.children[0] = leftSibling.children[leftSibling.keyCount];
        node.keyCount++;
        
        parent.keys[childIdx - 1] = leftSibling.keys[leftSibling.keyCount - 1];
        leftSibling.keyCount--;
    }
    
    write(leftPid, leftSibling);
    write(nodePid, node);
    return true;
}

bool DiskBPlusTree::borrowFromRightSibling(BPlusNode& parent, int childIdx, BPlusNode& node, uint32_t nodePid) {
    uint32_t rightPid = parent.children[childIdx + 1];
    BPlusNode rightSibling;
    read(rightPid, rightSibling);
    
    int minKeys = (pm->getOrder() - 1) / 2;
    
    if (rightSibling.keyCount <= minKeys) {
        return false;
    }
    
    if (node.isLeaf) {
        // Move first key from right sibling to end of current node
        node.keys[node.keyCount] = rightSibling.keys[0];
        node.vector_list_ids[node.keyCount] = rightSibling.vector_list_ids[0];
        node.vector_counts[node.keyCount] = rightSibling.vector_counts[0];
        node.keyCount++;
        
        // Shift all keys in right sibling left
        for (int i = 0; i < rightSibling.keyCount - 1; i++) {
            rightSibling.keys[i] = rightSibling.keys[i + 1];
            rightSibling.vector_list_ids[i] = rightSibling.vector_list_ids[i + 1];
            rightSibling.vector_counts[i] = rightSibling.vector_counts[i + 1];
        }
        rightSibling.keyCount--;
        
        parent.keys[childIdx] = rightSibling.keys[0];
    } else {
        // Internal node borrowing
        node.keys[node.keyCount] = parent.keys[childIdx];
        node.children[node.keyCount + 1] = rightSibling.children[0];
        node.keyCount++;
        
        parent.keys[childIdx] = rightSibling.keys[0];
        
        for (int i = 0; i < rightSibling.keyCount - 1; i++) {
            rightSibling.keys[i] = rightSibling.keys[i + 1];
        }
        for (int i = 0; i < rightSibling.keyCount; i++) {
            rightSibling.children[i] = rightSibling.children[i + 1];
        }
        rightSibling.keyCount--;
    }
    
    write(rightPid, rightSibling);
    write(nodePid, node);
    return true;
}

void DiskBPlusTree::mergeWithLeftSibling(BPlusNode& parent, int childIdx, BPlusNode& node, uint32_t nodePid) {
    uint32_t leftPid = parent.children[childIdx - 1];
    BPlusNode leftSibling;
    read(leftPid, leftSibling);
    
    if (node.isLeaf) {
        // Move all keys from current node to left sibling
        for (int i = 0; i < node.keyCount; i++) {
            leftSibling.keys[leftSibling.keyCount + i] = node.keys[i];
            leftSibling.vector_list_ids[leftSibling.keyCount + i] = node.vector_list_ids[i];
            leftSibling.vector_counts[leftSibling.keyCount + i] = node.vector_counts[i];
        }
        leftSibling.keyCount += node.keyCount;
        leftSibling.next = node.next;
    } else {
        // Internal node merge
        leftSibling.keys[leftSibling.keyCount] = parent.keys[childIdx - 1];
        leftSibling.keyCount++;
        
        for (int i = 0; i < node.keyCount; i++) {
            leftSibling.keys[leftSibling.keyCount + i] = node.keys[i];
        }
        for (int i = 0; i <= node.keyCount; i++) {
            leftSibling.children[leftSibling.keyCount + i] = node.children[i];
        }
        leftSibling.keyCount += node.keyCount;
    }
    
    write(leftPid, leftSibling);
    
    for (int i = childIdx - 1; i < parent.keyCount - 1; i++) {
        parent.keys[i] = parent.keys[i + 1];
    }
    for (int i = childIdx; i < parent.keyCount; i++) {
        parent.children[i] = parent.children[i + 1];
    }
    parent.keyCount--;
}

void DiskBPlusTree::mergeWithRightSibling(BPlusNode& parent, int childIdx, BPlusNode& node, uint32_t nodePid) {
    uint32_t rightPid = parent.children[childIdx + 1];
    BPlusNode rightSibling;
    read(rightPid, rightSibling);
    
    if (node.isLeaf) {
        // Move all keys from right sibling to current node
        for (int i = 0; i < rightSibling.keyCount; i++) {
            node.keys[node.keyCount + i] = rightSibling.keys[i];
            node.vector_list_ids[node.keyCount + i] = rightSibling.vector_list_ids[i];
            node.vector_counts[node.keyCount + i] = rightSibling.vector_counts[i];
        }
        node.keyCount += rightSibling.keyCount;
        node.next = rightSibling.next;
    } else {
        // Internal node merge
        node.keys[node.keyCount] = parent.keys[childIdx];
        node.keyCount++;
        
        for (int i = 0; i < rightSibling.keyCount; i++) {
            node.keys[node.keyCount + i] = rightSibling.keys[i];
        }
        for (int i = 0; i <= rightSibling.keyCount; i++) {
            node.children[node.keyCount + i] = rightSibling.children[i];
        }
        node.keyCount += rightSibling.keyCount;
    }
    
    write(nodePid, node);
    
    for (int i = childIdx; i < parent.keyCount - 1; i++) {
        parent.keys[i] = parent.keys[i + 1];
    }
    for (int i = childIdx + 1; i < parent.keyCount + 1; i++) {
        parent.children[i] = parent.children[i + 1];
    }
    parent.keyCount--;
}

std::vector<DataObject*> DiskBPlusTree::search_range(int min_key, int max_key, bool use_memory_index) {
    std::vector<DataObject*> results;
    
    uint32_t pid = pm->getRoot();
    if (pid == INVALID_PAGE) {
        return results;
    }
    
    // Find leaf node that contains min_key
    BPlusNode diskNode;
    const BPlusNode* nodePtr = nullptr;
    
    while (true) {
        if (use_memory_index && memory_index_loaded_) {
            nodePtr = getNodeFromMemory(pid);
        } else {
            read(pid, diskNode);
            nodePtr = &diskNode;
        }
        
        int i = 0;
        while (i < nodePtr->keyCount && min_key > nodePtr->keys[i]) i++;
        
        if (nodePtr->isLeaf) {
            break;
        }
        pid = nodePtr->children[i];
    }
    
    // Collect data from this leaf and subsequent leaves until max_key is reached
    uint32_t currentPid = pid;
    
    while (currentPid != INVALID_PAGE) {
        const BPlusNode* leafPtr = nullptr;
        BPlusNode diskLeaf;
        if (use_memory_index && memory_index_loaded_) {
            leafPtr = getNodeFromMemory(currentPid);
        } else {
            read(currentPid, diskLeaf);
            leafPtr = &diskLeaf;
        }
        if (!leafPtr) break;
        
        for (int i = 0; i < leafPtr->keyCount; i++) {
            if (leafPtr->keys[i] >= min_key && leafPtr->keys[i] <= max_key) {
                // Model B: Retrieve ALL vectors for this key
                std::vector<std::vector<float>> vectors;
                std::vector<uint32_t> sizes;
                std::vector<int32_t> original_ids;
                pm->getVectorStore()->retrieveVectorList(
                    leafPtr->vector_list_ids[i], 
                    leafPtr->vector_counts[i],
                    vectors, sizes, original_ids
                );
                
                for (size_t v = 0; v < vectors.size(); v++) {
                    DataObject* result = new DataObject(vectors[v], leafPtr->keys[i]);
                    if (v < original_ids.size()) result->set_id(original_ids[v]);
                    results.push_back(result);
                }
            }
            else if (leafPtr->keys[i] > max_key) {
                return results;
            }
        }
        
        uint32_t nextPid = leafPtr->next;
        if (nextPid == currentPid) {
            break;
        }
        currentPid = nextPid;
    }
    
    return results;
}

std::vector<DataObject*> DiskBPlusTree::search_range(float min_key, float max_key, bool use_memory_index) {
    return search_range(static_cast<int>(min_key), static_cast<int>(max_key), use_memory_index);
}

bool DiskBPlusTree::search(const DataObject& obj, bool use_memory_index) {
    int key;
    if (obj.is_int_value()) {
        key = obj.get_int_value();
    } else {
        key = static_cast<int>(obj.get_float_value());
    }
    
    uint32_t pid = pm->getRoot();
    if (pid == INVALID_PAGE) return false;
    
    BPlusNode diskNode;
    const BPlusNode* nodePtr = nullptr;
    
    while (true) {
        if (use_memory_index && memory_index_loaded_) {
            nodePtr = getNodeFromMemory(pid);
        } else {
            read(pid, diskNode);
            nodePtr = &diskNode;
        }
        
        int i = 0;
        while (i < nodePtr->keyCount && key > nodePtr->keys[i]) i++;
        
        if (nodePtr->isLeaf) break;
        pid = nodePtr->children[i];
    }
    
    for (int i = 0; i < nodePtr->keyCount; i++) {
        if (nodePtr->keys[i] == key) {
            return true;
        }
    }
    
    return false;
}

void DiskBPlusTree::print_tree() {
    uint32_t rootPid = pm->getRoot();
    if (rootPid == INVALID_PAGE) {
        std::cout << "(empty tree)" << std::endl;
        return;
    }
    print_tree_recursive(rootPid, 0);
}

void DiskBPlusTree::print_tree_recursive(uint32_t pid, int level) {
    BPlusNode node;
    read(pid, node);
    
    std::string indent(level * 2, ' ');
    std::cout << indent << "Node " << pid << " (";
    if (node.isLeaf) std::cout << "leaf";
    else std::cout << "internal";
    std::cout << ", keys=" << node.keyCount << "): [";
    
    for (int i = 0; i < node.keyCount; i++) {
        if (i > 0) std::cout << ", ";
        std::cout << node.keys[i];
        if (node.isLeaf) {
            // Model B: Show vector list info
            std::cout << "(list_id=" << node.vector_list_ids[i] << ",count=" << node.vector_counts[i] << ")";
        }
    }
    std::cout << "]" << std::endl;
    
    if (!node.isLeaf) {
        for (int i = 0; i <= node.keyCount; i++) {
            if (node.children[i] != INVALID_PAGE) {
                print_tree_recursive(node.children[i], level + 1);
            }
        }
    }
}

std::pair<int, int> DiskBPlusTree::get_key_range() {
    uint32_t pid = pm->getRoot();
    if (pid == INVALID_PAGE) {
        return {0, -1};
    }

    BPlusNode node;
    while (true) {
        read(pid, node);
        if (node.isLeaf) {
            break;
        }
        pid = node.children[0];
    }

    if (node.keyCount == 0) {
        return {0, -1};
    }

    int min_key = node.keys[0];
    int max_key = node.keys[node.keyCount - 1];

    uint32_t currentPid = pid;
    while (currentPid != INVALID_PAGE) {
        BPlusNode leaf;
        read(currentPid, leaf);

        if (leaf.keyCount > 0) {
            max_key = leaf.keys[leaf.keyCount - 1];
        }

        uint32_t nextPid = leaf.next;
        if (nextPid == INVALID_PAGE || nextPid == currentPid) {
            break;
        }
        currentPid = nextPid;
    }

    return {min_key, max_key};
}

// Calculate Euclidean distance between two vectors
static double calculate_euclidean_distance(const std::vector<float>& v1, const std::vector<float>& v2) {
    double sum = 0.0;
    size_t min_size = std::min(v1.size(), v2.size());
    for (size_t i = 0; i < min_size; i++) {
        double diff = static_cast<double>(v1[i]) - static_cast<double>(v2[i]);
        sum += diff * diff;
    }
    return std::sqrt(sum);
}

std::vector<DataObject*> DiskBPlusTree::search_knn_optimized(const std::vector<float>& query_vector, int min_key, int max_key, int k, bool use_memory_index) {
    auto search_start = std::chrono::high_resolution_clock::now();
    std::vector<DataObject*> results;
    
    uint32_t pid = pm->getRoot();
    if (pid == INVALID_PAGE || k <= 0) {
        return results;
    }
    
    // Priority queue to maintain K nearest neighbors (max-heap by distance)
    std::priority_queue<std::pair<double, DataObject*>> knn_heap;
    
    // Find leaf node that contains min_key
    auto traversal_start = std::chrono::high_resolution_clock::now();
    BPlusNode diskNode;
    const BPlusNode* nodePtr = nullptr;
    int tree_reads = 0;
    
    while (true) {
        if (use_memory_index && memory_index_loaded_) {
            nodePtr = getNodeFromMemory(pid);
        } else {
            read(pid, diskNode);
            nodePtr = &diskNode;
        }
        tree_reads++;
        
        int i = 0;
        while (i < nodePtr->keyCount && min_key > nodePtr->keys[i]) i++;
        
        if (nodePtr->isLeaf) {
            break;
        }
        pid = nodePtr->children[i];
    }
    
    auto traversal_end = std::chrono::high_resolution_clock::now();
    auto traversal_time = std::chrono::duration_cast<std::chrono::microseconds>(traversal_end - traversal_start).count();
    Logger::debug("Tree traversal completed: " + std::to_string(tree_reads) + " node reads, " + std::to_string(traversal_time) + " μs");
    
    // Traverse leaves and maintain K best candidates
    auto leaf_scan_start = std::chrono::high_resolution_clock::now();
    uint32_t currentPid = pid;
    int leaf_reads = 0;
    int vectors_processed = 0;
    long long vector_reconstruction_time = 0;
    long long distance_calculation_time = 0;
    long long heap_operation_time = 0;
    long long readahead_time = 0;
    
    // Progress tracking
    int range_size = max_key - min_key + 1;
    int keys_processed = 0;
    int last_progress_percent = -1;
    auto last_progress_time = leaf_scan_start;
    
    // Model B: Process all vectors from each key's list
    if (use_memory_index && memory_index_loaded_) {
        while (currentPid != INVALID_PAGE) {
            const BPlusNode* leafPtr = getNodeFromMemory(currentPid);
            if (!leafPtr) break;
            leaf_reads++;
            
            for (int i = 0; i < leafPtr->keyCount; i++) {
                if (leafPtr->keys[i] >= min_key && leafPtr->keys[i] <= max_key) {
                    keys_processed++;
                    
                    // Progress logging
                    int progress_percent = (keys_processed * 100) / range_size;
                    if (progress_percent != last_progress_percent && progress_percent % 10 == 0) {
                        auto current_time = std::chrono::high_resolution_clock::now();
                        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(current_time - last_progress_time).count();
                        Logger::info("Search progress: " + std::to_string(progress_percent) + "% (" + 
                                    std::to_string(keys_processed) + "/" + std::to_string(range_size) + " keys) | " +
                                    std::to_string(vectors_processed) + " vectors | " + std::to_string(elapsed) + "ms");
                        last_progress_percent = progress_percent;
                        last_progress_time = current_time;
                    }
                    
                    // Model B: Retrieve ALL vectors for this key
                    std::vector<std::vector<float>> vectors;
                    std::vector<uint32_t> sizes;
                    std::vector<int32_t> original_ids;
                    pm->getVectorStore()->retrieveVectorList(
                        leafPtr->vector_list_ids[i], 
                        leafPtr->vector_counts[i],
                        vectors, sizes, original_ids
                    );
                    
                    for (size_t v = 0; v < vectors.size(); v++) {
                        vectors_processed++;
                        double distance = calculate_euclidean_distance(query_vector, vectors[v]);
                        DataObject* candidate = new DataObject(vectors[v], leafPtr->keys[i]);
                        if (v < original_ids.size()) candidate->set_id(original_ids[v]);
                        
                        if (knn_heap.size() < static_cast<size_t>(k)) {
                            knn_heap.push({distance, candidate});
                        } else if (distance < knn_heap.top().first) {
                            delete knn_heap.top().second;
                            knn_heap.pop();
                            knn_heap.push({distance, candidate});
                        } else {
                            delete candidate;
                        }
                    }
                }
                else if (leafPtr->keys[i] > max_key) {
                    goto extract_results;
                }
            }
            currentPid = leafPtr->next;
        }
        goto extract_results;
    }
    
    // DISK PATH: Use read-ahead buffer for better I/O performance
    {
        const int READAHEAD_SIZE = 3;
        std::vector<BPlusNode> readahead_buffer;
        std::vector<uint32_t> readahead_pids;
        
        auto batch_read_start = std::chrono::high_resolution_clock::now();
        BPlusNode current_leaf;
        read(currentPid, current_leaf);
        leaf_reads++;
        
        uint32_t next_pid = current_leaf.next;
        for (int i = 0; i < READAHEAD_SIZE && next_pid != INVALID_PAGE; i++) {
            BPlusNode ahead_leaf;
            read(next_pid, ahead_leaf);
            leaf_reads++;
            readahead_buffer.push_back(ahead_leaf);
            readahead_pids.push_back(next_pid);
            next_pid = ahead_leaf.next;
        }
        auto batch_read_end = std::chrono::high_resolution_clock::now();
        readahead_time += std::chrono::duration_cast<std::chrono::microseconds>(batch_read_end - batch_read_start).count();
        
        while (currentPid != INVALID_PAGE) {
            const BPlusNode& leaf = current_leaf;
                    
            for (int i = 0; i < leaf.keyCount; i++) {
                if (leaf.keys[i] >= min_key && leaf.keys[i] <= max_key) {
                    keys_processed++;
                    
                    // Progress logging
                    int progress_percent = (keys_processed * 100) / range_size;
                    if (progress_percent != last_progress_percent && progress_percent % 10 == 0) {
                        auto current_time = std::chrono::high_resolution_clock::now();
                        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(current_time - last_progress_time).count();
                        Logger::info("Search progress: " + std::to_string(progress_percent) + "% (" + 
                                    std::to_string(keys_processed) + "/" + std::to_string(range_size) + " keys) | " +
                                    std::to_string(vectors_processed) + " vectors | " + std::to_string(elapsed) + "ms");
                        last_progress_percent = progress_percent;
                        last_progress_time = current_time;
                    }
                    
                    // Model B: Retrieve ALL vectors for this key
                    auto vec_start = std::chrono::high_resolution_clock::now();
                    std::vector<std::vector<float>> vectors;
                    std::vector<uint32_t> sizes;
                    std::vector<int32_t> original_ids;
                    pm->getVectorStore()->retrieveVectorList(
                        leaf.vector_list_ids[i], 
                        leaf.vector_counts[i],
                        vectors, sizes, original_ids
                    );
                    auto vec_end = std::chrono::high_resolution_clock::now();
                    vector_reconstruction_time += std::chrono::duration_cast<std::chrono::microseconds>(vec_end - vec_start).count();
                    
                    for (size_t v = 0; v < vectors.size(); v++) {
                        vectors_processed++;
                        
                        auto dist_start = std::chrono::high_resolution_clock::now();
                        double distance = calculate_euclidean_distance(query_vector, vectors[v]);
                        auto dist_end = std::chrono::high_resolution_clock::now();
                        distance_calculation_time += std::chrono::duration_cast<std::chrono::microseconds>(dist_end - dist_start).count();
                        
                        DataObject* candidate = new DataObject(vectors[v], leaf.keys[i]);
                        if (v < original_ids.size()) candidate->set_id(original_ids[v]);
                        
                        auto heap_start = std::chrono::high_resolution_clock::now();
                        if (knn_heap.size() < static_cast<size_t>(k)) {
                            knn_heap.push({distance, candidate});
                        } else if (distance < knn_heap.top().first) {
                            delete knn_heap.top().second;
                            knn_heap.pop();
                            knn_heap.push({distance, candidate});
                        } else {
                            delete candidate;
                        }
                        auto heap_end = std::chrono::high_resolution_clock::now();
                        heap_operation_time += std::chrono::duration_cast<std::chrono::microseconds>(heap_end - heap_start).count();
                    }
                }
                else if (leaf.keys[i] > max_key) {
                    goto extract_results;
                }
            }
            
            // Move to next leaf using read-ahead buffer
            if (!readahead_buffer.empty()) {
                current_leaf = readahead_buffer.front();
                readahead_buffer.erase(readahead_buffer.begin());
                readahead_pids.erase(readahead_pids.begin());
                currentPid = readahead_pids.empty() ? INVALID_PAGE : readahead_pids.front();
                
                if (!readahead_buffer.empty()) {
                    BPlusNode last_leaf = readahead_buffer.back();
                    next_pid = last_leaf.next;
                    
                    auto refill_start = std::chrono::high_resolution_clock::now();
                    while (readahead_buffer.size() < READAHEAD_SIZE && next_pid != INVALID_PAGE) {
                        BPlusNode ahead_leaf;
                        read(next_pid, ahead_leaf);
                        leaf_reads++;
                        readahead_buffer.push_back(ahead_leaf);
                        readahead_pids.push_back(next_pid);
                        next_pid = ahead_leaf.next;
                    }
                    auto refill_end = std::chrono::high_resolution_clock::now();
                    readahead_time += std::chrono::duration_cast<std::chrono::microseconds>(refill_end - refill_start).count();
                }
            } else {
                currentPid = INVALID_PAGE;
            }
        }
    }  // End of DISK PATH block
    
extract_results:
    auto leaf_scan_end = std::chrono::high_resolution_clock::now();
    auto leaf_scan_time = std::chrono::duration_cast<std::chrono::microseconds>(leaf_scan_end - leaf_scan_start).count();
    
    // Log detailed leaf scanning performance
    Logger::debug("Leaf scanning completed: " + std::to_string(leaf_reads) + " leaf reads, " + 
                  std::to_string(vectors_processed) + " vectors processed, " + 
                  std::to_string(leaf_scan_time) + " μs total");
    Logger::debug("  - Read-ahead I/O: " + std::to_string(readahead_time) + " μs (" + 
                  std::to_string(readahead_time * 100.0 / leaf_scan_time) + "%)");
    Logger::debug("  - Vector reconstruction: " + std::to_string(vector_reconstruction_time) + " μs (" + 
                  std::to_string(vector_reconstruction_time * 100.0 / leaf_scan_time) + "%)");
    Logger::debug("  - Distance calculation: " + std::to_string(distance_calculation_time) + " μs (" + 
                  std::to_string(distance_calculation_time * 100.0 / leaf_scan_time) + "%)");
    Logger::debug("  - Heap operations: " + std::to_string(heap_operation_time) + " μs (" + 
                  std::to_string(heap_operation_time * 100.0 / leaf_scan_time) + "%)");
    
    // Extract results from heap and reverse order (heap gives largest first, we want smallest first)
    auto extraction_start = std::chrono::high_resolution_clock::now();
    results.reserve(knn_heap.size());
    while (!knn_heap.empty()) {
        results.push_back(knn_heap.top().second);
        knn_heap.pop();
    }
    
    // Reverse to get ascending order by distance
    std::reverse(results.begin(), results.end());
    
    auto extraction_end = std::chrono::high_resolution_clock::now();
    auto extraction_time = std::chrono::duration_cast<std::chrono::microseconds>(extraction_end - extraction_start).count();
    
    auto search_end = std::chrono::high_resolution_clock::now();
    auto total_search_time = std::chrono::duration_cast<std::chrono::microseconds>(search_end - search_start).count();
    
    // Log overall search performance breakdown
    Logger::info("KNN search completed: " + std::to_string(results.size()) + " results, " + 
                 std::to_string(total_search_time) + " μs total");
    Logger::info("  - Tree traversal: " + std::to_string(traversal_time) + " μs (" + 
                 std::to_string(traversal_time * 100.0 / total_search_time) + "%)");
    Logger::info("  - Leaf scanning: " + std::to_string(leaf_scan_time) + " μs (" + 
                 std::to_string(leaf_scan_time * 100.0 / total_search_time) + "%)");
    Logger::info("  - Result extraction: " + std::to_string(extraction_time) + " μs (" + 
                 std::to_string(extraction_time * 100.0 / total_search_time) + "%)");
    
    return results;
}

// Structure to hold sub-range KNN results with distance for efficient merging
struct KNNCandidate {
    double distance;
    DataObject* obj;
    int source_thread;  // Which thread this came from
    size_t next_index;  // Next index to fetch from this thread's results
    
    // For min-heap (smallest distance first)
    bool operator>(const KNNCandidate& other) const {
        return distance > other.distance;
    }
};

std::vector<DataObject*> DiskBPlusTree::search_knn_parallel(
    const std::vector<float>& query_vector, 
    int min_key, 
    int max_key, 
    int k, 
    int num_threads,
    bool use_memory_index) {
    
    auto parallel_start = std::chrono::high_resolution_clock::now();
    std::vector<DataObject*> results;
    
    uint32_t rootPid = pm->getRoot();
    if (rootPid == INVALID_PAGE || k <= 0) {
        return results;
    }
    
    uint32_t maxVecSize = pm->getMaxVectorSize();
    int range_size = max_key - min_key + 1;
    
    Logger::debug("Parallel KNN search started: range=[" + std::to_string(min_key) + "," + 
                  std::to_string(max_key) + "], size=" + std::to_string(range_size) + 
                  ", K=" + std::to_string(k));
    
    // Determine optimal thread count
    // Rule: At least 1000 elements per thread to overcome I/O mutex overhead
    // Parallel is only beneficial for large ranges due to disk I/O serialization
    const int MIN_RANGE_PER_THREAD = 1000;
    const int MIN_TOTAL_RANGE_FOR_PARALLEL = 5000;  // Don't parallelize small ranges
    
    int hw_threads = static_cast<int>(std::thread::hardware_concurrency());
    if (hw_threads <= 0) hw_threads = 4;  // Fallback
    
    int max_useful_threads = std::max(1, range_size / MIN_RANGE_PER_THREAD);
    int actual_threads = (num_threads > 0) ? num_threads : std::min(hw_threads, max_useful_threads);
    
    Logger::debug("Thread configuration: requested=" + std::to_string(num_threads) + 
                  ", hw_threads=" + std::to_string(hw_threads) + 
                  ", max_useful=" + std::to_string(max_useful_threads) + 
                  ", actual_threads=" + std::to_string(actual_threads));
    
    // For small ranges, use single-threaded version (parallel overhead not worth it)
    if (actual_threads <= 1 || range_size < MIN_TOTAL_RANGE_FOR_PARALLEL) {
        Logger::debug("Falling back to single-threaded search (range too small or threads=1)");
        Logger::log_query("KNN_PARALLEL", "Fallback to single-threaded (range=" + std::to_string(range_size) + ", K=" + std::to_string(k) + ")", 0.0, 0);
        return search_knn_optimized(query_vector, min_key, max_key, k, use_memory_index);
    }
    
    // Divide range into sub-ranges
    std::vector<std::pair<int, int>> sub_ranges;
    int range_per_thread = range_size / actual_threads;
    int remainder = range_size % actual_threads;
    
    int current_start = min_key;
    for (int t = 0; t < actual_threads; t++) {
        int sub_range_size = range_per_thread + (t < remainder ? 1 : 0);
        int sub_end = current_start + sub_range_size - 1;
        sub_ranges.push_back({current_start, sub_end});
        current_start = sub_end + 1;
    }
    
    // Thread-local results: each thread returns sorted K candidates with distances
    std::vector<std::vector<std::pair<double, DataObject*>>> thread_results(actual_threads);
    std::vector<std::thread> threads;
    std::mutex pm_mutex;  // Protect PageManager access (file I/O)
    
    // Log actual threads being used for this query
    Logger::log_query("KNN_PARALLEL", "Threads: " + std::to_string(actual_threads) + " | Range: [" + std::to_string(min_key) + "," + std::to_string(max_key) + "] | K: " + std::to_string(k), 0.0, 0);
    
    // Worker function for each thread (Model B: retrieve all vectors from each key's list)
    auto worker = [&](int thread_id, int sub_min, int sub_max) {
        std::priority_queue<std::pair<double, DataObject*>> local_heap;
        
        uint32_t pid;
        {
            std::lock_guard<std::mutex> lock(pm_mutex);
            pid = pm->getRoot();
        }
        
        if (pid == INVALID_PAGE) return;
        
        BPlusNode diskNode;
        const BPlusNode* nodePtr = nullptr;
        
        // Navigate to leaf containing sub_min
        while (true) {
            if (use_memory_index && memory_index_loaded_) {
                nodePtr = getNodeFromMemory(pid);
            } else {
                std::lock_guard<std::mutex> lock(pm_mutex);
                read(pid, diskNode);
                nodePtr = &diskNode;
            }
            
            int i = 0;
            while (i < nodePtr->keyCount && sub_min > nodePtr->keys[i]) i++;
            
            if (nodePtr->isLeaf) break;
            pid = nodePtr->children[i];
        }
        
        // Traverse leaves in this sub-range
        uint32_t currentPid = pid;
        
        while (currentPid != INVALID_PAGE) {
            const BPlusNode* leafPtr = nullptr;
            BPlusNode diskLeaf;
            if (use_memory_index && memory_index_loaded_) {
                leafPtr = getNodeFromMemory(currentPid);
            } else {
                std::lock_guard<std::mutex> lock(pm_mutex);
                read(currentPid, diskLeaf);
                leafPtr = &diskLeaf;
            }
            if (!leafPtr) break;
            
            for (int i = 0; i < leafPtr->keyCount; i++) {
                if (leafPtr->keys[i] >= sub_min && leafPtr->keys[i] <= sub_max) {
                    // Model B: Retrieve ALL vectors for this key
                    std::vector<std::vector<float>> vectors;
                    std::vector<uint32_t> sizes;
                    std::vector<int32_t> original_ids;
                    {
                        std::lock_guard<std::mutex> lock(pm_mutex);
                        pm->getVectorStore()->retrieveVectorList(
                            leafPtr->vector_list_ids[i], 
                            leafPtr->vector_counts[i],
                            vectors, sizes, original_ids
                        );
                    }
                    
                    for (size_t v = 0; v < vectors.size(); v++) {
                        double distance = calculate_euclidean_distance(query_vector, vectors[v]);
                        
                        if (local_heap.size() >= static_cast<size_t>(k) && distance >= local_heap.top().first) {
                            continue;
                        }
                        
                        DataObject* candidate = new DataObject(vectors[v], leafPtr->keys[i]);
                        if (v < original_ids.size()) candidate->set_id(original_ids[v]);
                        
                        if (local_heap.size() < static_cast<size_t>(k)) {
                            local_heap.push({distance, candidate});
                        } else {
                            delete local_heap.top().second;
                            local_heap.pop();
                            local_heap.push({distance, candidate});
                        }
                    }
                }
                else if (leafPtr->keys[i] > sub_max) {
                    break;
                }
            }
            
            if (leafPtr->keyCount > 0 && leafPtr->keys[leafPtr->keyCount - 1] > sub_max) {
                break;
            }
            
            uint32_t nextPid = leafPtr->next;
            if (nextPid == currentPid || nextPid == INVALID_PAGE) break;
            currentPid = nextPid;
        }
        
        // Extract sorted results
        std::vector<std::pair<double, DataObject*>> sorted_results;
        sorted_results.reserve(local_heap.size());
        while (!local_heap.empty()) {
            sorted_results.push_back(local_heap.top());
            local_heap.pop();
        }
        std::reverse(sorted_results.begin(), sorted_results.end());
        
        thread_results[thread_id] = std::move(sorted_results);
    };
    
    // Launch threads
    for (int t = 0; t < actual_threads; t++) {
        threads.emplace_back(worker, t, sub_ranges[t].first, sub_ranges[t].second);
    }
    
    // Wait for all threads
    for (auto& thread : threads) {
        thread.join();
    }
    
    // K-way merge using min-heap - O(K log T) instead of O(TK log TK)
    // This is the key optimization: we don't sort all results, just merge sorted lists
    std::priority_queue<KNNCandidate, std::vector<KNNCandidate>, std::greater<KNNCandidate>> merge_heap;
    
    // Initialize merge heap with first element from each non-empty thread result
    for (int t = 0; t < actual_threads; t++) {
        if (!thread_results[t].empty()) {
            merge_heap.push({
                thread_results[t][0].first,
                thread_results[t][0].second,
                t,
                1  // Next index to fetch
            });
        }
    }
    
    // Extract K smallest elements
    results.reserve(k);
    while (results.size() < static_cast<size_t>(k) && !merge_heap.empty()) {
        KNNCandidate best = merge_heap.top();
        merge_heap.pop();
        
        results.push_back(best.obj);
        
        // Push next element from same thread's results
        if (best.next_index < thread_results[best.source_thread].size()) {
            auto& next = thread_results[best.source_thread][best.next_index];
            merge_heap.push({
                next.first,
                next.second,
                best.source_thread,
                best.next_index + 1
            });
        }
    }
    
    // Clean up remaining objects that weren't selected
    for (int t = 0; t < actual_threads; t++) {
        for (size_t i = 0; i < thread_results[t].size(); i++) {
            DataObject* obj = thread_results[t][i].second;
            // Check if this object was already added to results
            bool in_results = false;
            for (DataObject* r : results) {
                if (r == obj) {
                    in_results = true;
                    break;
                }
            }
            if (!in_results) {
                delete obj;
            }
        }
    }
    
    return results;
}

int DiskBPlusTree::get_min_keys() {
    return (static_cast<int>(pm->getOrder()) - 1) / 2;
}
