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

DiskBPlusTree::DiskBPlusTree(const std::string& filename)
    : pm(std::make_unique<PageManager>(filename)) {}

// Constructor for creating new index with specified config
DiskBPlusTree::DiskBPlusTree(const std::string& filename, const BPTreeConfig& config)
    : pm(std::make_unique<PageManager>(filename, config)) {}

void DiskBPlusTree::read(uint32_t pid, BPlusNode& node) {
    pm->readNode(pid, node);
}

void DiskBPlusTree::write(uint32_t pid, const BPlusNode& node) {
    pm->writeNode(pid, node);
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

    uint32_t maxVecSize = pm->getMaxVectorSize();
    for (int i = 0; i < newLeaf.keyCount; i++) {
        newLeaf.keys[i] = leaf.keys[mid + i];
        newLeaf.vector_sizes[i] = leaf.vector_sizes[mid + i];
        for (int j = 0; j < leaf.vector_sizes[mid + i] && j < static_cast<int>(maxVecSize); j++) {
            newLeaf.data_vectors[i][j] = leaf.data_vectors[mid + i][j];
        }
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
    uint32_t maxVecSize = pm->getMaxVectorSize();

    if (rootPid == INVALID_PAGE) {
        // Create first leaf node as root
        BPlusNode root = createNode();
        root.isLeaf = true;
        root.keyCount = 1;
        root.keys[0] = key;
        root.next = INVALID_PAGE;
        
        // Copy vector data
        const std::vector<float>& vec = obj.get_vector();
        root.vector_sizes[0] = static_cast<int>(vec.size());
        for (size_t j = 0; j < vec.size() && j < maxVecSize; j++) {
            root.data_vectors[0][j] = vec[j];
        }

        uint32_t pid = pm->allocatePage();
        write(pid, root);
        pm->setRoot(pid);
        return;
    }

    // Find the leaf node where key should be inserted
    std::vector<uint32_t> path;
    std::vector<int> pathIndex; // Which child index was taken at each level
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

    // Insert key into leaf node
    int i = node.keyCount - 1;
    while (i >= 0 && node.keys[i] > key) {
        node.keys[i + 1] = node.keys[i];
        node.vector_sizes[i + 1] = node.vector_sizes[i];
        for (int j = 0; j < node.vector_sizes[i] && j < static_cast<int>(maxVecSize); j++) {
            node.data_vectors[i + 1][j] = node.data_vectors[i][j];
        }
        i--;
    }
    node.keys[i + 1] = key;
    
    const std::vector<float>& vec = obj.get_vector();
    node.vector_sizes[i + 1] = static_cast<int>(vec.size());
    for (size_t j = 0; j < vec.size() && j < maxVecSize; j++) {
        node.data_vectors[i + 1][j] = vec[j];
    }
    
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
        promotedKey = parent.keys[mid]; // This key goes up
        
        // Move keys after mid to new node
        newInternal.keyCount = parent.keyCount - mid - 1;
        for (int k = 0; k < newInternal.keyCount; k++) {
            newInternal.keys[k] = parent.keys[mid + 1 + k];
        }
        
        // Move children after mid to new node
        for (int k = 0; k <= newInternal.keyCount; k++) {
            newInternal.children[k] = parent.children[mid + 1 + k];
        }
        
        parent.keyCount = mid;

        uint32_t newInternalPid = pm->allocatePage();
        write(parentPid, parent);
        write(newInternalPid, newInternal);
        
        childPid = newInternalPid;
        // promotedKey is already set above
    }

    // If we get here, root needs to split
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

DataObject* DiskBPlusTree::search_data_object(const DataObject& obj) {
    int key;
    if (obj.is_int_value()) {
        key = obj.get_int_value();
    } else {
        key = static_cast<int>(obj.get_float_value());
    }
    
    uint32_t pid = pm->getRoot();
    if (pid == INVALID_PAGE) return nullptr;
    
    uint32_t maxVecSize = pm->getMaxVectorSize();
    BPlusNode node;
    
    while (true) {
        read(pid, node);
        
        int i = 0;
        while (i < node.keyCount && key > node.keys[i]) i++;
        
        if (node.isLeaf) break;
        pid = node.children[i];
    }
    
    for (int i = 0; i < node.keyCount; i++) {
        if (node.keys[i] == key) {
            // Reconstruct DataObject from dynamic array
            std::vector<float> vec(node.vector_sizes[i]);
            for (int j = 0; j < node.vector_sizes[i] && j < static_cast<int>(maxVecSize); j++) {
                vec[j] = node.data_vectors[i][j];
            }
            DataObject* result = new DataObject(vec, key);
            return result;
        }
    }
    
    return nullptr;
}

DataObject* DiskBPlusTree::search_data_object(int key) {
    uint32_t pid = pm->getRoot();
    if (pid == INVALID_PAGE) return nullptr;
    
    uint32_t maxVecSize = pm->getMaxVectorSize();
    BPlusNode node;
    
    // Navigate to the leaf that should contain the key
    while (true) {
        read(pid, node);
        
        int i = 0;
        while (i < node.keyCount && key > node.keys[i]) i++;
        
        if (node.isLeaf) break;
        pid = node.children[i];
    }
    
    // Search through leaf nodes (following next pointers if needed)
    // This handles cases where separator keys may not perfectly guide to the right leaf
    uint32_t currentPid = pid;
    while (currentPid != INVALID_PAGE) {
        BPlusNode leaf;
        read(currentPid, leaf);
        
        for (int i = 0; i < leaf.keyCount; i++) {
            if (leaf.keys[i] == key) {
                // Reconstruct DataObject from dynamic array
                std::vector<float> vec(leaf.vector_sizes[i]);
                for (int j = 0; j < leaf.vector_sizes[i] && j < static_cast<int>(maxVecSize); j++) {
                    vec[j] = leaf.data_vectors[i][j];
                }
                DataObject* result = new DataObject(vec, key);
                return result;
            }
            // If we've passed the key, it doesn't exist
            if (leaf.keys[i] > key) {
                return nullptr;
            }
        }
        
        currentPid = leaf.next;
    }
    
    return nullptr;
}

DataObject* DiskBPlusTree::search_data_object(float key) {
    return search_data_object(static_cast<int>(key));
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

bool DiskBPlusTree::vectorsMatch(const std::vector<float>& v1, const BPlusNode& node, int idx, uint32_t maxVecSize) {
    int vecSize = node.vector_sizes[idx];
    if (static_cast<int>(v1.size()) != vecSize) {
        return false;
    }
    for (int j = 0; j < vecSize && j < static_cast<int>(maxVecSize); j++) {
        // Use larger epsilon for floating point comparison (handles command-line parsing precision)
        if (std::abs(v1[j] - node.data_vectors[idx][j]) > 1e-3f) {
            return false;
        }
    }
    return true;
}

bool DiskBPlusTree::deleteDataObject(int key, const std::vector<float>& vector) {
    uint32_t rootPid = pm->getRoot();
    if (rootPid == INVALID_PAGE) {
        return false;
    }
    
    uint32_t order = pm->getOrder();
    uint32_t maxVecSize = pm->getMaxVectorSize();
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
    
    // Find the specific entry matching both key AND vector
    int keyIndex = -1;
    uint32_t currentPid = pid;
    
    while (currentPid != INVALID_PAGE) {
        read(currentPid, node);
        
        for (int i = 0; i < node.keyCount; i++) {
            if (node.keys[i] == key && vectorsMatch(vector, node, i, maxVecSize)) {
                keyIndex = i;
                pid = currentPid;
                
                // Rebuild path to this leaf
                path.clear();
                pathIndex.clear();
                uint32_t tempPid = rootPid;
                BPlusNode tempNode;
                
                while (true) {
                    read(tempPid, tempNode);
                    path.push_back(tempPid);
                    
                    int j = 0;
                    while (j < tempNode.keyCount && key > tempNode.keys[j]) {
                        j++;
                    }
                    pathIndex.push_back(j);
                    
                    if (tempNode.isLeaf) {
                        break;
                    }
                    tempPid = tempNode.children[j];
                }
                
                // Re-read the correct leaf
                read(pid, node);
                goto found;
            }
            if (node.keys[i] > key) {
                return false; // Passed the key, not found
            }
        }
        currentPid = node.next;
    }
    
    return false; // Not found
    
found:
    // Remove the entry by shifting elements left
    for (int i = keyIndex; i < node.keyCount - 1; i++) {
        node.keys[i] = node.keys[i + 1];
        node.vector_sizes[i] = node.vector_sizes[i + 1];
        for (uint32_t j = 0; j < maxVecSize; j++) {
            node.data_vectors[i][j] = node.data_vectors[i + 1][j];
        }
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
        return false; // Tree is empty
    }
    
    uint32_t order = pm->getOrder();
    uint32_t maxVecSize = pm->getMaxVectorSize();
    int minKeys = (order - 1) / 2; // Minimum keys for non-root nodes
    
    // Find the leaf node containing the key, keeping track of the path
    std::vector<uint32_t> path;
    std::vector<int> pathIndex; // Which child index was taken at each level
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
    
    // Find the key in the leaf node (may need to traverse to next leaf)
    int keyIndex = -1;
    for (int i = 0; i < node.keyCount; i++) {
        if (node.keys[i] == key) {
            keyIndex = i;
            break;
        }
    }
    
    // If not found in this leaf, check next leaves (separator keys may be stale)
    while (keyIndex == -1 && node.next != INVALID_PAGE) {
        // Check if we've passed the key
        if (node.keyCount > 0 && node.keys[node.keyCount - 1] > key) {
            break; // Key doesn't exist
        }
        
        // Move to next leaf and rebuild path
        uint32_t nextPid = node.next;
        
        // We need to rebuild the path to this new leaf
        // Start fresh from root
        path.clear();
        pathIndex.clear();
        pid = rootPid;
        
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
        
        // Now search in this leaf
        for (int i = 0; i < node.keyCount; i++) {
            if (node.keys[i] == key) {
                keyIndex = i;
                break;
            }
        }
        
        // If still not found and we're at the same leaf, break to avoid infinite loop
        if (pid == nextPid || node.next == INVALID_PAGE) {
            break;
        }
    }
    
    if (keyIndex == -1) {
        return false; // Key not found
    }
    
    // Check if we're deleting the first key in the leaf (need to update parent separators)
    bool deletingFirstKey = (keyIndex == 0 && node.keyCount > 1);
    int newFirstKey = deletingFirstKey ? node.keys[1] : 0;
    
    // Remove the key by shifting elements left
    for (int i = keyIndex; i < node.keyCount - 1; i++) {
        node.keys[i] = node.keys[i + 1];
        node.vector_sizes[i] = node.vector_sizes[i + 1];
        for (uint32_t j = 0; j < maxVecSize; j++) {
            node.data_vectors[i][j] = node.data_vectors[i + 1][j];
        }
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
    
    uint32_t maxVecSize = pm->getMaxVectorSize();
    int minKeys = (pm->getOrder() - 1) / 2;
    
    // Check if left sibling has enough keys to lend
    if (leftSibling.keyCount <= minKeys) {
        return false;
    }
    
    if (node.isLeaf) {
        // Shift all keys in current node right
        for (int i = node.keyCount; i > 0; i--) {
            node.keys[i] = node.keys[i - 1];
            node.vector_sizes[i] = node.vector_sizes[i - 1];
            for (uint32_t j = 0; j < maxVecSize; j++) {
                node.data_vectors[i][j] = node.data_vectors[i - 1][j];
            }
        }
        
        // Move last key from left sibling to current node
        int lastIdx = leftSibling.keyCount - 1;
        node.keys[0] = leftSibling.keys[lastIdx];
        node.vector_sizes[0] = leftSibling.vector_sizes[lastIdx];
        for (uint32_t j = 0; j < maxVecSize; j++) {
            node.data_vectors[0][j] = leftSibling.data_vectors[lastIdx][j];
        }
        node.keyCount++;
        leftSibling.keyCount--;
        
        // Update parent key to be the new first key of current node
        parent.keys[childIdx - 1] = node.keys[0];
    } else {
        // Internal node borrowing
        // Shift all keys and children in current node right
        for (int i = node.keyCount; i > 0; i--) {
            node.keys[i] = node.keys[i - 1];
        }
        for (int i = node.keyCount + 1; i > 0; i--) {
            node.children[i] = node.children[i - 1];
        }
        
        // Move parent key down to current node
        node.keys[0] = parent.keys[childIdx - 1];
        
        // Move last child from left sibling to current node
        node.children[0] = leftSibling.children[leftSibling.keyCount];
        node.keyCount++;
        
        // Move last key from left sibling up to parent
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
    
    uint32_t maxVecSize = pm->getMaxVectorSize();
    int minKeys = (pm->getOrder() - 1) / 2;
    
    // Check if right sibling has enough keys to lend
    if (rightSibling.keyCount <= minKeys) {
        return false;
    }
    
    if (node.isLeaf) {
        // Move first key from right sibling to end of current node
        node.keys[node.keyCount] = rightSibling.keys[0];
        node.vector_sizes[node.keyCount] = rightSibling.vector_sizes[0];
        for (uint32_t j = 0; j < maxVecSize; j++) {
            node.data_vectors[node.keyCount][j] = rightSibling.data_vectors[0][j];
        }
        node.keyCount++;
        
        // Shift all keys in right sibling left
        for (int i = 0; i < rightSibling.keyCount - 1; i++) {
            rightSibling.keys[i] = rightSibling.keys[i + 1];
            rightSibling.vector_sizes[i] = rightSibling.vector_sizes[i + 1];
            for (uint32_t j = 0; j < maxVecSize; j++) {
                rightSibling.data_vectors[i][j] = rightSibling.data_vectors[i + 1][j];
            }
        }
        rightSibling.keyCount--;
        
        // Update parent key to be the new first key of right sibling
        parent.keys[childIdx] = rightSibling.keys[0];
    } else {
        // Internal node borrowing
        // Move parent key down to end of current node
        node.keys[node.keyCount] = parent.keys[childIdx];
        
        // Move first child from right sibling to current node
        node.children[node.keyCount + 1] = rightSibling.children[0];
        node.keyCount++;
        
        // Move first key from right sibling up to parent
        parent.keys[childIdx] = rightSibling.keys[0];
        
        // Shift all keys and children in right sibling left
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
    
    uint32_t maxVecSize = pm->getMaxVectorSize();
    
    if (node.isLeaf) {
        // Move all keys from current node to left sibling
        for (int i = 0; i < node.keyCount; i++) {
            leftSibling.keys[leftSibling.keyCount + i] = node.keys[i];
            leftSibling.vector_sizes[leftSibling.keyCount + i] = node.vector_sizes[i];
            for (uint32_t j = 0; j < maxVecSize; j++) {
                leftSibling.data_vectors[leftSibling.keyCount + i][j] = node.data_vectors[i][j];
            }
        }
        leftSibling.keyCount += node.keyCount;
        
        // Update next pointer
        leftSibling.next = node.next;
    } else {
        // Internal node merge - bring down parent key
        leftSibling.keys[leftSibling.keyCount] = parent.keys[childIdx - 1];
        leftSibling.keyCount++;
        
        // Move all keys and children from current node to left sibling
        for (int i = 0; i < node.keyCount; i++) {
            leftSibling.keys[leftSibling.keyCount + i] = node.keys[i];
        }
        for (int i = 0; i <= node.keyCount; i++) {
            leftSibling.children[leftSibling.keyCount + i] = node.children[i];
        }
        leftSibling.keyCount += node.keyCount;
    }
    
    write(leftPid, leftSibling);
    
    // Remove the key and child pointer from parent
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
    
    uint32_t maxVecSize = pm->getMaxVectorSize();
    
    if (node.isLeaf) {
        // Move all keys from right sibling to current node
        for (int i = 0; i < rightSibling.keyCount; i++) {
            node.keys[node.keyCount + i] = rightSibling.keys[i];
            node.vector_sizes[node.keyCount + i] = rightSibling.vector_sizes[i];
            for (uint32_t j = 0; j < maxVecSize; j++) {
                node.data_vectors[node.keyCount + i][j] = rightSibling.data_vectors[i][j];
            }
        }
        node.keyCount += rightSibling.keyCount;
        
        // Update next pointer
        node.next = rightSibling.next;
    } else {
        // Internal node merge - bring down parent key
        node.keys[node.keyCount] = parent.keys[childIdx];
        node.keyCount++;
        
        // Move all keys and children from right sibling to current node
        for (int i = 0; i < rightSibling.keyCount; i++) {
            node.keys[node.keyCount + i] = rightSibling.keys[i];
        }
        for (int i = 0; i <= rightSibling.keyCount; i++) {
            node.children[node.keyCount + i] = rightSibling.children[i];
        }
        node.keyCount += rightSibling.keyCount;
    }
    
    write(nodePid, node);
    
    // Remove the key and child pointer from parent
    for (int i = childIdx; i < parent.keyCount - 1; i++) {
        parent.keys[i] = parent.keys[i + 1];
    }
    for (int i = childIdx + 1; i < parent.keyCount + 1; i++) {
        parent.children[i] = parent.children[i + 1];
    }
    parent.keyCount--;
}

std::vector<DataObject*> DiskBPlusTree::search_range(int min_key, int max_key) {
    std::vector<DataObject*> results;
    
    uint32_t pid = pm->getRoot();
    if (pid == INVALID_PAGE) {
        return results;
    }
    
    uint32_t maxVecSize = pm->getMaxVectorSize();
    
    // Find leaf node that contains min_key
    BPlusNode node;
    
    while (true) {
        read(pid, node);
        
        int i = 0;
        while (i < node.keyCount && min_key > node.keys[i]) i++;
        
        if (node.isLeaf) {
            break;
        }
        pid = node.children[i];
    }
    
    // Collect data from this leaf and subsequent leaves until max_key is reached
    uint32_t currentPid = pid;
    
    while (currentPid != INVALID_PAGE) {
        BPlusNode leaf;
        read(currentPid, leaf);
                
        for (int i = 0; i < leaf.keyCount; i++) {
            if (leaf.keys[i] >= min_key && leaf.keys[i] <= max_key) {
                std::vector<float> vec(leaf.vector_sizes[i]);
                for (int j = 0; j < leaf.vector_sizes[i] && j < static_cast<int>(maxVecSize); j++) {
                    vec[j] = leaf.data_vectors[i][j];
                }
                DataObject* result = new DataObject(vec, leaf.keys[i]);
                results.push_back(result);
            }
            else if (leaf.keys[i] > max_key) {
                return results; // We've passed the range
            }
        }
        
        uint32_t nextPid = leaf.next;
        
        if (nextPid == currentPid) {
            break;  // Circular reference protection
        }
        
        currentPid = nextPid; // Move to next leaf
    }
    
    return results;
}

std::vector<DataObject*> DiskBPlusTree::search_range(float min_key, float max_key) {
    return search_range(static_cast<int>(min_key), static_cast<int>(max_key));
}

bool DiskBPlusTree::search(const DataObject& obj) {
    int key;
    if (obj.is_int_value()) {
        key = obj.get_int_value();
    } else {
        key = static_cast<int>(obj.get_float_value());
    }
    
    uint32_t pid = pm->getRoot();
    if (pid == INVALID_PAGE) return false;
    
    BPlusNode node;
    
    while (true) {
        read(pid, node);
        
        int i = 0;
        while (i < node.keyCount && key > node.keys[i]) i++;
        
        if (node.isLeaf) break;
        pid = node.children[i];
    }
    
    for (int i = 0; i < node.keyCount; i++) {
        if (node.keys[i] == key) {
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
    
    uint32_t maxVecSize = pm->getMaxVectorSize();
    std::string indent(level * 2, ' ');
    std::cout << indent << "Node " << pid << " (";
    if (node.isLeaf) std::cout << "leaf";
    else std::cout << "internal";
    std::cout << ", keys=" << node.keyCount << "): [";
    
    for (int i = 0; i < node.keyCount; i++) {
        if (i > 0) std::cout << ", ";
        std::cout << node.keys[i];
        if (node.isLeaf && node.vector_sizes[i] > 0) {
            std::cout << "([";
            for (int j = 0; j < node.vector_sizes[i] && j < static_cast<int>(maxVecSize); j++) {
                if (j > 0) std::cout << ",";
                std::cout << node.data_vectors[i][j];
            }
            std::cout << "])";
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

std::vector<DataObject*> DiskBPlusTree::search_knn_optimized(const std::vector<float>& query_vector, int min_key, int max_key, int k) {
    auto search_start = std::chrono::high_resolution_clock::now();
    std::vector<DataObject*> results;
    
    uint32_t pid = pm->getRoot();
    if (pid == INVALID_PAGE || k <= 0) {
        return results;
    }
    
    uint32_t maxVecSize = pm->getMaxVectorSize();
    
    // Priority queue to maintain K nearest neighbors (max-heap by distance)
    // pair<distance, DataObject*> - largest distance at top
    std::priority_queue<std::pair<double, DataObject*>> knn_heap;
    
    // Find leaf node that contains min_key
    auto traversal_start = std::chrono::high_resolution_clock::now();
    BPlusNode node;
    int tree_reads = 0;
    
    while (true) {
        read(pid, node);
        tree_reads++;
        
        int i = 0;
        while (i < node.keyCount && min_key > node.keys[i]) i++;
        
        if (node.isLeaf) {
            break;
        }
        pid = node.children[i];
    }
    
    auto traversal_end = std::chrono::high_resolution_clock::now();
    auto traversal_time = std::chrono::duration_cast<std::chrono::microseconds>(traversal_end - traversal_start).count();
    Logger::debug("Tree traversal completed: " + std::to_string(tree_reads) + " node reads, " + std::to_string(traversal_time) + " μs");
    
    // Traverse leaves and maintain K best candidates with read-ahead and batch I/O
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
    
    // Read-ahead buffer: prefetch next leaves while processing current one
    const int READAHEAD_SIZE = 3;  // Prefetch 3 leaves ahead
    std::vector<BPlusNode> readahead_buffer;
    std::vector<uint32_t> readahead_pids;
    
    // Initial batch read: load first leaf + readahead leaves
    auto batch_read_start = std::chrono::high_resolution_clock::now();
    BPlusNode current_leaf;
    read(currentPid, current_leaf);
    leaf_reads++;
    
    // Batch read next READAHEAD_SIZE leaves
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
        BPlusNode leaf = current_leaf;
                
        for (int i = 0; i < leaf.keyCount; i++) {
            if (leaf.keys[i] >= min_key && leaf.keys[i] <= max_key) {
                vectors_processed++;
                keys_processed++;
                
                // Progress logging every 10% or every 10000 keys for large ranges
                int progress_percent = (keys_processed * 100) / range_size;
                bool should_log = false;
                
                // Log at 10% intervals
                if (progress_percent != last_progress_percent && progress_percent % 10 == 0) {
                    should_log = true;
                }
                // Log every 10000 keys for large ranges, or every 1000 keys for smaller ranges
                else if (range_size > 50000 && keys_processed % 10000 == 0) {
                    should_log = true;
                }
                else if (range_size <= 50000 && keys_processed % 1000 == 0) {
                    should_log = true;
                }
                
                if (should_log) {
                    auto current_time = std::chrono::high_resolution_clock::now();
                    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(current_time - last_progress_time).count();
                    Logger::info("Search progress: " + std::to_string(progress_percent) + "% (" + 
                                std::to_string(keys_processed) + "/" + std::to_string(range_size) + " keys) | " +
                                std::to_string(vectors_processed) + " vectors processed | " +
                                std::to_string(elapsed) + "ms elapsed");
                    last_progress_percent = progress_percent;
                    last_progress_time = current_time;
                }
                
                // Reconstruct vector from leaf node
                auto vec_start = std::chrono::high_resolution_clock::now();
                std::vector<float> vec(leaf.vector_sizes[i]);
                for (int j = 0; j < leaf.vector_sizes[i] && j < static_cast<int>(maxVecSize); j++) {
                    vec[j] = leaf.data_vectors[i][j];
                }
                auto vec_end = std::chrono::high_resolution_clock::now();
                vector_reconstruction_time += std::chrono::duration_cast<std::chrono::microseconds>(vec_end - vec_start).count();
                
                // Calculate distance to query vector
                auto dist_start = std::chrono::high_resolution_clock::now();
                double distance = calculate_euclidean_distance(query_vector, vec);
                auto dist_end = std::chrono::high_resolution_clock::now();
                distance_calculation_time += std::chrono::duration_cast<std::chrono::microseconds>(dist_end - dist_start).count();
                
                // Create DataObject for this candidate
                DataObject* candidate = new DataObject(vec, leaf.keys[i]);
                
                // Heap operations
                auto heap_start = std::chrono::high_resolution_clock::now();
                if (knn_heap.size() < static_cast<size_t>(k)) {
                    // Heap not full, add candidate
                    knn_heap.push({distance, candidate});
                } else if (distance < knn_heap.top().first) {
                    // Found better candidate, replace worst one
                    delete knn_heap.top().second;  // Clean up old worst
                    knn_heap.pop();
                    knn_heap.push({distance, candidate});
                } else {
                    // Candidate is worse than current K-th best, discard
                    delete candidate;
                }
                auto heap_end = std::chrono::high_resolution_clock::now();
                heap_operation_time += std::chrono::duration_cast<std::chrono::microseconds>(heap_end - heap_start).count();
            }
            else if (leaf.keys[i] > max_key) {
                // We've passed the range, extract results and return
                goto extract_results;
            }
        }
        
        // Move to next leaf using read-ahead buffer
        if (!readahead_buffer.empty()) {
            // Use prefetched leaf from buffer
            current_leaf = readahead_buffer.front();
            readahead_buffer.erase(readahead_buffer.begin());
            readahead_pids.erase(readahead_pids.begin());
            currentPid = readahead_pids.empty() ? INVALID_PAGE : readahead_pids.front();
            
            // Refill read-ahead buffer if needed
            if (!readahead_buffer.empty()) {
                uint32_t last_pid = readahead_pids.back();
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
            // No more prefetched leaves
            currentPid = INVALID_PAGE;
        }
    }
    
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
    int num_threads) {
    
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
        return search_knn_optimized(query_vector, min_key, max_key, k);
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
    
    // Worker function for each thread
    auto worker = [&](int thread_id, int sub_min, int sub_max) {
        std::priority_queue<std::pair<double, DataObject*>> local_heap;
        
        // Find starting leaf for this sub-range
        uint32_t pid;
        {
            std::lock_guard<std::mutex> lock(pm_mutex);
            pid = pm->getRoot();
        }
        
        if (pid == INVALID_PAGE) return;
        
        BPlusNode node;
        
        // Navigate to leaf containing sub_min
        while (true) {
            {
                std::lock_guard<std::mutex> lock(pm_mutex);
                read(pid, node);
            }
            
            int i = 0;
            while (i < node.keyCount && sub_min > node.keys[i]) i++;
            
            if (node.isLeaf) break;
            pid = node.children[i];
        }
        
        // Traverse leaves in this sub-range
        uint32_t currentPid = pid;
        
        while (currentPid != INVALID_PAGE) {
            BPlusNode leaf;
            {
                std::lock_guard<std::mutex> lock(pm_mutex);
                read(currentPid, leaf);
            }
            
            for (int i = 0; i < leaf.keyCount; i++) {
                if (leaf.keys[i] >= sub_min && leaf.keys[i] <= sub_max) {
                    // Reconstruct vector
                    std::vector<float> vec(leaf.vector_sizes[i]);
                    for (int j = 0; j < leaf.vector_sizes[i] && j < static_cast<int>(maxVecSize); j++) {
                        vec[j] = leaf.data_vectors[i][j];
                    }
                    
                    double distance = calculate_euclidean_distance(query_vector, vec);
                    
                    // Early pruning: if heap is full and this is worse than K-th best, skip
                    if (local_heap.size() >= static_cast<size_t>(k) && distance >= local_heap.top().first) {
                        continue;
                    }
                    
                    DataObject* candidate = new DataObject(vec, leaf.keys[i]);
                    
                    if (local_heap.size() < static_cast<size_t>(k)) {
                        local_heap.push({distance, candidate});
                    } else {
                        delete local_heap.top().second;
                        local_heap.pop();
                        local_heap.push({distance, candidate});
                    }
                }
                else if (leaf.keys[i] > sub_max) {
                    break;  // Done with this sub-range
                }
            }
            
            // Check if we've exceeded sub-range
            if (leaf.keyCount > 0 && leaf.keys[leaf.keyCount - 1] > sub_max) {
                break;
            }
            
            uint32_t nextPid = leaf.next;
            if (nextPid == currentPid || nextPid == INVALID_PAGE) break;
            currentPid = nextPid;
        }
        
        // Extract sorted results (smallest distance first)
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
