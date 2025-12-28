#include "bplustree_disk.h"
#include <vector>
#include <iostream>

template <typename T>
DiskBPlusTree<T>::DiskBPlusTree(const std::string& filename)
    : pm(filename) {}

template <typename T>
void DiskBPlusTree<T>::read(uint32_t pid, BPlusNode<T>& node) {
    pm.readPage(pid, &node);
}

template <typename T>
void DiskBPlusTree<T>::write(uint32_t pid, const BPlusNode<T>& node) {
    pm.writePage(pid, &node);
}

template <typename T>
void DiskBPlusTree<T>::splitLeaf(uint32_t leafPid,
                                 BPlusNode<T>& leaf,
                                 T& promotedKey,
                                 uint32_t& newLeafPid) {
    BPlusNode<T> newLeaf{};
    newLeaf.isLeaf = true;

    int mid = leaf.keyCount / 2;
    newLeaf.keyCount = leaf.keyCount - mid;

    for (int i = 0; i < newLeaf.keyCount; i++)
        newLeaf.keys[i] = leaf.keys[mid + i];

    leaf.keyCount = mid;

    newLeaf.next = leaf.next;
    newLeafPid = pm.allocatePage();
    leaf.next = newLeafPid;

    promotedKey = newLeaf.keys[0];

    write(leafPid, leaf);
    write(newLeafPid, newLeaf);
}

template <typename T>
void DiskBPlusTree<T>::insert(const T& key) {
    uint32_t rootPid = pm.getRoot();

    if (rootPid == INVALID_PAGE) {
        BPlusNode<T> root{};
        root.isLeaf = true;
        root.keyCount = 1;
        root.keys[0] = key;
        root.next = INVALID_PAGE;

        uint32_t pid = pm.allocatePage();
        write(pid, root);
        pm.setRoot(pid);
        return;
    }

    std::vector<uint32_t> path;
    uint32_t pid = rootPid;
    BPlusNode<T> node;

    while (true) {
        read(pid, node);
        path.push_back(pid);

        if (node.isLeaf) break;

        int i = 0;
        while (i < node.keyCount && key >= node.keys[i]) i++;
        pid = node.children[i];
    }

    int i = node.keyCount - 1;
    while (i >= 0 && node.keys[i] > key) {
        node.keys[i + 1] = node.keys[i];
        i--;
    }
    node.keys[i + 1] = key;
    node.keyCount++;

    if (node.keyCount < ORDER) {
        write(pid, node);
        return;
    }

    T promoted;
    uint32_t newLeafPid;
    splitLeaf(pid, node, promoted, newLeafPid);

    for (int level = path.size() - 2; level >= 0; level--) {
        uint32_t parentPid = path[level];
        BPlusNode<T> parent;
        read(parentPid, parent);

        int j = parent.keyCount - 1;
        while (j >= 0 && parent.keys[j] > promoted) {
            parent.keys[j + 1] = parent.keys[j];
            parent.children[j + 2] = parent.children[j + 1];
            j--;
        }

        parent.keys[j + 1] = promoted;
        parent.children[j + 2] = newLeafPid;
        parent.keyCount++;

        write(parentPid, parent);
        return;
    }

    BPlusNode<T> newRoot{};
    newRoot.isLeaf = false;
    newRoot.keyCount = 1;
    newRoot.keys[0] = promoted;
    newRoot.children[0] = rootPid;
    newRoot.children[1] = newLeafPid;

    uint32_t newRootPid = pm.allocatePage();
    write(newRootPid, newRoot);
    pm.setRoot(newRootPid);
}

template <typename T>
bool DiskBPlusTree<T>::search(const T& key) {
    uint32_t pid = pm.getRoot();
    if (pid == INVALID_PAGE) return false;
    
    BPlusNode<T> node;
    
    while (true) {
        read(pid, node);
        
        int i = 0;
        while (i < node.keyCount && key >= node.keys[i]) i++;
        
        if (node.isLeaf) break;
        pid = node.children[i];
    }
    
    for (int i = 0; i < node.keyCount; i++)
        if (node.keys[i] == key)
            return true;
    
    return false;
}

template <typename T>
bool DiskBPlusTree<T>::delete_key(const T& key) {
    uint32_t rootPid = pm.getRoot();
    if (rootPid == INVALID_PAGE) return false;
    
    // Find the leaf containing the key
    std::vector<uint32_t> path;
    uint32_t pid = rootPid;
    BPlusNode<T> node;
    
    while (true) {
        read(pid, node);
        path.push_back(pid);
        
        if (node.isLeaf) break;
        
        int i = 0;
        while (i < node.keyCount && key >= node.keys[i]) i++;
        pid = node.children[i];
    }
    
    // Delete from leaf
    bool deleted = delete_from_leaf(pid, key);
    if (!deleted) return false;
    
    // Check if leaf underflows and handle redistribution/merging
    read(pid, node);
    if (node.keyCount < get_min_keys() && path.size() > 1) {
        uint32_t parentPid = path[path.size() - 2];
        int childIndex = 0;
        BPlusNode<T> parent;
        read(parentPid, parent);
        
        // Find child index in parent
        for (int i = 0; i <= parent.keyCount; i++) {
            if (parent.children[i] == pid) {
                childIndex = i;
                break;
            }
        }
        
        borrow_or_merge(parentPid, childIndex, pid);
    }
    
    return true;
}

template <typename T>
void DiskBPlusTree<T>::print_tree() {
    uint32_t rootPid = pm.getRoot();
    if (rootPid == INVALID_PAGE) {
        std::cout << "Tree is empty" << std::endl;
        return;
    }
    std::cout << "B+ Tree structure:" << std::endl;
    print_tree_recursive(rootPid, 0);
}

template <typename T>
void DiskBPlusTree<T>::print_tree_recursive(uint32_t pid, int level) {
    BPlusNode<T> node;
    read(pid, node);
    
    // Print indentation for level
    for (int i = 0; i < level; i++) {
        std::cout << "  ";
    }
    
    std::cout << "Level " << level << " (Page " << pid << "): ";
    
    if (node.isLeaf) {
        std::cout << "[LEAF] Keys: ";
        for (int i = 0; i < node.keyCount; i++) {
            std::cout << node.keys[i];
            if (i < node.keyCount - 1) std::cout << ", ";
        }
        std::cout << " -> Next: " << node.next << std::endl;
    } else {
        std::cout << "[INTERNAL] Keys: ";
        for (int i = 0; i < node.keyCount; i++) {
            std::cout << node.keys[i];
            if (i < node.keyCount - 1) std::cout << ", ";
        }
        std::cout << std::endl;
        
        // Recursively print children
        for (int i = 0; i <= node.keyCount; i++) {
            print_tree_recursive(node.children[i], level + 1);
        }
    }
}

template <typename T>
int DiskBPlusTree<T>::get_min_keys() {
    return (ORDER - 1) / 2;
}

template <typename T>
bool DiskBPlusTree<T>::delete_from_leaf(uint32_t leafPid, const T& key) {
    BPlusNode<T> leaf;
    read(leafPid, leaf);
    
    int i = 0;
    while (i < leaf.keyCount && leaf.keys[i] != key) i++;
    
    if (i >= leaf.keyCount) return false; // Key not found
    
    // Remove key by shifting remaining keys
    for (int j = i; j < leaf.keyCount - 1; j++) {
        leaf.keys[j] = leaf.keys[j + 1];
    }
    leaf.keyCount--;
    write(leafPid, leaf);
    return true;
}

template <typename T>
bool DiskBPlusTree<T>::borrow_or_merge(uint32_t parentPid, int childIndex, uint32_t childPid) {
    BPlusNode<T> parent, child;
    read(parentPid, parent);
    read(childPid, child);
    
    // Try to borrow from left sibling
    if (childIndex > 0) {
        uint32_t leftPid = parent.children[childIndex - 1];
        BPlusNode<T> leftSibling;
        read(leftPid, leftSibling);
        
        if (leftSibling.keyCount > get_min_keys()) {
            redistribute_keys(leftPid, childPid, parentPid, childIndex - 1);
            return true;
        }
    }
    
    // Try to borrow from right sibling
    if (childIndex < parent.keyCount) {
        uint32_t rightPid = parent.children[childIndex + 1];
        BPlusNode<T> rightSibling;
        read(rightPid, rightSibling);
        
        if (rightSibling.keyCount > get_min_keys()) {
            redistribute_keys(childPid, rightPid, parentPid, childIndex);
            return true;
        }
    }
    
    // Merge with sibling
    if (childIndex > 0) {
        // Merge with left sibling
        uint32_t leftPid = parent.children[childIndex - 1];
        merge_nodes(leftPid, childPid, parentPid, childIndex - 1);
    } else if (childIndex < parent.keyCount) {
        // Merge with right sibling
        uint32_t rightPid = parent.children[childIndex + 1];
        merge_nodes(childPid, rightPid, parentPid, childIndex);
    }
    
    return false;
}

template <typename T>
void DiskBPlusTree<T>::redistribute_keys(uint32_t leftPid, uint32_t rightPid, uint32_t parentPid, int keyIndex) {
    BPlusNode<T> left, right, parent;
    read(leftPid, left);
    read(rightPid, right);
    read(parentPid, parent);
    
    if (left.keyCount > get_min_keys()) {
        // Move last key from left to right
        T movedKey = left.keys[left.keyCount - 1];
        left.keyCount--;
        
        // Shift right keys to make space
        for (int i = right.keyCount; i > 0; i--) {
            right.keys[i] = right.keys[i - 1];
        }
        right.keys[0] = movedKey;
        right.keyCount++;
        
        // Update parent key
        parent.keys[keyIndex] = movedKey;
    } else {
        // Move first key from right to left
        T movedKey = right.keys[0];
        
        // Shift right keys to fill gap
        for (int i = 0; i < right.keyCount - 1; i++) {
            right.keys[i] = right.keys[i + 1];
        }
        right.keyCount--;
        
        left.keys[left.keyCount] = movedKey;
        left.keyCount++;
        
        // Update parent key
        parent.keys[keyIndex] = right.keys[0];
    }
    
    write(leftPid, left);
    write(rightPid, right);
    write(parentPid, parent);
}

template <typename T>
void DiskBPlusTree<T>::merge_nodes(uint32_t leftPid, uint32_t rightPid, uint32_t parentPid, int keyIndex) {
    BPlusNode<T> left, right, parent;
    read(leftPid, left);
    read(rightPid, right);
    read(parentPid, parent);
    
    // Copy all keys from right to left
    for (int i = 0; i < right.keyCount; i++) {
        left.keys[left.keyCount + i] = right.keys[i];
    }
    left.keyCount += right.keyCount;
    left.next = right.next;
    
    // Remove key and child pointer from parent
    for (int i = keyIndex; i < parent.keyCount - 1; i++) {
        parent.keys[i] = parent.keys[i + 1];
    }
    for (int i = keyIndex + 1; i <= parent.keyCount; i++) {
        parent.children[i] = parent.children[i + 1];
    }
    parent.keyCount--;
    
    write(leftPid, left);
    write(parentPid, parent);
    // Note: right node becomes unused and could be recycled
}

// Explicit instantiation (important!)
template class DiskBPlusTree<int>;
