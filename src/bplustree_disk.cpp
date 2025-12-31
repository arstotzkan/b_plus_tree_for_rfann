#include "bplustree_disk.h"
#include <vector>
#include <iostream>

DiskBPlusTree::DiskBPlusTree(const std::string& filename)
    : pm(filename) {}

void DiskBPlusTree::read(uint32_t pid, BPlusNode& node) {
    pm.readPage(pid, &node);
}

void DiskBPlusTree::write(uint32_t pid, const BPlusNode& node) {
    pm.writePage(pid, &node);
}

void DiskBPlusTree::splitLeaf(uint32_t leafPid, BPlusNode& leaf, int& promotedKey, uint32_t& newLeafPid) {
    BPlusNode newLeaf{};
    newLeaf.isLeaf = true;

    int mid = leaf.keyCount / 2;
    newLeaf.keyCount = leaf.keyCount - mid;

    for (int i = 0; i < newLeaf.keyCount; i++) {
        newLeaf.keys[i] = leaf.keys[mid + i];
        newLeaf.vector_sizes[i] = leaf.vector_sizes[mid + i];
        for (int j = 0; j < leaf.vector_sizes[mid + i] && j < MAX_VECTOR_SIZE; j++) {
            newLeaf.data_vectors[i][j] = leaf.data_vectors[mid + i][j];
        }
    }

    leaf.keyCount = mid;

    newLeaf.next = leaf.next;
    newLeafPid = pm.allocatePage();
    leaf.next = newLeafPid;

    promotedKey = newLeaf.keys[0];

    write(leafPid, leaf);
    write(newLeafPid, newLeaf);
}

void DiskBPlusTree::insert_data_object(const DataObject& obj) {
    std::cout << "insert_data_object called" << std::endl;
    int key;
    if (obj.is_int_value()) {
        key = obj.get_int_value();
    } else {
        key = static_cast<int>(obj.get_float_value());
    }
    std::cout << "Key extracted: " << key << std::endl;
    
    uint32_t rootPid = pm.getRoot();
    std::cout << "Root PID: " << rootPid << std::endl;

    if (rootPid == INVALID_PAGE) {
        std::cout << "Creating new root" << std::endl;
        BPlusNode root{};
        std::cout << "BPlusNode created" << std::endl;
        root.isLeaf = true;
        root.keyCount = 1;
        root.keys[0] = key;
        std::cout << "Basic fields set" << std::endl;
        
        // Copy vector data to fixed-size array
        const std::vector<int>& vec = obj.get_vector();
        std::cout << "Vector size: " << vec.size() << std::endl;
        root.vector_sizes[0] = vec.size();
        std::cout << "Vector size set" << std::endl;
        for (size_t j = 0; j < vec.size() && j < MAX_VECTOR_SIZE; j++) {
            root.data_vectors[0][j] = vec[j];
        }
        std::cout << "Vector data copied" << std::endl;
        
        root.next = INVALID_PAGE;
        std::cout << "Next pointer set" << std::endl;

        uint32_t pid = pm.allocatePage();
        std::cout << "Allocated page: " << pid << std::endl;
        write(pid, root);
        std::cout << "Page written" << std::endl;
        pm.setRoot(pid);
        std::cout << "Root set successfully" << std::endl;
        return;
    }

    std::vector<uint32_t> path;
    uint32_t pid = rootPid;
    BPlusNode node;

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
        node.vector_sizes[i + 1] = node.vector_sizes[i];
        for (int j = 0; j < node.vector_sizes[i] && j < MAX_VECTOR_SIZE; j++) {
            node.data_vectors[i + 1][j] = node.data_vectors[i][j];
        }
        i--;
    }
    node.keys[i + 1] = key;
    
    // Copy vector data to fixed-size array
    const std::vector<int>& vec = obj.get_vector();
    node.vector_sizes[i + 1] = vec.size();
    for (size_t j = 0; j < vec.size() && j < MAX_VECTOR_SIZE; j++) {
        node.data_vectors[i + 1][j] = vec[j];
    }
    
    node.keyCount++;

    if (node.keyCount < ORDER) {
        write(pid, node);
        return;
    }

    int promoted;
    uint32_t newLeafPid;
    splitLeaf(pid, node, promoted, newLeafPid);

    for (int level = path.size() - 2; level >= 0; level--) {
        uint32_t parentPid = path[level];
        BPlusNode parent;
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

    BPlusNode newRoot{};
    newRoot.isLeaf = false;
    newRoot.keyCount = 1;
    newRoot.keys[0] = promoted;
    newRoot.children[0] = rootPid;
    newRoot.children[1] = newLeafPid;

    uint32_t newRootPid = pm.allocatePage();
    write(newRootPid, newRoot);
    pm.setRoot(newRootPid);
}

DataObject* DiskBPlusTree::search_data_object(const DataObject& obj) {
    int key;
    if (obj.is_int_value()) {
        key = obj.get_int_value();
    } else {
        key = static_cast<int>(obj.get_float_value());
    }
    
    uint32_t pid = pm.getRoot();
    if (pid == INVALID_PAGE) return nullptr;
    
    BPlusNode node;
    
    while (true) {
        read(pid, node);
        
        int i = 0;
        while (i < node.keyCount && key >= node.keys[i]) i++;
        
        if (node.isLeaf) break;
        pid = node.children[i];
    }
    
    for (int i = 0; i < node.keyCount; i++) {
        if (node.keys[i] == key) {
            // Reconstruct DataObject from fixed-size array
            std::vector<int> vec(node.vector_sizes[i]);
            for (int j = 0; j < node.vector_sizes[i] && j < MAX_VECTOR_SIZE; j++) {
                vec[j] = node.data_vectors[i][j];
            }
            DataObject* result = new DataObject(vec, key);
            return result;
        }
    }
    
    return nullptr;
}

DataObject* DiskBPlusTree::search_data_object(int key) {
    uint32_t pid = pm.getRoot();
    if (pid == INVALID_PAGE) return nullptr;
    
    BPlusNode node;
    
    while (true) {
        read(pid, node);
        
        int i = 0;
        while (i < node.keyCount && key >= node.keys[i]) i++;
        
        if (node.isLeaf) break;
        pid = node.children[i];
    }
    
    for (int i = 0; i < node.keyCount; i++) {
        if (node.keys[i] == key) {
            // Reconstruct DataObject from fixed-size array
            std::vector<int> vec(node.vector_sizes[i]);
            for (int j = 0; j < node.vector_sizes[i] && j < MAX_VECTOR_SIZE; j++) {
                vec[j] = node.data_vectors[i][j];
            }
            DataObject* result = new DataObject(vec, key);
            return result;
        }
    }
    
    return nullptr;
}

DataObject* DiskBPlusTree::search_data_object(float key) {
    return search_data_object(static_cast<int>(key));
}

std::vector<DataObject*> DiskBPlusTree::search_range(int min_key, int max_key) {
    std::vector<DataObject*> results;
    
    uint32_t pid = pm.getRoot();
    if (pid == INVALID_PAGE) return results;
    
    // Find leaf node that contains min_key
    BPlusNode node;
    while (true) {
        read(pid, node);
        
        int i = 0;
        while (i < node.keyCount && min_key >= node.keys[i]) i++;
        
        if (node.isLeaf) break;
        pid = node.children[i];
    }
    
    // Collect data from this leaf and subsequent leaves until max_key is reached
    uint32_t currentPid = pid;
    uint32_t visitedCount = 0;
    
    while (currentPid != INVALID_PAGE) {
        BPlusNode leaf;
        read(currentPid, leaf);
        
        for (int i = 0; i < leaf.keyCount; i++) {
            if (leaf.keys[i] >= min_key && leaf.keys[i] <= max_key) {
                std::vector<int> vec(leaf.vector_sizes[i]);
                for (int j = 0; j < leaf.vector_sizes[i] && j < MAX_VECTOR_SIZE; j++) {
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
            break; // Circular reference detected
        }
        currentPid = nextPid; // Move to next leaf
        visitedCount++;
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
    
    uint32_t pid = pm.getRoot();
    if (pid == INVALID_PAGE) return false;
    
    BPlusNode node;
    
    while (true) {
        read(pid, node);
        
        int i = 0;
        while (i < node.keyCount && key >= node.keys[i]) i++;
        
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
    uint32_t rootPid = pm.getRoot();
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
        if (node.isLeaf && node.vector_sizes[i] > 0) {
            std::cout << "(v:" << node.vector_sizes[i] << ")";
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

int DiskBPlusTree::get_min_keys() {
    return (ORDER - 1) / 2;
}
