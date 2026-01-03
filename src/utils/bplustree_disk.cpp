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
    int key;
    if (obj.is_int_value()) {
        key = obj.get_int_value();
    } else {
        key = static_cast<int>(obj.get_float_value());
    }
    
    uint32_t rootPid = pm.getRoot();

    if (rootPid == INVALID_PAGE) {
        // Create first leaf node as root
        BPlusNode root{};
        root.isLeaf = true;
        root.keyCount = 1;
        root.keys[0] = key;
        root.next = INVALID_PAGE;
        
        // Initialize all children to INVALID_PAGE
        for (int i = 0; i <= ORDER; i++) {
            root.children[i] = INVALID_PAGE;
        }
        
        // Copy vector data
        const std::vector<int>& vec = obj.get_vector();
        root.vector_sizes[0] = vec.size();
        for (size_t j = 0; j < vec.size() && j < MAX_VECTOR_SIZE; j++) {
            root.data_vectors[0][j] = vec[j];
        }

        uint32_t pid = pm.allocatePage();
        write(pid, root);
        pm.setRoot(pid);
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
        for (int j = 0; j < node.vector_sizes[i] && j < MAX_VECTOR_SIZE; j++) {
            node.data_vectors[i + 1][j] = node.data_vectors[i][j];
        }
        i--;
    }
    node.keys[i + 1] = key;
    
    const std::vector<int>& vec = obj.get_vector();
    node.vector_sizes[i + 1] = vec.size();
    for (size_t j = 0; j < vec.size() && j < MAX_VECTOR_SIZE; j++) {
        node.data_vectors[i + 1][j] = vec[j];
    }
    
    node.keyCount++;

    // If leaf doesn't overflow, just write and return
    if (node.keyCount < ORDER) {
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
    
    for (int level = path.size() - 2; level >= 0; level--) {
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
        if (parent.keyCount < ORDER) {
            write(parentPid, parent);
            return;
        }

        // Parent overflows - split internal node
        BPlusNode newInternal{};
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
        
        // Initialize remaining children to INVALID_PAGE
        for (int k = newInternal.keyCount + 1; k <= ORDER; k++) {
            newInternal.children[k] = INVALID_PAGE;
        }
        
        parent.keyCount = mid;
        
        // Initialize remaining children in parent to INVALID_PAGE
        for (int k = parent.keyCount + 1; k <= ORDER; k++) {
            parent.children[k] = INVALID_PAGE;
        }

        uint32_t newInternalPid = pm.allocatePage();
        write(parentPid, parent);
        write(newInternalPid, newInternal);
        
        childPid = newInternalPid;
        // promotedKey is already set above
    }

    // If we get here, root needs to split
    BPlusNode newRoot{};
    newRoot.isLeaf = false;
    newRoot.keyCount = 1;
    newRoot.keys[0] = promotedKey;
    newRoot.children[0] = rootPid;
    newRoot.children[1] = childPid;
    
    // Initialize remaining children to INVALID_PAGE
    for (int k = 2; k <= ORDER; k++) {
        newRoot.children[k] = INVALID_PAGE;
    }

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
        while (i < node.keyCount && key > node.keys[i]) i++;
        
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
        while (i < node.keyCount && key > node.keys[i]) i++;
        
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
    if (pid == INVALID_PAGE) {
        return results;
    }
    
    // Find leaf node that contains min_key
    BPlusNode node;
    int traversal_count = 0;
    const int MAX_TRAVERSAL = 100;
    
    while (true) {
        traversal_count++;
        if (traversal_count > MAX_TRAVERSAL) {
            std::cout << "ERROR: Infinite loop in range search traversal!" << std::endl;
            return results;
        }
        
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
    uint32_t visitedCount = 0;
    const uint32_t MAX_LEAVES_TO_VISIT = 100000000; // Safety limit
    
    
    while (currentPid != INVALID_PAGE && visitedCount < MAX_LEAVES_TO_VISIT) {
        visitedCount++;
        
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
            std::cout << "ERROR: Circular reference detected in leaf linking!" << std::endl;
            break;
        }
        if (nextPid == INVALID_PAGE) {
            break;
        }
        
        currentPid = nextPid; // Move to next leaf
    }
    
    if (visitedCount >= MAX_LEAVES_TO_VISIT) {
        std::cout << "ERROR: Visited too many leaves (" << visitedCount << "), possible infinite loop!" << std::endl;
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
            std::cout << "([";
            for (int j = 0; j < node.vector_sizes[i] && j < MAX_VECTOR_SIZE; j++) {
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

int DiskBPlusTree::get_min_keys() {
    return (ORDER - 1) / 2;
}
