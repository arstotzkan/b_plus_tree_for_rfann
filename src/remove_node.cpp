#include "bplustree_disk.h"
#include "DataObject.h"
#include "index_directory.h"
#include "query_cache.h"
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>

std::vector<float> parse_vector(const std::string& str) {
    std::vector<float> result;
    std::stringstream ss(str);
    std::string item;
    while (std::getline(ss, item, ',')) {
        result.push_back(static_cast<float>(std::atof(item.c_str())));
    }
    return result;
}

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " --index <index_dir> --key <key> [--vector <v1,v2,...>]" << std::endl;
    std::cout << std::endl;
    std::cout << "Flags:" << std::endl;
    std::cout << "  --index, -i   Path to the index directory (required)" << std::endl;
    std::cout << "  --key, -k     Key value of the node to remove (required)" << std::endl;
    std::cout << "  --vector, -v  Vector data to match (comma-separated). If provided, deletes" << std::endl;
    std::cout << "                only the entry matching both key AND vector." << std::endl;
    std::cout << "                If not provided, deletes the first entry with the key." << std::endl;
    std::cout << "  --help, -h    Show this help message" << std::endl;
    std::cout << std::endl;
    std::cout << "Examples:" << std::endl;
    std::cout << "  Remove by key only:       " << program_name << " --index data/my_index --key 42" << std::endl;
    std::cout << "  Remove specific entry:    " << program_name << " --index data/my_index --key 42 --vector 1.0,2.0,3.0" << std::endl;
}

int main(int argc, char* argv[]) {
    std::string index_dir;
    std::string key_str;
    std::vector<float> vector_data;
    bool has_index = false;
    bool has_key = false;
    bool has_vector = false;
    
    // Parse command line flags
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if ((arg == "--index" || arg == "-i") && i + 1 < argc) {
            index_dir = argv[++i];
            has_index = true;
        } else if ((arg == "--key" || arg == "-k") && i + 1 < argc) {
            key_str = argv[++i];
            has_key = true;
        } else if ((arg == "--vector" || arg == "-v") && i + 1 < argc) {
            vector_data = parse_vector(argv[++i]);
            has_vector = true;
        } else if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        }
    }
    
    if (!has_index) {
        std::cerr << "Error: Missing required --index flag" << std::endl;
        print_usage(argv[0]);
        return 1;
    }
    
    if (!has_key) {
        std::cerr << "Error: Missing required --key flag" << std::endl;
        print_usage(argv[0]);
        return 1;
    }
    
    try {
        // Initialize index directory
        IndexDirectory idx_dir(index_dir);
        if (!idx_dir.index_exists()) {
            std::cerr << "Error: Index directory does not exist: " << index_dir << std::endl;
            return 1;
        }
        
        std::string index_file = idx_dir.get_index_file_path();
        if (!std::filesystem::exists(index_file)) {
            std::cerr << "Error: Index file not found: " << index_file << std::endl;
            return 1;
        }
        
        // Open B+ tree
        DiskBPlusTree dataTree(index_file);
        
        // Initialize query cache for invalidation
        QueryCache cache(index_dir, true);
        cache.load_config(idx_dir.get_config_file_path());
        
        // Determine if key is integer or float
        bool is_float_key = key_str.find('.') != std::string::npos;
        
        DataObject* found_obj = nullptr;
        if (is_float_key) {
            float key_val = std::stof(key_str);
            std::cout << "Searching for node with float key: " << key_val << std::endl;
            found_obj = dataTree.search_data_object(key_val);
        } else {
            int key_val = std::stoi(key_str);
            std::cout << "Searching for node with integer key: " << key_val << std::endl;
            found_obj = dataTree.search_data_object(key_val);
        }
        
        if (!found_obj) {
            std::cout << "Node with key " << key_str << " not found in the index." << std::endl;
            return 1;
        }
        
        // Display found node info
        std::cout << "Found node:" << std::endl;
        std::cout << "  Key: " << key_str << std::endl;
        std::cout << "  Vector dimension: " << found_obj->get_vector().size() << std::endl;
        std::cout << "  Vector data: [";
        const auto& vec = found_obj->get_vector();
        for (size_t i = 0; i < std::min(vec.size(), size_t(5)); i++) {
            std::cout << vec[i];
            if (i < std::min(vec.size(), size_t(5)) - 1) std::cout << ", ";
        }
        if (vec.size() > 5) std::cout << ", ... (" << vec.size() << " dims)";
        std::cout << "]" << std::endl;
        
        // Store the vector of the object to be deleted (for cache update)
        std::vector<float> deleted_vector;
        int key_for_cache = is_float_key ? static_cast<int>(std::stof(key_str)) : std::stoi(key_str);
        
        // Perform deletion
        std::cout << std::endl;
        
        bool deleted = false;
        if (has_vector) {
            // Delete specific DataObject matching key + vector
            std::cout << "Deleting specific entry with key " << key_str << " and matching vector..." << std::endl;
            
            // Create DataObject with the provided vector and key
            int key_val = is_float_key ? static_cast<int>(std::stof(key_str)) : std::stoi(key_str);
            DataObject obj_to_delete(vector_data, key_val);
            deleted_vector = vector_data;
            deleted = dataTree.delete_data_object(obj_to_delete);
        } else {
            // Delete first entry with this key (use the found object)
            std::cout << "Deleting first entry with key " << key_str << "..." << std::endl;
            deleted_vector = found_obj->get_vector();
            deleted = dataTree.delete_data_object(*found_obj);
        }
        
        // Clean up found object after deletion attempt
        delete found_obj;
        found_obj = nullptr;
        
        if (deleted) {
            std::cout << "Successfully deleted node with key " << key_str << std::endl;
            
            // Update affected cache entries by removing the deleted object
            int updated_caches = cache.update_for_deleted_object(key_for_cache, deleted_vector);
            
            if (updated_caches > 0) {
                std::cout << "Updated " << updated_caches << " cached queries to remove deleted entry" << std::endl;
            }
            
            // Verify deletion
            DataObject* verify = nullptr;
            if (is_float_key) {
                verify = dataTree.search_data_object(std::stof(key_str));
            } else {
                verify = dataTree.search_data_object(std::stoi(key_str));
            }
            
            if (verify == nullptr) {
                std::cout << "Verification: Key " << key_str << " no longer exists in the index." << std::endl;
            } else {
                std::cerr << "Warning: Key " << key_str << " still found after deletion!" << std::endl;
                delete verify;
            }
        } else {
            std::cerr << "Failed to delete node with key " << key_str << std::endl;
            return 1;
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
