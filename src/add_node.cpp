#include "bplustree_disk.h"
#include "DataObject.h"
#include "index_directory.h"
#include "query_cache.h"
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>
#include <cmath>

// Parse comma-separated vector string into vector<float>
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
    std::cout << "Usage: " << program_name << " --index <index_dir> --key <key> --vector <v1,v2,...>" << std::endl;
    std::cout << std::endl;
    std::cout << "Flags:" << std::endl;
    std::cout << "  --index, -i   Path to the index directory (required)" << std::endl;
    std::cout << "  --key, -k     Key value for the new node (required)" << std::endl;
    std::cout << "  --vector, -v  Vector data (comma-separated, e.g., 1.0,2.0,3.0)" << std::endl;
    std::cout << "  --help, -h    Show this help message" << std::endl;
    std::cout << std::endl;
    std::cout << "Examples:" << std::endl;
    std::cout << "  Add integer key:  " << program_name << " --index data/my_index --key 42 --vector 1.0,2.0,3.0" << std::endl;
    std::cout << "  Add float key:    " << program_name << " --index data/my_index --key 42.5 --vector 1.0,2.0,3.0" << std::endl;
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
    
    if (!has_vector) {
        std::cerr << "Error: Missing required --vector flag" << std::endl;
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
        
        DataObject new_obj(vector_data, 0);  // Initialize with dummy value
        if (is_float_key) {
            float key_val = std::stof(key_str);
            new_obj = DataObject(vector_data, key_val);
            std::cout << "Adding node with float key: " << key_val << std::endl;
        } else {
            int key_val = std::stoi(key_str);
            new_obj = DataObject(vector_data, key_val);
            std::cout << "Adding node with integer key: " << key_val << std::endl;
        }
        
        std::cout << "Vector dimension: " << vector_data.size() << std::endl;
        std::cout << "Vector data: [";
        for (size_t i = 0; i < vector_data.size(); i++) {
            std::cout << vector_data[i];
            if (i < vector_data.size() - 1) std::cout << ", ";
        }
        std::cout << "]" << std::endl;
        
        // Insert the new node
        dataTree.insert_data_object(new_obj);
        
        // Update affected cache entries - insert new object if it's closer than furthest cached neighbor
        int key_for_cache = is_float_key ? static_cast<int>(std::stof(key_str)) : std::stoi(key_str);
        
        // Distance function for cache update
        auto distance_fn = [](const std::vector<float>& a, const std::vector<float>& b) -> double {
            double sum = 0.0;
            size_t min_size = std::min(a.size(), b.size());
            for (size_t i = 0; i < min_size; i++) {
                double diff = a[i] - b[i];
                sum += diff * diff;
            }
            return std::sqrt(sum);
        };
        
        int updated_caches = cache.update_for_inserted_object(key_for_cache, vector_data, distance_fn);
        
        if (updated_caches > 0) {
            std::cout << "Updated " << updated_caches << " cached queries with new closer neighbor" << std::endl;
        }
        
        std::cout << "Node added successfully!" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
