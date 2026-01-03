#include "bplustree_disk.h"
#include "DataObject.h"
#include <iostream>
#include <string>
#include <vector>
#include <cstdlib>

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " --index <path> --min <value> --max <value>" << std::endl;
    std::cout << "Flags:" << std::endl;
    std::cout << "  --index, -i  Path to the B+ tree index file" << std::endl;
    std::cout << "  --min        Minimum value for range search" << std::endl;
    std::cout << "  --max        Maximum value for range search" << std::endl;
    std::cout << std::endl;
    std::cout << "Example: " << program_name << " --index data/my_index.bpt --min 20 --max 80" << std::endl;
}

int main(int argc, char* argv[]) {
    std::string index_path;
    int min_key = -1;
    int max_key = -1;
    bool has_index = false;
    bool has_min = false;
    bool has_max = false;

    // Parse command line flags
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if ((arg == "--index" || arg == "-i") && i + 1 < argc) {
            index_path = argv[++i];
            has_index = true;
        } else if (arg == "--min" && i + 1 < argc) {
            min_key = std::atoi(argv[++i]);
            has_min = true;
        } else if (arg == "--max" && i + 1 < argc) {
            max_key = std::atoi(argv[++i]);
            has_max = true;
        } else if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        }
    }

    if (!has_index || !has_min || !has_max) {
        std::cerr << "Error: Missing required flags" << std::endl;
        print_usage(argv[0]);
        return 1;
    }

    if (min_key > max_key) {
        std::cerr << "Error: min value must be less than or equal to max value" << std::endl;
        return 1;
    }

    std::cout << "=== B+ Tree Range Search ===" << std::endl;
    std::cout << "Index path: " << index_path << std::endl;
    std::cout << "Range: [" << min_key << ", " << max_key << "]" << std::endl;
    std::cout << std::endl;

    DiskBPlusTree dataTree(index_path);

    std::vector<DataObject*> results = dataTree.search_range(min_key, max_key);
    std::cout << "Found " << results.size() << " objects in range [" << min_key << ", " << max_key << "]:" << std::endl;
    
    for (size_t i = 0; i < results.size(); i++) {
        std::cout << "  #" << (i+1) << ": ";
        results[i]->print();
        delete results[i];
    }

    return 0;
}
