#include "bplustree_disk.h"
#include "DataObject.h"
#include <iostream>
#include <string>
#include <vector>
#include <cstdlib>
#include <cmath>
#include <algorithm>
#include <sstream>

// Calculate Euclidean distance between two vectors
double calculate_distance(const std::vector<int>& v1, const std::vector<int>& v2) {
    double sum = 0.0;
    size_t min_size = std::min(v1.size(), v2.size());
    for (size_t i = 0; i < min_size; i++) {
        double diff = static_cast<double>(v1[i]) - static_cast<double>(v2[i]);
        sum += diff * diff;
    }
    return std::sqrt(sum);
}

// Parse comma-separated vector string like "1,2,3" into vector<int>
std::vector<int> parse_vector(const std::string& str) {
    std::vector<int> result;
    std::stringstream ss(str);
    std::string item;
    while (std::getline(ss, item, ',')) {
        result.push_back(std::atoi(item.c_str()));
    }
    return result;
}

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " --index <path> [--value <value>] [--vector <v1,v2,...>] [--K <count>]" << std::endl;
    std::cout << std::endl;
    std::cout << "Flags:" << std::endl;
    std::cout << "  --index, -i   Path to the B+ tree index file (required)" << std::endl;
    std::cout << "  --value, -v   Search for all objects with a specific value" << std::endl;
    std::cout << "  --vector      Query vector for KNN search (comma-separated, e.g., 1,2,3)" << std::endl;
    std::cout << "  --K, -k       Number of nearest neighbors to return (requires --vector)" << std::endl;
    std::cout << std::endl;
    std::cout << "Note: If no flags besides --index are specified, runs predefined tests" << std::endl;
    std::cout << std::endl;
    std::cout << "Examples:" << std::endl;
    std::cout << "  Run all tests:   " << program_name << " --index data/my_index.bpt" << std::endl;
    std::cout << "  Value search:    " << program_name << " --index data/my_index.bpt --value 42" << std::endl;
    std::cout << "  KNN at value:    " << program_name << " --index data/my_index.bpt --value 42 --vector 10,20,30 --K 5" << std::endl;
}

int main(int argc, char* argv[]) {
    std::string index_path;
    int search_value = -1;
    std::vector<int> query_vector;
    int k_neighbors = -1;
    bool has_index = false;
    bool has_value = false;
    bool has_vector = false;
    bool has_k = false;

    // Parse command line flags
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if ((arg == "--index" || arg == "-i") && i + 1 < argc) {
            index_path = argv[++i];
            has_index = true;
        } else if ((arg == "--value" || arg == "-v") && i + 1 < argc) {
            search_value = std::atoi(argv[++i]);
            has_value = true;
        } else if (arg == "--vector" && i + 1 < argc) {
            query_vector = parse_vector(argv[++i]);
            has_vector = true;
        } else if ((arg == "--K" || arg == "-k") && i + 1 < argc) {
            k_neighbors = std::atoi(argv[++i]);
            has_k = true;
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

    if (has_k && !has_vector) {
        std::cerr << "Error: --K requires --vector to be specified" << std::endl;
        print_usage(argv[0]);
        return 1;
    }

    if (has_k && k_neighbors <= 0) {
        std::cerr << "Error: K must be a positive integer" << std::endl;
        return 1;
    }

    std::cout << "=== B+ Tree Index Search Test ===" << std::endl;
    std::cout << "Index path: " << index_path << std::endl;
    std::cout << std::endl;

    DiskBPlusTree dataTree(index_path);

    if (has_value) {
        // Value search mode - get all objects with this value
        std::cout << "=== Searching for value " << search_value << " ===" << std::endl;
        
        if (has_vector) {
            std::cout << "Query vector: [";
            for (size_t i = 0; i < query_vector.size(); i++) {
                std::cout << query_vector[i];
                if (i < query_vector.size() - 1) std::cout << ", ";
            }
            std::cout << "]" << std::endl;
        }
        if (has_k) {
            std::cout << "K nearest neighbors: " << k_neighbors << std::endl;
        }
        std::cout << std::endl;

        std::vector<DataObject*> results = dataTree.search_range(search_value, search_value);
        
        if (has_vector && has_k) {
            // KNN search
            std::vector<std::pair<double, DataObject*>> distances;
            for (DataObject* obj : results) {
                double dist = calculate_distance(query_vector, obj->get_vector());
                distances.push_back({dist, obj});
            }

            std::sort(distances.begin(), distances.end(), 
                [](const auto& a, const auto& b) { return a.first < b.first; });

            int output_count = std::min(k_neighbors, static_cast<int>(distances.size()));
            std::cout << "Found " << results.size() << " objects, showing " << output_count << " nearest neighbors:" << std::endl;
            
            for (int i = 0; i < output_count; i++) {
                std::cout << "  #" << (i+1) << " (dist=" << distances[i].first << "): ";
                distances[i].second->print();
            }

            for (auto& pair : distances) {
                delete pair.second;
            }
        } else {
            std::cout << "Found " << results.size() << " objects:" << std::endl;
            for (size_t i = 0; i < results.size(); i++) {
                std::cout << "  #" << (i+1) << ": ";
                results[i]->print();
                delete results[i];
            }
        }
        return 0;
    }

    // Run predefined tests
    std::cout << "=== Searching DataObjects ===" << std::endl;

    // Test search with int keys
    int search_keys[] = {42, 3, 100, 999};
    for (int key : search_keys) {
        DataObject* found = dataTree.search_data_object(key);
        if (found) {
            std::cout << "Found DataObject with key " << key << ": ";
            found->print();
            delete found;
        } else {
            std::cout << "DataObject with key " << key << " not found" << std::endl;
        }
    }

    // Test search with float key
    std::cout << std::endl << "=== Testing Float Key Search ===" << std::endl;
    DataObject* found_float = dataTree.search_data_object(3.14f);
    if (found_float) {
        std::cout << "Found with float key 3.14: ";
        found_float->print();
        delete found_float;
    } else {
        std::cout << "Not found with float key 3.14" << std::endl;
    }

    // Test range searches
    std::cout << std::endl << "=== Testing Range Search (20 to 80) ===" << std::endl;
    std::vector<DataObject*> range_results = dataTree.search_range(20, 80);
    std::cout << "Found " << range_results.size() << " objects in range [20, 80]:" << std::endl;
    for (size_t i = 0; i < range_results.size(); i++) {
        std::cout << "  #" << (i+1) << ": ";
        range_results[i]->print();
        delete range_results[i];
    }

    std::cout << std::endl << "=== Testing Range Search (90 to 105) ===" << std::endl;
    std::vector<DataObject*> range_results2 = dataTree.search_range(90, 105);
    std::cout << "Found " << range_results2.size() << " objects in range [90, 105]:" << std::endl;
    for (size_t i = 0; i < range_results2.size(); i++) {
        std::cout << "  #" << (i+1) << ": ";
        range_results2[i]->print();
        delete range_results2[i];
    }

    std::cout << std::endl << "=== Testing Range Search (300 to 500) ===" << std::endl;
    std::vector<DataObject*> high_range_results = dataTree.search_range(300, 500);
    std::cout << "Found " << high_range_results.size() << " objects in range [300, 500]" << std::endl;

    return 0;
}
