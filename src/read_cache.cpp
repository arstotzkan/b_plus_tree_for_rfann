#include "query_cache.h"
#include "index_directory.h"
#include <iostream>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <ctime>

namespace fs = std::filesystem;

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " --index <index_dir> [options]" << std::endl;
    std::cout << std::endl;
    std::cout << "Flags:" << std::endl;
    std::cout << "  --index, -i     Path to the index directory (required)" << std::endl;
    std::cout << "  --query-id, -q  Show specific query by ID (optional)" << std::endl;
    std::cout << "  --summary, -s   Show only summary information (optional)" << std::endl;
    std::cout << "  --help, -h      Show this help message" << std::endl;
    std::cout << std::endl;
    std::cout << "Examples:" << std::endl;
    std::cout << "  List all cached queries:     " << program_name << " --index data/my_index" << std::endl;
    std::cout << "  Show summary only:           " << program_name << " --index data/my_index --summary" << std::endl;
    std::cout << "  Show specific query:         " << program_name << " --index data/my_index --query-id abc123def" << std::endl;
}

std::string format_time(std::time_t timestamp) {
    if (timestamp == 0) return "N/A";
    
    std::tm* tm_info = std::localtime(&timestamp);
    std::ostringstream oss;
    oss << std::put_time(tm_info, "%Y-%m-%d %H:%M:%S");
    return oss.str();
}

std::string format_vector(const std::vector<float>& vec, size_t max_elements = 5) {
    std::ostringstream oss;
    oss << "[";
    size_t display_count = std::min(vec.size(), max_elements);
    for (size_t i = 0; i < display_count; i++) {
        oss << std::fixed << std::setprecision(3) << vec[i];
        if (i < display_count - 1) oss << ", ";
    }
    if (vec.size() > max_elements) {
        oss << ", ... (" << vec.size() << " dims)";
    }
    oss << "]";
    return oss.str();
}

void display_query_summary(const std::string& query_id, const CachedQueryResult& result) {
    std::cout << "Query ID: " << query_id << std::endl;
    std::cout << "  Created:     " << format_time(result.created_date) << std::endl;
    std::cout << "  Last used:   " << format_time(result.last_used_date) << std::endl;
    std::cout << "  Range:       [" << result.min_key << ", " << result.max_key << "]" << std::endl;
    std::cout << "  Max K:       " << result.max_k << std::endl;
    std::cout << "  Neighbors:   " << result.neighbors.size() << std::endl;
    std::cout << "  Query vec:   " << format_vector(result.input_vector) << std::endl;
    std::cout << std::endl;
}

void display_query_detailed(const std::string& query_id, const CachedQueryResult& result) {
    std::cout << "=== Query Details ===" << std::endl;
    std::cout << "Query ID: " << query_id << std::endl;
    std::cout << "Created:  " << format_time(result.created_date) << std::endl;
    std::cout << "Last used: " << format_time(result.last_used_date) << std::endl;
    std::cout << "Range: [" << result.min_key << ", " << result.max_key << "]" << std::endl;
    std::cout << "Max K: " << result.max_k << std::endl;
    std::cout << "Query vector (" << result.input_vector.size() << " dims): " << format_vector(result.input_vector, 10) << std::endl;
    std::cout << std::endl;
    
    std::cout << "Cached neighbors (" << result.neighbors.size() << "):" << std::endl;
    for (size_t i = 0; i < result.neighbors.size(); i++) {
        const auto& neighbor = result.neighbors[i];
        std::cout << "  #" << (i+1) << " (dist=" << std::fixed << std::setprecision(4) << neighbor.distance << "): ";
        std::cout << format_vector(neighbor.vector, 8) << "  (key=" << neighbor.key << ", id=" << neighbor.original_id << ")" << std::endl;
    }
    std::cout << std::endl;
}

int main(int argc, char* argv[]) {
    std::string index_dir;
    std::string specific_query_id;
    bool has_index = false;
    bool summary_only = false;
    bool has_specific_query = false;
    
    // Parse command line flags
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if ((arg == "--index" || arg == "-i") && i + 1 < argc) {
            index_dir = argv[++i];
            has_index = true;
        } else if ((arg == "--query-id" || arg == "-q") && i + 1 < argc) {
            specific_query_id = argv[++i];
            has_specific_query = true;
        } else if (arg == "--summary" || arg == "-s") {
            summary_only = true;
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
    
    try {
        // Initialize index directory
        IndexDirectory idx_dir(index_dir);
        if (!idx_dir.index_exists()) {
            std::cerr << "Error: Index directory does not exist: " << index_dir << std::endl;
            return 1;
        }
        
        // Initialize query cache
        QueryCache cache(index_dir, true);
        cache.load_config(idx_dir.get_config_file_path());
        
        // Check if cache directory exists
        std::string cache_dir = index_dir + "/.cache";
        if (!fs::exists(cache_dir)) {
            std::cout << "No cache directory found at: " << cache_dir << std::endl;
            return 0;
        }
        
        // Get all cache files
        std::vector<std::string> cache_files;
        for (const auto& entry : fs::directory_iterator(cache_dir)) {
            if (entry.is_regular_file() && entry.path().extension() == ".qcache") {
                std::string filename = entry.path().stem().string();
                cache_files.push_back(filename);
            }
        }
        
        if (cache_files.empty()) {
            std::cout << "No cache files found in: " << cache_dir << std::endl;
            return 0;
        }
        
        std::cout << "=== Cache Reader ===" << std::endl;
        std::cout << "Index directory: " << index_dir << std::endl;
        std::cout << "Cache directory: " << cache_dir << std::endl;
        std::cout << "Total cached queries: " << cache_files.size() << std::endl;
        std::cout << std::endl;
        
        if (has_specific_query) {
            // Show specific query
            bool found = false;
            for (const auto& query_id : cache_files) {
                if (query_id == specific_query_id) {
                    CachedQueryResult result;
                    if (cache.load_query_result(query_id, result)) {
                        display_query_detailed(query_id, result);
                        found = true;
                    } else {
                        std::cerr << "Error: Failed to load query result for ID: " << query_id << std::endl;
                    }
                    break;
                }
            }
            
            if (!found) {
                std::cerr << "Error: Query ID not found: " << specific_query_id << std::endl;
                std::cout << "Available query IDs:" << std::endl;
                for (const auto& query_id : cache_files) {
                    std::cout << "  " << query_id << std::endl;
                }
                return 1;
            }
        } else {
            // Show all queries
            for (const auto& query_id : cache_files) {
                CachedQueryResult result;
                if (cache.load_query_result(query_id, result)) {
                    if (summary_only) {
                        display_query_summary(query_id, result);
                    } else {
                        display_query_detailed(query_id, result);
                    }
                } else {
                    std::cerr << "Warning: Failed to load query result for ID: " << query_id << std::endl;
                }
            }
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
