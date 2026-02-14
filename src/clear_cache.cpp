#include "index_directory.h"
#include <iostream>
#include <filesystem>
#include <string>

namespace fs = std::filesystem;

void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " --index <index_dir> [options]" << std::endl;
    std::cout << std::endl;
    std::cout << "Flags:" << std::endl;
    std::cout << "  --index, -i     Path to the index directory (required)" << std::endl;
    std::cout << "  --yes, -y   Confirm deletion without prompting" << std::endl;
    std::cout << "  --help, -h      Show this help message" << std::endl;
    std::cout << std::endl;
    std::cout << "Examples:" << std::endl;
    std::cout << "  Clear cache with prompt:     " << program_name << " --index data/my_index" << std::endl;
    std::cout << "  Clear cache without prompt:  " << program_name << " --index data/my_index --yes" << std::endl;
}

int main(int argc, char* argv[]) {
    std::string index_dir;
    bool has_index = false;
    bool auto_confirm = false;
    
    // Parse command line flags
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if ((arg == "--index" || arg == "-i") && i + 1 < argc) {
            index_dir = argv[++i];
            has_index = true;
        } else if (arg == "--yes" || arg == "-y") {
            auto_confirm = true;
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
        
        std::string cache_dir = idx_dir.get_cache_dir_path();
        
        // Check if cache directory exists
        if (!fs::exists(cache_dir)) {
            std::cout << "No cache directory found at: " << cache_dir << std::endl;
            return 0;
        }
        
        // Count cache files
        int cache_file_count = 0;
        size_t total_size = 0;
        
        for (const auto& entry : fs::directory_iterator(cache_dir)) {
            if (entry.is_regular_file()) {
                cache_file_count++;
                total_size += entry.file_size();
            }
        }
        
        if (cache_file_count == 0) {
            std::cout << "Cache directory is already empty: " << cache_dir << std::endl;
            return 0;
        }
        
        std::cout << "=== Cache Clear Utility ===" << std::endl;
        std::cout << "Index directory: " << index_dir << std::endl;
        std::cout << "Cache directory: " << cache_dir << std::endl;
        std::cout << "Cache files found: " << cache_file_count << std::endl;
        std::cout << "Total cache size: " << (total_size / 1024.0) << " KB" << std::endl;
        std::cout << std::endl;
        
        // Confirm deletion
        bool proceed = auto_confirm;
        if (!auto_confirm) {
            std::cout << "Are you sure you want to delete all cache files? (y/N): ";
            std::string response;
            std::getline(std::cin, response);
            proceed = (response == "y" || response == "Y" || response == "yes" || response == "YES");
        }
        
        if (!proceed) {
            std::cout << "Cache clear cancelled." << std::endl;
            return 0;
        }
        
        // Delete all files in cache directory
        int deleted_count = 0;
        size_t deleted_size = 0;
        
        for (const auto& entry : fs::directory_iterator(cache_dir)) {
            if (entry.is_regular_file()) {
                try {
                    deleted_size += entry.file_size();
                    fs::remove(entry.path());
                    deleted_count++;
                } catch (const std::exception& e) {
                    std::cerr << "Warning: Failed to delete " << entry.path().filename() 
                              << ": " << e.what() << std::endl;
                }
            }
        }
        
        std::cout << "Cache cleared successfully!" << std::endl;
        std::cout << "Deleted " << deleted_count << " files (" << (deleted_size / 1024.0) << " KB)" << std::endl;
        
        // Verify cache directory is empty
        int remaining_files = 0;
        for (const auto& entry : fs::directory_iterator(cache_dir)) {
            if (entry.is_regular_file()) {
                remaining_files++;
            }
        }
        
        if (remaining_files > 0) {
            std::cerr << "Warning: " << remaining_files << " files could not be deleted" << std::endl;
            return 1;
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
