#include "index_directory.h"
#include <fstream>

namespace fs = std::filesystem;

IndexDirectory::IndexDirectory(const std::string& dir_path)
    : base_dir_(dir_path) {
    index_file_ = base_dir_ + "/index.bpt";
    cache_dir_ = base_dir_ + "/.cache";
    config_file_ = base_dir_ + "/config.ini";
}

bool IndexDirectory::ensure_exists() {
    try {
        fs::create_directories(base_dir_);
        fs::create_directories(cache_dir_);
        
        if (!fs::exists(config_file_)) {
            create_default_config(config_file_);
        }
        
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

std::string IndexDirectory::get_index_file_path() const {
    return index_file_;
}

std::string IndexDirectory::get_cache_dir_path() const {
    return cache_dir_;
}

std::string IndexDirectory::get_config_file_path() const {
    return config_file_;
}

std::string IndexDirectory::get_base_dir() const {
    return base_dir_;
}

bool IndexDirectory::index_exists() const {
    return fs::exists(index_file_);
}

bool IndexDirectory::cache_exists() const {
    return fs::exists(cache_dir_);
}

bool IndexDirectory::create_default_config(const std::string& config_path) {
    std::ofstream file(config_path);
    if (!file.is_open()) return false;
    
    file << "[cache]\n";
    file << "cache_enabled = true\n";
    file << "max_cache_size_mb = 100\n";
    file << "\n";
    file << "[index]\n";
    file << "# Index configuration options\n";
    
    return true;
}
