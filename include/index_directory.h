#pragma once
#include <string>
#include <filesystem>

class IndexDirectory {
public:
    explicit IndexDirectory(const std::string& dir_path);
    
    bool ensure_exists();
    
    std::string get_index_file_path() const;
    std::string get_cache_dir_path() const;
    std::string get_config_file_path() const;
    std::string get_base_dir() const;
    
    bool index_exists() const;
    bool cache_exists() const;
    
    static bool create_default_config(const std::string& config_path);

private:
    std::string base_dir_;
    std::string index_file_;
    std::string cache_dir_;
    std::string config_file_;
};
