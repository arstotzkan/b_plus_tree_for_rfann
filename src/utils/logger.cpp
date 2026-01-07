#include "logger.h"
#include <filesystem>
#include <iostream>

// Static member definitions
std::ofstream Logger::log_file_;
std::mutex Logger::log_mutex_;
LogLevel Logger::min_level_ = LogLevel::INFO;
std::string Logger::operation_type_ = "general";
bool Logger::initialized_ = false;

void Logger::init(const std::string& index_dir, const std::string& operation_type) {
    std::lock_guard<std::mutex> lock(log_mutex_);
    
    if (initialized_) {
        close();
    }
    
    operation_type_ = operation_type;
    
    // Create log file path
    std::filesystem::path log_path = std::filesystem::path(index_dir) / (operation_type + ".log");
    
    // Ensure directory exists
    std::filesystem::create_directories(std::filesystem::path(index_dir));
    
    // Open log file in append mode
    log_file_.open(log_path, std::ios::app);
    
    if (!log_file_.is_open()) {
        std::cerr << "Warning: Could not open log file: " << log_path << std::endl;
        return;
    }
    
    initialized_ = true;
    
    // Log session start
    log_file_ << "\n" << std::string(80, '=') << "\n";
    log_file_ << "LOG SESSION START: " << get_timestamp() << " - " << operation_type << "\n";
    log_file_ << std::string(80, '=') << "\n";
    log_file_.flush();
}

void Logger::debug(const std::string& message) {
    log(LogLevel::DEBUG, message);
}

void Logger::info(const std::string& message) {
    log(LogLevel::INFO, message);
}

void Logger::warning(const std::string& message) {
    log(LogLevel::WARNING, message);
}

void Logger::error(const std::string& message) {
    log(LogLevel::ERROR, message);
}

void Logger::log(LogLevel level, const std::string& message) {
    if (!initialized_ || level < min_level_) {
        return;
    }
    
    write_log(level, message);
}

void Logger::log_performance(const std::string& operation, double duration_ms, const std::string& details) {
    if (!initialized_) return;
    
    std::ostringstream oss;
    oss << "PERFORMANCE: " << operation << " took " << std::fixed << std::setprecision(3) << duration_ms << " ms";
    if (!details.empty()) {
        oss << " (" << details << ")";
    }
    
    write_log(LogLevel::INFO, oss.str());
}

void Logger::log_config(const std::string& config_info) {
    if (!initialized_) return;
    
    write_log(LogLevel::INFO, "CONFIG: " + config_info);
}

void Logger::log_query(const std::string& query_type, const std::string& parameters, double duration_ms, int result_count) {
    if (!initialized_) return;
    
    std::ostringstream oss;
    oss << "QUERY: " << query_type << " | " << parameters << " | " 
        << std::fixed << std::setprecision(3) << duration_ms << " ms | " 
        << result_count << " results";
    
    write_log(LogLevel::INFO, oss.str());
}

void Logger::close() {
    std::lock_guard<std::mutex> lock(log_mutex_);
    
    if (initialized_ && log_file_.is_open()) {
        log_file_ << "LOG SESSION END: " << get_timestamp() << "\n";
        log_file_ << std::string(80, '=') << "\n\n";
        log_file_.close();
    }
    
    initialized_ = false;
}

void Logger::set_log_level(LogLevel level) {
    min_level_ = level;
}

std::string Logger::get_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;
    
    std::ostringstream oss;
    oss << std::put_time(std::localtime(&time_t), "%Y-%m-%d %H:%M:%S");
    oss << "." << std::setfill('0') << std::setw(3) << ms.count();
    
    return oss.str();
}

std::string Logger::level_to_string(LogLevel level) {
    switch (level) {
        case LogLevel::DEBUG:   return "DEBUG";
        case LogLevel::INFO:    return "INFO ";
        case LogLevel::WARNING: return "WARN ";
        case LogLevel::ERROR:   return "ERROR";
        default:                return "UNKN ";
    }
}

void Logger::write_log(LogLevel level, const std::string& message) {
    std::lock_guard<std::mutex> lock(log_mutex_);
    
    if (!log_file_.is_open()) return;
    
    log_file_ << "[" << get_timestamp() << "] [" << level_to_string(level) << "] " << message << "\n";
    log_file_.flush();
    
    // Also output to console for important messages
    if (level >= LogLevel::WARNING) {
        std::cout << "[" << level_to_string(level) << "] " << message << std::endl;
    }
}
