#pragma once
#include <string>
#include <fstream>
#include <chrono>
#include <mutex>
#include <sstream>
#include <iomanip>

enum class LogLevel {
    DEBUG = 0,
    INFO = 1,
    WARNING = 2,
    ERROR = 3
};

class Logger {
public:
    // Initialize logger for a specific index directory
    static void init(const std::string& index_dir, const std::string& operation_type = "general");
    
    // Log messages with different levels
    static void debug(const std::string& message);
    static void info(const std::string& message);
    static void warning(const std::string& message);
    static void error(const std::string& message);
    
    // Log with custom formatting
    static void log(LogLevel level, const std::string& message);
    
    // Log performance metrics
    static void log_performance(const std::string& operation, double duration_ms, const std::string& details = "");
    
    // Log B+ tree configuration
    static void log_config(const std::string& config_info);
    
    // Log query details
    static void log_query(const std::string& query_type, const std::string& parameters, double duration_ms, int result_count);
    
    // Log node operations (add/remove)
    static void log_node_operation(const std::string& operation, const std::string& details);
    
    // Flush and close logger
    static void close();
    
    // Set minimum log level
    static void set_log_level(LogLevel level);
    
    // Get current session ID
    static std::string get_session_id();

private:
    static std::string get_timestamp();
    static std::string level_to_string(LogLevel level);
    static void write_log(LogLevel level, const std::string& message, const std::string& log_type = "general");
    static std::string generate_session_id();
    
    static std::ofstream log_file_;
    static std::ofstream search_log_file_;
    static std::ofstream index_log_file_;
    static std::mutex log_mutex_;
    static LogLevel min_level_;
    static std::string operation_type_;
    static std::string session_id_;
    static bool initialized_;
};

// Convenience macros for logging
#define LOG_DEBUG(msg) Logger::debug(msg)
#define LOG_INFO(msg) Logger::info(msg)
#define LOG_WARNING(msg) Logger::warning(msg)
#define LOG_ERROR(msg) Logger::error(msg)

// Performance logging helper
class PerformanceTimer {
public:
    PerformanceTimer(const std::string& operation_name) 
        : operation_name_(operation_name), start_time_(std::chrono::high_resolution_clock::now()) {}
    
    ~PerformanceTimer() {
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time_);
        double duration_ms = duration.count() / 1000.0;
        Logger::log_performance(operation_name_, duration_ms);
    }
    
    void add_details(const std::string& details) {
        details_ = details;
    }
    
    void finish_with_details(const std::string& details) {
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time_);
        double duration_ms = duration.count() / 1000.0;
        Logger::log_performance(operation_name_, duration_ms, details);
    }

private:
    std::string operation_name_;
    std::string details_;
    std::chrono::high_resolution_clock::time_point start_time_;
};

#define PERF_TIMER(name) PerformanceTimer _perf_timer(name)
