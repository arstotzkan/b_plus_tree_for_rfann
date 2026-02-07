#pragma once
#include <cstdint>
#include <vector>
#include <variant>

class DataObject {
private:
    std::vector<float> data_vector;  // Changed to float for SIFT vectors
    std::variant<int, float> numeric_value;
    int32_t id_ = -1;  // Original index in the fvecs file (-1 = unset)

public:
    // Constructors
    DataObject(const std::vector<float>& vector, int value);
    DataObject(const std::vector<float>& vector, float value);
    DataObject(std::vector<float>&& vector, int value);    // Move-taking constructor
    DataObject(std::vector<float>&& vector, float value);  // Move-taking constructor
    DataObject(const std::vector<int>& vector, int value);  // Legacy support
    DataObject(const std::vector<int>& vector, float value);  // Legacy support
    DataObject(int vector_size, int value);  // Size-based constructor
    DataObject(const DataObject& other);
    DataObject(DataObject&& other) noexcept;  // Move constructor

    // Assignment operators
    DataObject& operator=(const DataObject& other);
    DataObject& operator=(DataObject&& other) noexcept;  // Move assignment

    // Destructor
    ~DataObject();

    // Getters
    const std::vector<float>& get_vector() const;
    std::vector<float>& get_vector();
    
    int get_int_value() const;
    float get_float_value() const;
    bool is_int_value() const;
    int32_t get_id() const { return id_; }

    // Setters
    void set_vector_size(size_t new_size);
    void set_int_value(int value);
    void set_float_value(float value);
    void set_id(int32_t id) { id_ = id; }

    // Vector operations
    void set_vector_element(size_t index, float value);
    float get_vector_element(size_t index) const;
    size_t get_vector_size() const;

    // Utility functions
    void print() const;
    void clear_vector();

    // Comparison operators for B+ tree ordering
    bool operator<(const DataObject& other) const;
    bool operator>(const DataObject& other) const;
    bool operator==(const DataObject& other) const;
    bool operator<=(const DataObject& other) const;
    bool operator>=(const DataObject& other) const;
    bool operator!=(const DataObject& other) const;
};

// Template function implementations (after class definition)
template<size_t N>
DataObject createDataObject(const int (&array)[N], int value) {
    std::vector<float> vec(N);
    for (size_t i = 0; i < N; i++) vec[i] = static_cast<float>(array[i]);
    return DataObject(vec, value);
}

template<size_t N>
DataObject createDataObject(const int (&array)[N], float value) {
    std::vector<float> vec(N);
    for (size_t i = 0; i < N; i++) vec[i] = static_cast<float>(array[i]);
    return DataObject(vec, value);
}

template<size_t N>
DataObject createDataObject(const float (&array)[N], int value) {
    return DataObject(std::vector<float>(array, array + N), value);
}

template<size_t N>
DataObject createDataObject(const float (&array)[N], float value) {
    return DataObject(std::vector<float>(array, array + N), value);
}
