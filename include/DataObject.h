#pragma once
#include <vector>
#include <variant>

class DataObject {
private:
    std::vector<int> data_vector;
    std::variant<int, float> numeric_value;

public:
    // Constructors
    DataObject(const std::vector<int>& vector, int value);
    DataObject(const std::vector<int>& vector, float value);
    DataObject(const DataObject& other);

    // Assignment operator
    DataObject& operator=(const DataObject& other);

    // Destructor
    ~DataObject();

    // Getters
    const std::vector<int>& get_vector() const;
    std::vector<int>& get_vector();
    
    int get_int_value() const;
    float get_float_value() const;
    bool is_int_value() const;

    // Setters
    void set_vector_size(size_t new_size);
    void set_int_value(int value);
    void set_float_value(float value);

    // Vector operations
    void set_vector_element(size_t index, int value);
    int get_vector_element(size_t index) const;
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
    return DataObject(std::vector<int>(array, array + N), value);
}

template<size_t N>
DataObject createDataObject(const int (&array)[N], float value) {
    return DataObject(std::vector<int>(array, array + N), value);
}
