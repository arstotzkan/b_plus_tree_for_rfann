#include "DataObject.h"
#include <iostream>
#include <stdexcept>

// Constructor with vector and int value
DataObject::DataObject(const std::vector<int>& vector, int value)
    : data_vector(vector), numeric_value(value) {
}

// Constructor with vector and float value
DataObject::DataObject(const std::vector<int>& vector, float value)
    : data_vector(vector), numeric_value(value) {
}

// Copy constructor
DataObject::DataObject(const DataObject& other)
    : data_vector(other.data_vector), numeric_value(other.numeric_value) {
}

// Assignment operator
DataObject& DataObject::operator=(const DataObject& other) {
    if (this != &other) {
        data_vector = other.data_vector;
        numeric_value = other.numeric_value;
    }
    return *this;
}

// Destructor
DataObject::~DataObject() {
}

// Getters
const std::vector<int>& DataObject::get_vector() const {
    return data_vector;
}

std::vector<int>& DataObject::get_vector() {
    return data_vector;
}

int DataObject::get_int_value() const {
    return std::get<int>(numeric_value);
}

float DataObject::get_float_value() const {
    return std::get<float>(numeric_value);
}

bool DataObject::is_int_value() const {
    return std::holds_alternative<int>(numeric_value);
}

// Setters
void DataObject::set_vector_size(size_t new_size) {
    data_vector.resize(new_size, 0);
}

void DataObject::set_int_value(int value) {
    numeric_value = value;
}

void DataObject::set_float_value(float value) {
    numeric_value = value;
}

// Vector operations
void DataObject::set_vector_element(size_t index, int value) {
    if (index >= data_vector.size()) {
        throw std::out_of_range("Index out of range");
    }
    data_vector[index] = value;
}

int DataObject::get_vector_element(size_t index) const {
    if (index >= data_vector.size()) {
        throw std::out_of_range("Index out of range");
    }
    return data_vector[index];
}

size_t DataObject::get_vector_size() const {
    return data_vector.size();
}

// Utility functions
void DataObject::print() const {
    std::cout << "[";
    for (size_t i = 0; i < data_vector.size(); i++) {
        std::cout << data_vector[i];
        if (i < data_vector.size() - 1) std::cout << ", ";
    }
    std::cout << "]";
    
    if (is_int_value()) {
        std::cout << "  (" << get_int_value() << ")" << std::endl;
    } else {
        std::cout << "  (" << get_float_value() << ")" << std::endl;
    }
}

void DataObject::clear_vector() {
    data_vector.clear();
}

// Comparison operators for B+ tree ordering
bool DataObject::operator<(const DataObject& other) const {
    int this_val = is_int_value() ? get_int_value() : static_cast<int>(get_float_value());
    int other_val = other.is_int_value() ? other.get_int_value() : static_cast<int>(other.get_float_value());
    return this_val < other_val;
}

bool DataObject::operator>(const DataObject& other) const {
    return other < *this;
}

bool DataObject::operator==(const DataObject& other) const {
    int this_val = is_int_value() ? get_int_value() : static_cast<int>(get_float_value());
    int other_val = other.is_int_value() ? other.get_int_value() : static_cast<int>(other.get_float_value());
    return this_val == other_val;
}

bool DataObject::operator<=(const DataObject& other) const {
    return *this < other || *this == other;
}

bool DataObject::operator>=(const DataObject& other) const {
    return *this > other || *this == other;
}

bool DataObject::operator!=(const DataObject& other) const {
    return !(*this == other);
}
