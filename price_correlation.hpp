#ifndef PRICE_CORRELATION_HPP
#define PRICE_CORRELATION_HPP

#include <vector>
#include <string>
#include <optional>
#include <cstdint>

// Define the binary format for fills bars
#pragma pack(push, 1)
struct FillsBarRecord {
    uint64_t timestamp_sec;
    double high;
    double low;
    double open;
    double close;
    int32_t volume;
};

// Define the binary format for tops bars
struct TopsBarRecord {
    uint64_t timestamp_sec;
    double high;
    double low;
    double open;
    double close;
};
#pragma pack(pop)

// Declare the constant, its definition is in price_correlation.cpp
extern const size_t MIN_DATA_LENGTH = 10;

// Function declarations from price_correlation.cpp
std::string to_lower(std::string s);
std::string to_upper(std::string s);

std::vector<double> read_fills_bar_file(const std::string& file_path);
std::vector<double> read_tops_bar_file(const std::string& file_path);

std::optional<double> calculate_file_correlation(
    const std::string& file1_path,
    const std::string& file2_path,
    bool is_fills_file_type);

std::optional<double> calculate_weighted_correlation(
    const std::vector<std::optional<double>>& correlations,
    const std::vector<double>& weights);

#endif