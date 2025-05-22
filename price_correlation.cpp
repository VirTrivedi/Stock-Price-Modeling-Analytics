#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstdint>
#include <algorithm>
#include <cmath>
#include <numeric>
#include <iomanip>
#include <optional>
#include <limits>
#include <cstring>

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

const size_t FILLS_BAR_SIZE = sizeof(FillsBarRecord);
const size_t TOPS_BAR_SIZE = sizeof(TopsBarRecord);
const size_t MIN_DATA_LENGTH = 10;

// Helper to convert string to lower case
std::string to_lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c){ return std::tolower(c); });
    return s;
}

// Helper to convert string to upper case
std::string to_upper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c){ return std::toupper(c); });
    return s;
}

// Generic function to read closing prices from a binary bar file
template<typename BarType>
std::vector<double> read_bar_file_closing_prices(const std::string& file_path, size_t bar_size) {
    std::vector<double> closing_prices;
    std::ifstream file(file_path, std::ios::binary);

    if (!file.is_open()) {
        std::cerr << "Error: File not found - " << file_path << std::endl;
        return closing_prices; // Return empty vector
    }

    BarType bar_record;
    char buffer[sizeof(BarType)];

    while (file.read(buffer, bar_size)) {
        if (static_cast<size_t>(file.gcount()) < bar_size) {
            break; 
        }
        std::memcpy(&bar_record, buffer, bar_size);
        closing_prices.push_back(bar_record.close);
    }
    
    if (!file.eof() && file.fail()) {
        std::cerr << "Error reading file " << file_path << std::endl;
    }

    file.close();
    return closing_prices;
}

// Specific file readers
std::vector<double> read_fills_bar_file(const std::string& file_path) {
    return read_bar_file_closing_prices<FillsBarRecord>(file_path, FILLS_BAR_SIZE);
}

std::vector<double> read_tops_bar_file(const std::string& file_path) {
    return read_bar_file_closing_prices<TopsBarRecord>(file_path, TOPS_BAR_SIZE);
}

// Trims two lists to the same length by evenly removing entries from the longer list.
std::pair<std::vector<double>, std::vector<double>> trim_to_same_length(
    const std::vector<double>& list1_in, 
    const std::vector<double>& list2_in) {
    
    std::vector<double> list1 = list1_in; 
    std::vector<double> list2 = list2_in;

    size_t len1 = list1.size();
    size_t len2 = list2.size();

    if (len1 == 0 || len2 == 0) {
        return {{}, {}};
    }

    std::vector<double> result1, result2;

    if (len1 > len2) {
        result2 = list2; 
        size_t step = std::max(static_cast<size_t>(1), len1 / len2);
        for (size_t i = 0; i < len1 && result1.size() < len2; i += step) {
            result1.push_back(list1[i]);
        }
    } else if (len2 > len1) {
        result1 = list1; 
        size_t step = std::max(static_cast<size_t>(1), len2 / len1);
        for (size_t i = 0; i < len2 && result2.size() < len1; i += step) {
            result2.push_back(list2[i]);
        }
    } else {
        result1 = list1;
        result2 = list2;
    }
    return {result1, result2};
}

// Calculates Pearson correlation coefficient
std::optional<double> calculate_pearson_correlation(
    const std::vector<double>& x, 
    const std::vector<double>& y) {
    
    size_t n = x.size();
    if (n == 0 || n != y.size() || n < 2) {
        return std::nullopt;
    }

    double sum_x = 0.0, sum_y = 0.0, sum_xy = 0.0;
    double sum_x_sq = 0.0, sum_y_sq = 0.0;

    for (size_t i = 0; i < n; ++i) {
        sum_x += x[i];
        sum_y += y[i];
        sum_xy += x[i] * y[i];
        sum_x_sq += x[i] * x[i];
        sum_y_sq += y[i] * y[i];
    }

    double numerator = static_cast<double>(n) * sum_xy - sum_x * sum_y;
    double denom_x_term = static_cast<double>(n) * sum_x_sq - sum_x * sum_x;
    double denom_y_term = static_cast<double>(n) * sum_y_sq - sum_y * sum_y;

    double epsilon = 1e-9; 
    if (denom_x_term < epsilon || denom_y_term < epsilon) { 
        return std::nullopt; 
    }

    double denominator = std::sqrt(denom_x_term * denom_y_term);
    
    if (std::abs(denominator) < epsilon) { 
        return std::nullopt; 
    }

    return numerator / denominator;
}

// Wrapper to calculate correlation between closing prices of two files.
std::optional<double> calculate_file_correlation(
    const std::string& file1_path, 
    const std::string& file2_path, 
    bool is_fills_file_type) {
    
    std::vector<double> prices1, prices2;

    if (is_fills_file_type) {
        prices1 = read_fills_bar_file(file1_path);
        prices2 = read_fills_bar_file(file2_path);
    } else {
        prices1 = read_tops_bar_file(file1_path);
        prices2 = read_tops_bar_file(file2_path);
    }

    if (prices1.empty() || prices2.empty()) {
        std::cout << "Skipping: Empty data in files:\n  " << file1_path << "\n  " << file2_path << std::endl;
        return std::nullopt;
    }

    auto trimmed_prices_pair = trim_to_same_length(prices1, prices2);
    std::vector<double>& trimmed_prices1 = trimmed_prices_pair.first;
    std::vector<double>& trimmed_prices2 = trimmed_prices_pair.second;

    if (trimmed_prices1.size() < MIN_DATA_LENGTH || trimmed_prices2.size() < MIN_DATA_LENGTH) {
        std::cout << "Skipping after trimming (too little data):\n  " 
                  << file1_path << " (" << trimmed_prices1.size() << " entries)\n  " 
                  << file2_path << " (" << trimmed_prices2.size() << " entries)" << std::endl;
        return std::nullopt;
    }
    
    return calculate_pearson_correlation(trimmed_prices1, trimmed_prices2);
}

// Calculates a weighted average of the given correlations.
std::optional<double> calculate_weighted_correlation(
    const std::vector<std::optional<double>>& correlations, 
    const std::vector<double>& weights) {
    
    if (correlations.size() != weights.size()) {
        std::cerr << "Error: Correlations and weights lists must have the same size." << std::endl;
        return std::nullopt;
    }

    double weighted_sum = 0.0;
    double total_weight = 0.0;
    bool has_valid_correlation = false;

    for (size_t i = 0; i < correlations.size(); ++i) {
        if (correlations[i].has_value()) {
            weighted_sum += correlations[i].value() * weights[i];
            total_weight += weights[i];
            has_valid_correlation = true;
        }
    }

    if (!has_valid_correlation || std::abs(total_weight) < 1e-9) {
        std::cout << "Error: No valid correlations or zero total weight to calculate the overall correlation." << std::endl;
        return std::nullopt;
    }

    return weighted_sum / total_weight;
}


int main() {
    std::string date, feed, symbol1_str, symbol2_str;

    std::cout << "Enter file date (yearMonthDay): ";
    std::cin >> date;
    std::cout << "Enter file feed: ";
    std::cin >> feed;
    std::cout << "Enter first symbol: ";
    std::cin >> symbol1_str;
    std::cout << "Enter second symbol: ";
    std::cin >> symbol2_str;

    std::string base_path_template = "/home/vir/" + date + "/" + to_lower(feed) + "/bars/" + to_upper(feed);
    
    std::string s1_upper = to_upper(symbol1_str);
    std::string s2_upper = to_upper(symbol2_str);

    // File paths
    std::string fills_file_s1 = base_path_template + ".fills_bars." + s1_upper + ".bin";
    std::string l1_bid_file_s1 = base_path_template + ".bid_bars_L1." + s1_upper + ".bin";
    std::string l1_ask_file_s1 = base_path_template + ".ask_bars_L1." + s1_upper + ".bin";
    std::string l2_bid_file_s1 = base_path_template + ".bid_bars_L2." + s1_upper + ".bin";
    std::string l2_ask_file_s1 = base_path_template + ".ask_bars_L2." + s1_upper + ".bin";
    std::string l3_bid_file_s1 = base_path_template + ".bid_bars_L3." + s1_upper + ".bin";
    std::string l3_ask_file_s1 = base_path_template + ".ask_bars_L3." + s1_upper + ".bin";

    std::string fills_file_s2 = base_path_template + ".fills_bars." + s2_upper + ".bin";
    std::string l1_bid_file_s2 = base_path_template + ".bid_bars_L1." + s2_upper + ".bin";
    std::string l1_ask_file_s2 = base_path_template + ".ask_bars_L1." + s2_upper + ".bin";
    std::string l2_bid_file_s2 = base_path_template + ".bid_bars_L2." + s2_upper + ".bin";
    std::string l2_ask_file_s2 = base_path_template + ".ask_bars_L2." + s2_upper + ".bin";
    std::string l3_bid_file_s2 = base_path_template + ".bid_bars_L3." + s2_upper + ".bin";
    std::string l3_ask_file_s2 = base_path_template + ".ask_bars_L3." + s2_upper + ".bin";

    // Calculate correlations
    std::vector<std::optional<double>> correlations_list;
    correlations_list.push_back(calculate_file_correlation(fills_file_s1, fills_file_s2, true));
    correlations_list.push_back(calculate_file_correlation(l1_bid_file_s1, l1_bid_file_s2, false));
    correlations_list.push_back(calculate_file_correlation(l1_ask_file_s1, l1_ask_file_s2, false));
    correlations_list.push_back(calculate_file_correlation(l2_bid_file_s1, l2_bid_file_s2, false));
    correlations_list.push_back(calculate_file_correlation(l2_ask_file_s1, l2_ask_file_s2, false));
    correlations_list.push_back(calculate_file_correlation(l3_bid_file_s1, l3_bid_file_s2, false));
    correlations_list.push_back(calculate_file_correlation(l3_ask_file_s1, l3_ask_file_s2, false));

    std::vector<double> weights = {0.125, 0.125, 0.125, 0.125, 0.125, 0.125, 0.125}; // 7 weights
    
    std::optional<double> overall_correlation = calculate_weighted_correlation(correlations_list, weights);

    // Display results
    std::cout << std::fixed << std::setprecision(4);
    const char* labels[] = {
        "fills closing prices",
        "L1 bid closing prices", "L1 ask closing prices",
        "L2 bid closing prices", "L2 ask closing prices",
        "L3 bid closing prices", "L3 ask closing prices"
    };

    for (size_t i = 0; i < correlations_list.size(); ++i) {
        if (correlations_list[i].has_value()) {
            std::cout << "Correlation between " << labels[i] << " of " << s1_upper 
                      << " and " << s2_upper << ": " << correlations_list[i].value() << std::endl;
        }
    }

    if (overall_correlation.has_value()) {
        std::cout << "Overall correlation between " << s1_upper << " and " << s2_upper 
                  << ": " << overall_correlation.value() << std::endl;
    }

    return 0;
}