#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <set>
#include <map>
#include <regex>
#include <filesystem>
#include <algorithm>
#include <iomanip>
#include <cmath>
#include <optional>
#include "price_correlation.hpp"

namespace fs = std::filesystem;

// Function to extract unique stock symbols from .bin filenames in a folder
std::vector<std::string> extract_symbols_from_folder(const fs::path& folder_path) {
    std::regex symbol_pattern("\\.(?:fills_bars|bid_bars_L[0-9]|ask_bars_L[0-9])\\.([A-Z0-9_]+)\\.bin$", std::regex_constants::icase);
    std::set<std::string> symbols_set;

    if (!fs::is_directory(folder_path)) {
        std::cerr << "Error: " << folder_path << " is not a valid directory." << std::endl;
        return {};
    }

    for (const auto& entry : fs::directory_iterator(folder_path)) {
        if (entry.is_regular_file()) {
            std::string filename = entry.path().filename().string();
            std::smatch match;
            if (std::regex_search(filename, match, symbol_pattern)) {
                if (match.size() > 1) {
                    symbols_set.insert(to_upper(match[1].str()));
                }
            }
        }
    }
    std::vector<std::string> symbols_vec(symbols_set.begin(), symbols_set.end());
    std::sort(symbols_vec.begin(), symbols_vec.end());
    return symbols_vec;
}

// Creates a map of all required file paths for a symbol
std::map<std::string, std::string> generate_file_paths_cpp(const std::string& base_path_for_feed, const std::string& symbol) {
    std::string upper_symbol = to_upper(symbol);
    return {
        {"fills", base_path_for_feed + ".fills_bars." + upper_symbol + ".bin"},
        {"L1_bid", base_path_for_feed + ".bid_bars_L1." + upper_symbol + ".bin"},
        {"L1_ask", base_path_for_feed + ".ask_bars_L1." + upper_symbol + ".bin"},
        {"L2_bid", base_path_for_feed + ".bid_bars_L2." + upper_symbol + ".bin"},
        {"L2_ask", base_path_for_feed + ".ask_bars_L2." + upper_symbol + ".bin"},
        {"L3_bid", base_path_for_feed + ".bid_bars_L3." + upper_symbol + ".bin"},
        {"L3_ask", base_path_for_feed + ".ask_bars_L3." + upper_symbol + ".bin"}
    };
}

// Check if all required files for a symbol contain sufficient data
bool is_symbol_valid_cpp(const std::string& base_path_for_feed, const std::string& symbol) {
    auto paths = generate_file_paths_cpp(base_path_for_feed, symbol);
    if (paths.empty()) return false;

    std::vector<std::vector<double>> data_checks;
    data_checks.push_back(read_fills_bar_file(paths.at("fills")));
    data_checks.push_back(read_tops_bar_file(paths.at("L1_bid")));
    data_checks.push_back(read_tops_bar_file(paths.at("L1_ask")));
    data_checks.push_back(read_tops_bar_file(paths.at("L2_bid")));
    data_checks.push_back(read_tops_bar_file(paths.at("L2_ask")));
    data_checks.push_back(read_tops_bar_file(paths.at("L3_bid")));
    data_checks.push_back(read_tops_bar_file(paths.at("L3_ask")));

    for (const auto& data : data_checks) {
        if (data.size() < MIN_DATA_LENGTH) {
            std::cout << "Debug: Symbol " << symbol << " invalid due to insufficient data in one of its files." << std::endl;
            return false;
        }
    }
    return true;
}

struct CorrelationResult {
    std::string symbol1;
    std::string symbol2;
    double overall_correlation;
};

// Computes overall correlation for all valid symbol pairs
std::vector<CorrelationResult> compute_overall_correlations_cpp(
    const std::vector<std::string>& valid_symbols,
    const std::string& base_path_for_feed) {
    std::vector<CorrelationResult> results;
    if (valid_symbols.size() < 2) {
        return results;
    }

    size_t total_pairs = valid_symbols.size() * (valid_symbols.size() - 1) / 2;
    size_t current_pair = 0;

    for (size_t i = 0; i < valid_symbols.size(); ++i) {
        for (size_t j = i + 1; j < valid_symbols.size(); ++j) {
            current_pair++;
            const std::string& sym1 = valid_symbols[i];
            const std::string& sym2 = valid_symbols[j];

            std::cout << "[" << current_pair << "/" << total_pairs << "] Processing: " << sym1 << " vs " << sym2 << std::endl;

            auto paths1 = generate_file_paths_cpp(base_path_for_feed, sym1);
            auto paths2 = generate_file_paths_cpp(base_path_for_feed, sym2);

            std::vector<std::optional<double>> correlations;

            correlations.push_back(calculate_file_correlation(paths1.at("fills"), paths2.at("fills"), true));
            correlations.push_back(calculate_file_correlation(paths1.at("L1_bid"), paths2.at("L1_bid"), false));
            correlations.push_back(calculate_file_correlation(paths1.at("L1_ask"), paths2.at("L1_ask"), false));
            correlations.push_back(calculate_file_correlation(paths1.at("L2_bid"), paths2.at("L2_bid"), false));
            correlations.push_back(calculate_file_correlation(paths1.at("L2_ask"), paths2.at("L2_ask"), false));
            correlations.push_back(calculate_file_correlation(paths1.at("L3_bid"), paths2.at("L3_bid"), false));
            correlations.push_back(calculate_file_correlation(paths1.at("L3_ask"), paths2.at("L3_ask"), false));
            
            std::vector<double> weights(correlations.size(), 0.125);

            std::optional<double> overall_opt = calculate_weighted_correlation(correlations, weights);

            if (overall_opt.has_value()) {
                results.push_back({sym1, sym2, std::round(overall_opt.value() * 10000.0) / 10000.0});
            }
        }
    }
    return results;
}

// Saves only overall correlation values to a CSV
void save_correlations_to_csv_cpp(const std::vector<CorrelationResult>& results, const std::string& output_file_path) {
    std::ofstream outfile(output_file_path);
    if (!outfile.is_open()) {
        std::cerr << "Error: Could not open file for writing: " << output_file_path << std::endl;
        return;
    }

    outfile << "symbol1,symbol2,overall_correlation\n";
    outfile << std::fixed << std::setprecision(4);

    for (const auto& res : results) {
        outfile << res.symbol1 << ","
                << res.symbol2 << ","
                << res.overall_correlation << "\n";
    }
    outfile.close();
    std::cout << "Results saved to " << output_file_path << std::endl;
}

int main() {
    std::string date_str, feed_str;
    std::cout << "Enter file date (YYYYMMDD): ";
    std::cin >> date_str;
    std::cout << "Enter file feed: ";
    std::cin >> feed_str;

    fs::path base_folder = fs::path("/home/vir") / date_str / to_lower(feed_str) / "bars";
    std::string base_path_for_feed = (base_folder / to_upper(feed_str)).string();

    if (!fs::exists(base_folder) || !fs::is_directory(base_folder)) {
        std::cerr << "Error: Base folder for bars not found or is not a directory: " << base_folder.string() << std::endl;
        return 1;
    }
    
    std::cout << "Finding symbols in " << base_folder.string() << "..." << std::endl;
    std::vector<std::string> all_symbols = extract_symbols_from_folder(base_folder);

    std::cout << "Found " << all_symbols.size() << " unique symbols. Validating data files..." << std::endl;
    std::vector<std::string> valid_symbols;
    std::vector<std::string> invalid_symbols;

    for (const auto& symbol : all_symbols) {
        if (is_symbol_valid_cpp(base_path_for_feed, symbol)) {
            valid_symbols.push_back(symbol);
        } else {
            invalid_symbols.push_back(symbol);
        }
    }

    std::cout << valid_symbols.size() << " symbols have valid data." << std::endl;
    if (!invalid_symbols.empty()) {
        std::cout << invalid_symbols.size() << " symbols were skipped due to missing or empty files: ";
        for (size_t i = 0; i < invalid_symbols.size(); ++i) {
            std::cout << invalid_symbols[i] << (i == invalid_symbols.size() - 1 ? "" : ", ");
        }
        std::cout << std::endl;
    }
    
    if (valid_symbols.size() < 2) {
        std::cout << "Not enough valid symbols to compute correlations. Exiting." << std::endl;
        return 0;
    }

    std::cout << "Computing overall correlations..." << std::endl;
    std::vector<CorrelationResult> final_results = compute_overall_correlations_cpp(valid_symbols, base_path_for_feed);

    if (!final_results.empty()) {
        std::string output_csv_path = (base_folder / "overall_correlations.csv").string();
        save_correlations_to_csv_cpp(final_results, output_csv_path);
    } else {
        std::cout << "No correlation results were computed." << std::endl;
    }

    std::cout << "Done." << std::endl;

    return 0;
}