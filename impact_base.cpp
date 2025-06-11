#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <iomanip>
#include <cmath>
#include <cstdint>
#include <stdexcept>
#include <algorithm>
#include <filesystem>
#include <sys/stat.h>
#include <cerrno>

// Header format
struct Header {
    uint64_t feed_id;
    uint32_t dateint;
    uint32_t number_of_tops;
    uint64_t symbol_idx;
};

// Book top format
struct BookTop {
    uint64_t ts;
    uint64_t seqno;
    int64_t bid_price[3];
    int64_t ask_price[3];
    uint32_t bid_qty[3];
    uint32_t ask_qty[3];
};

// Output format
struct ExecutionResult {
    uint64_t timestamp;
    uint64_t seqno;
    double bid_exec_price;
    uint32_t bid_levels_consumed;
    double ask_exec_price;
    uint32_t ask_levels_consumed;

    ExecutionResult() : timestamp(0), seqno(0),
                        bid_exec_price(NAN), bid_levels_consumed(0),
                        ask_exec_price(NAN), ask_levels_consumed(0) {}
};

// Function to calculate effective price and levels consumed for one side
std::pair<double, uint32_t> calculate_side_execution(
    uint32_t target_exec_quantity,
    const int64_t side_prices[3],
    const uint32_t side_quantities[3]) {

    if (target_exec_quantity == 0) {
        return {NAN, 0};
    }

    double total_value_for_qty = 0.0;
    uint32_t quantity_filled = 0;
    uint32_t levels_touched = 0;

    for (int i = 0; i < 3; ++i) {
        if (quantity_filled == target_exec_quantity) {
            break; 
        }

        if (side_prices[i] == 0 || side_quantities[i] == 0) {
            break; 
        }
        
        levels_touched++;

        double price_at_level = static_cast<double>(side_prices[i]) / 1e9;
        uint32_t qty_available_at_level = side_quantities[i];
        
        uint32_t qty_needed_from_this_level = target_exec_quantity - quantity_filled;
        uint32_t qty_executed_this_level = std::min(qty_needed_from_this_level, qty_available_at_level);

        total_value_for_qty += qty_executed_this_level * price_at_level;
        quantity_filled += qty_executed_this_level;
    }

    if (quantity_filled < target_exec_quantity) {
        return {NAN, levels_touched}; 
    }

    return {total_value_for_qty / static_cast<double>(target_exec_quantity), levels_touched};
}

// Function to check if the relevant fields of ExecutionResult have changed
bool results_meaningfully_changed(const ExecutionResult& r1, const ExecutionResult& r2) {
    bool bid_price_diff = (std::isnan(r1.bid_exec_price) != std::isnan(r2.bid_exec_price)) ||
                          (!std::isnan(r1.bid_exec_price) && r1.bid_exec_price != r2.bid_exec_price);
    bool ask_price_diff = (std::isnan(r1.ask_exec_price) != std::isnan(r2.ask_exec_price)) ||
                          (!std::isnan(r1.ask_exec_price) && r1.ask_exec_price != r2.ask_exec_price);

    return bid_price_diff || r1.bid_levels_consumed != r2.bid_levels_consumed ||
           ask_price_diff || r1.ask_levels_consumed != r2.ask_levels_consumed;
}


int main(int argc, char *argv[]) {
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0] << " <date> <venue> <symbol> <target_quantity>" << std::endl;
        return 1;
    }

    std::string upper_venue = argv[2];
    std::transform(upper_venue.begin(), upper_venue.end(), upper_venue.begin(), ::toupper);

    std::string symbol_arg = argv[3];
    std::transform(symbol_arg.begin(), symbol_arg.end(), symbol_arg.begin(), ::toupper);

    std::string input_file_path = "/home/vir/" + std::string(argv[1]) + "/" + std::string(argv[2]) + "/books/" +
                                  upper_venue + ".book_tops." + symbol_arg + ".bin";
    uint32_t target_quantity = 0;
    try {
        unsigned long temp_qty = std::stoul(argv[4]);
        if (temp_qty == 0 || temp_qty > UINT32_MAX) {
             std::cerr << "Error: Target quantity must be a positive integer within uint32_t range and not zero." << std::endl;
            return 1;
        }
        target_quantity = static_cast<uint32_t>(temp_qty);
    } catch (const std::invalid_argument& ia) {
        std::cerr << "Error: Invalid target quantity (not a number): " << argv[4] << std::endl;
        return 1;
    } catch (const std::out_of_range& oor) {
        std::cerr << "Error: Target quantity out of range: " << argv[4] << std::endl;
        return 1;
    }

    std::ifstream input_file(input_file_path, std::ios::binary);
    if (!input_file.is_open()) {
        std::cerr << "Error: Could not open input file: " << input_file_path << std::endl;
        return 1;
    }

    Header header;
    input_file.read(reinterpret_cast<char *>(&header), sizeof(Header));
    if (input_file.gcount() < sizeof(Header)) {
        std::cerr << "Error: File is too small to contain a valid header or read error." << std::endl;
        input_file.close();
        return 1;
    }

    std::cout << "Processing file: " << input_file_path << std::endl;
    std::cout << "  Feed ID: " << header.feed_id << ", Date: " << header.dateint
              << ", Tops: " << header.number_of_tops << ", Symbol Idx: " << header.symbol_idx << std::endl;
    std::cout << "Target quantity for execution: " << target_quantity << std::endl;

    std::string path_str = input_file_path;
    size_t last_slash = path_str.rfind('/');
    std::string file_name_with_ext = (last_slash == std::string::npos) ? path_str : path_str.substr(last_slash + 1);
    
    std::string base_file_name_part;
    size_t last_dot_idx = file_name_with_ext.rfind('.');
    if (last_dot_idx != std::string::npos) {
        base_file_name_part = file_name_with_ext.substr(0, last_dot_idx);
    } else {
        base_file_name_part = file_name_with_ext;
    }

    std::string input_dir_path = (last_slash == std::string::npos) ? "" : path_str.substr(0, last_slash + 1);
    
    std::string impactbase_dir_path_str = input_dir_path + "impactbase";

    struct stat st = {0};
    if (stat(impactbase_dir_path_str.c_str(), &st) == -1) {
        if (errno == ENOENT) {
            if (mkdir(impactbase_dir_path_str.c_str(), 0775) == 0) {
                std::cout << "Created directory: " << impactbase_dir_path_str << std::endl;
            } else {
                std::cerr << "Error: Could not create directory '" << impactbase_dir_path_str << "'. Errno: " << errno << std::endl;
                input_file.close();
                return 1;
            }
        } else {
            std::cerr << "Error: Could not stat path '" << impactbase_dir_path_str << "'. Errno: " << errno << std::endl;
            input_file.close();
            return 1;
        }
    } else if (!S_ISDIR(st.st_mode)) {
        std::cerr << "Error: Path '" << impactbase_dir_path_str << "' exists but is not a directory." << std::endl;
        input_file.close();
        return 1;
    }
    
    std::string output_file_name = base_file_name_part + ".qty" + std::to_string(target_quantity) + ".results.bin";
    std::string output_file_path = impactbase_dir_path_str + "/" + output_file_name;

    std::ofstream output_file(output_file_path, std::ios::binary | std::ios::trunc);
    if (!output_file.is_open()) {
        std::cerr << "Error: Could not open output file: " << output_file_path << std::endl;
        input_file.close();
        return 1;
    }

    BookTop current_book_top;
    ExecutionResult last_written_exec_result; 
    bool first_record_to_write = true;
    long records_written = 0;
    uint32_t book_tops_processed = 0;

    for (book_tops_processed = 0; book_tops_processed < header.number_of_tops; ++book_tops_processed) {
        input_file.read(reinterpret_cast<char *>(&current_book_top), sizeof(BookTop));
        if (input_file.gcount() < sizeof(BookTop)) {
            std::cerr << "Warning: Could not read full BookTop entry " << book_tops_processed + 1 
                      << "/" << header.number_of_tops << ". Processed " << book_tops_processed << " entries." << std::endl;
            break; 
        }

        ExecutionResult current_exec_result;
        current_exec_result.timestamp = current_book_top.ts;
        current_exec_result.seqno = current_book_top.seqno;

        auto bid_details = calculate_side_execution(target_quantity, current_book_top.bid_price, current_book_top.bid_qty);
        current_exec_result.bid_exec_price = bid_details.first;
        current_exec_result.bid_levels_consumed = bid_details.second;

        auto ask_details = calculate_side_execution(target_quantity, current_book_top.ask_price, current_book_top.ask_qty);
        current_exec_result.ask_exec_price = ask_details.first;
        current_exec_result.ask_levels_consumed = ask_details.second;

        if (first_record_to_write || results_meaningfully_changed(last_written_exec_result, current_exec_result)) {
            output_file.write(reinterpret_cast<const char *>(&current_exec_result), sizeof(ExecutionResult));
            if (!output_file) {
                std::cerr << "Error: Failed to write to output file. Disk full or other I/O error?" << std::endl;
                input_file.close();
                output_file.close();
                return 1;
            }
            last_written_exec_result = current_exec_result;
            first_record_to_write = false;
            records_written++;
        }
    }

    input_file.close();
    output_file.close();
    
    std::cout << "Processing complete." << std::endl;
    std::cout << "Total BookTop entries processed: " << book_tops_processed << std::endl;
    std::cout << "Execution result records written: " << records_written << std::endl;
    std::cout << "Output written to: " << output_file_path << std::endl;

    return 0;
}