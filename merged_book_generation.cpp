#include <iostream>
#include <string>
#include <vector>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <regex>
#include <queue>
#include <set>
#include <iomanip>
#include <random>
#include <cstdlib>
#include <cstdio>
#include <cstdint>
#include <map>
#include <optional>
#include <future>
#include <mutex>
#include <atomic>
#include <functional>

#ifdef _WIN32
#else
#include <sys/wait.h>
#endif

namespace fs = std::filesystem;

// --- Constants ---
const size_t HEADER_SIZE = 24;
const std::string PYTHON_EXECUTABLE = "python";

#pragma pack(push, 1)

struct Header {
    uint64_t feed_id;
    uint32_t dateint;
    uint32_t count;
    uint64_t symbol_idx;
};
static_assert(sizeof(Header) == HEADER_SIZE, "Header size mismatch");

struct FillsRecord {
    uint64_t ts;
    uint64_t seq_no;
    uint64_t resting_order_id;
    bool was_hidden;
    int64_t trade_price;
    uint32_t trade_qty;
    uint64_t execution_id;
    uint32_t resting_original_qty;
    uint32_t resting_order_remaining_qty;
    uint64_t resting_order_last_update_ts;
    bool resting_side_is_bid;
    int64_t resting_side_price;
    uint32_t resting_side_qty;
    int64_t opposing_side_price;
    uint32_t opposing_side_qty;
    uint32_t resting_side_number_of_orders;
};
const size_t FILLS_RECORD_SIZE = sizeof(FillsRecord);
static_assert(sizeof(FillsRecord) == 90, "FillsRecord size mismatch");

struct top_level
{
    int64_t bid_nanos;
    int64_t ask_nanos;
    uint32_t bid_qty;
    uint32_t ask_qty;
};
static_assert(sizeof(top_level) == 24, "top_level size mismatch");

struct TopsRecord {
    uint64_t ts;
    uint64_t seqno;
    top_level first_level;
    top_level second_level;
    top_level third_level;
};
const size_t TOPS_RECORD_SIZE = sizeof(TopsRecord);
static_assert(sizeof(TopsRecord) == 88, "TopsRecord size mismatch");

#pragma pack(pop)

// --- Helper Functions ---
std::string to_lower_str(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c){ return std::tolower(c); });
    return s;
}

std::string to_upper_str(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c){ return std::toupper(c); });
    return s;
}

std::string get_user_input_date(std::mutex& console_mutex) {
    std::string date;
    {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cout << "Enter the date (e.g., YYYYMMDD): ";
    }
    std::cin >> date;
    return date;
}

std::vector<std::string> find_venue_folders(const fs::path& base_date_path, std::mutex& console_mutex) {
    std::vector<std::string> venue_folders;
    if (!fs::is_directory(base_date_path)) {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cerr << "Error: Base date directory not found: " << base_date_path << std::endl;
        return venue_folders;
    }
    try {
        for (const auto& entry : fs::directory_iterator(base_date_path)) {
            if (entry.is_directory()) {
                std::string dir_name = entry.path().filename().string();
                if (to_lower_str(dir_name) != "mergedbooks") {
                    venue_folders.push_back(dir_name);
                }
            }
        }
    } catch (const fs::filesystem_error& e) {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cerr << "Error iterating directory " << base_date_path << ": " << e.what() << std::endl;
    }
    return venue_folders;
}

std::optional<std::pair<uint64_t, std::vector<char>>>
read_next_record_with_timestamp_raw(std::ifstream& file_handle, size_t record_size) {
    if (record_size == 0) return std::nullopt;
    std::vector<char> record_data_bytes(record_size);
    file_handle.read(record_data_bytes.data(), record_size);

    if (static_cast<size_t>(file_handle.gcount()) < record_size) {
        return std::nullopt;
    }

    uint64_t timestamp;
    std::memcpy(&timestamp, record_data_bytes.data(), sizeof(uint64_t));
    return std::make_pair(timestamp, record_data_bytes);
}


// Structure for items in the min-heap
struct HeapItem {
    uint64_t timestamp;
    std::vector<char> record_data;
    size_t file_index;
    uint64_t feed_id;

    // Min-heap comparison
    bool operator>(const HeapItem& other) const {
        if (timestamp != other.timestamp) {
            return timestamp > other.timestamp;
        }
        return false;
    }
};

std::optional<fs::path> merge_files_for_symbol_by_timestamp(
    const fs::path& base_date_path,
    const std::vector<std::string>& venue_folders,
    const std::string& symbol,
    const std::string& file_type_suffix,
    const fs::path& merged_output_folder,
    std::mutex& console_mutex) {

    std::string merged_filename_key;
    if (file_type_suffix == "book_fills") {
        merged_filename_key = "fills";
    } else if (file_type_suffix == "book_tops") {
        merged_filename_key = "tops";
    } else {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cerr << "Error: Unknown file_type_suffix for merging: " << file_type_suffix << std::endl;
        return std::nullopt;
    }

    fs::path merged_filepath = merged_output_folder / ("merged_" + merged_filename_key + "." + symbol + ".bin");

    size_t record_size = 0;
    if (file_type_suffix == "book_fills") {
        record_size = FILLS_RECORD_SIZE;
    } else {
        record_size = TOPS_RECORD_SIZE;
    }

    std::vector<fs::path> source_files_to_process;
    for (const auto& venue : venue_folders) {
        fs::path books_folder_path = base_date_path / venue / "books";
        fs::path source_filepath = books_folder_path / (to_upper_str(venue) + "." + file_type_suffix + "." + symbol + ".bin");

        if (fs::exists(source_filepath) && fs::is_regular_file(source_filepath)) {
            try {
                if (fs::file_size(source_filepath) >= HEADER_SIZE) {
                    source_files_to_process.push_back(source_filepath);
                } else {
                    std::lock_guard<std::mutex> lock(console_mutex);
                    std::cout << "  Skipping small file (less than header size): " << source_filepath << std::endl;
                }
            } catch (const fs::filesystem_error& e) {
                std::lock_guard<std::mutex> lock(console_mutex);
                std::cerr << "  Error checking file size for " << source_filepath << ": " << e.what() << std::endl;
            }
        }
    }

    if (source_files_to_process.empty()) {
        return std::nullopt;
    }

    std::vector<std::unique_ptr<std::ifstream>> file_streams;
    file_streams.reserve(source_files_to_process.size());

    std::optional<Header> first_valid_header_data_opt;
    std::priority_queue<HeapItem, std::vector<HeapItem>, std::greater<HeapItem>> min_heap;
    uint32_t total_records_merged = 0;

    for (size_t i = 0; i < source_files_to_process.size(); ++i) {
        const auto& source_filepath = source_files_to_process[i];
        auto ifs = std::make_unique<std::ifstream>(source_filepath, std::ios::binary);
        if (!ifs->is_open()) {
            std::lock_guard<std::mutex> lock(console_mutex);
            std::cerr << "  Failed to open source file: " << source_filepath << std::endl;
            continue;
        }

        Header current_header;
        ifs->read(reinterpret_cast<char*>(&current_header), sizeof(Header));
        if (static_cast<size_t>(ifs->gcount()) < sizeof(Header)) {
            std::lock_guard<std::mutex> lock(console_mutex);
            std::cerr << "  Failed to read header from: " << source_filepath << std::endl;
            continue;
        }

        if (!first_valid_header_data_opt) {
            first_valid_header_data_opt = current_header;
        }
        file_streams.push_back(std::move(ifs));
        size_t current_file_idx = file_streams.size() - 1;

        auto next_item_opt = read_next_record_with_timestamp_raw(*file_streams.back(), record_size);
        if (next_item_opt) {
            min_heap.push({next_item_opt->first, next_item_opt->second, current_file_idx, current_header.feed_id});
        }
    }
    
    if (min_heap.empty() || !first_valid_header_data_opt) {
        return std::nullopt;
    }

    std::ofstream merged_file_handle(merged_filepath, std::ios::binary | std::ios::trunc);
    if (!merged_file_handle.is_open()) {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cerr << "  Failed to open merged output file: " << merged_filepath << std::endl;
        return std::nullopt;
    }

    std::vector<char> null_header(HEADER_SIZE, 0);
    merged_file_handle.write(null_header.data(), HEADER_SIZE);

    while (!min_heap.empty()) {
        HeapItem current_item = min_heap.top();
        min_heap.pop();

        merged_file_handle.write(reinterpret_cast<const char*>(&current_item.feed_id), sizeof(uint64_t));
        merged_file_handle.write(current_item.record_data.data(), current_item.record_data.size());
        total_records_merged++;

        auto next_item_opt = read_next_record_with_timestamp_raw(*file_streams[current_item.file_index], record_size);
        if (next_item_opt) {
            min_heap.push({next_item_opt->first, next_item_opt->second, current_item.file_index, current_item.feed_id});
        }
    }

    for(auto& stream_ptr : file_streams) {
        if(stream_ptr && stream_ptr->is_open()) {
            stream_ptr->close();
        }
    }
    file_streams.clear();


    if (total_records_merged > 0) {
        Header final_header = first_valid_header_data_opt.value();
        final_header.count = total_records_merged;
        
        merged_file_handle.seekp(0, std::ios::beg);
        merged_file_handle.write(reinterpret_cast<const char*>(&final_header), sizeof(Header));
        merged_file_handle.close();
        {
            std::lock_guard<std::mutex> lock(console_mutex);
            std::cout << "  Successfully merged " << file_type_suffix << " for " << symbol 
                      << " into: " << merged_filepath << " (" << total_records_merged << " records)" << std::endl;
        }
        return merged_filepath;
    } else {
        merged_file_handle.close();
        try {
            if (fs::exists(merged_filepath) && fs::file_size(merged_filepath) == HEADER_SIZE) {
                std::ifstream temp_check(merged_filepath, std::ios::binary);
                std::vector<char> content(HEADER_SIZE);
                temp_check.read(content.data(), HEADER_SIZE);
                temp_check.close();
                if (std::all_of(content.begin(), content.end(), [](char c){ return c == 0; })) {
                    fs::remove(merged_filepath);
                     {
                        std::lock_guard<std::mutex> lock(console_mutex);
                        std::cout << "  Removed empty merged file: " << merged_filepath << std::endl;
                     }
                }
            }
        } catch (const fs::filesystem_error& e) {
            std::lock_guard<std::mutex> lock(console_mutex);
            std::cerr << "  Error during cleanup of empty merged file " << merged_filepath << ": " << e.what() << std::endl;
        }
        return std::nullopt;
    }
}


std::vector<std::string> extract_symbols_from_all_venues(
    const fs::path& base_date_path,
    const std::vector<std::string>& venue_folders,
    std::mutex& console_mutex) {
    
    std::set<std::string> symbols_set;
    std::regex symbol_pattern(R"(^[A-Z0-9_-]+\.(?:book_fills|book_tops)\.([A-Z0-9_^+=-]+)\.bin$)", std::regex::icase);

    for (const auto& venue : venue_folders) {
        fs::path books_folder_path = base_date_path / venue / "books";
        if (!fs::is_directory(books_folder_path)) {
            continue;
        }
        try {
            for (const auto& entry : fs::directory_iterator(books_folder_path)) {
                if (entry.is_regular_file()) {
                    std::string filename = entry.path().filename().string();
                    std::smatch match;
                    if (std::regex_match(filename, match, symbol_pattern)) {
                        if (match.size() > 1) {
                            symbols_set.insert(to_upper_str(match[1].str()));
                        }
                    }
                }
            }
        } catch (const fs::filesystem_error& e) {
            std::lock_guard<std::mutex> lock(console_mutex);
            std::cerr << "Error iterating directory " << books_folder_path << ": " << e.what() << std::endl;
        }
    }
    
    std::vector<std::string> sorted_symbols(symbols_set.begin(), symbols_set.end());
    return sorted_symbols;
}

struct MergedFileInfo {
    fs::path path;
    std::string type;
};

// Function to execute a command and capture its output
std::pair<int, std::string> execute_command_and_get_output(const std::string& command, std::mutex& console_mutex) {
    std::string output_str;
    int return_code = -1;

#ifndef _WIN32
    FILE* pipe = popen((command + " 2>&1").c_str(), "r");
    if (!pipe) {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cerr << "popen() failed for command: " << command << std::endl;
        return {return_code, "popen() failed!"};
    }

    char buffer[256];
    while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
        output_str += buffer;
    }

    int status = pclose(pipe);
    if (WIFEXITED(status)) {
        return_code = WEXITSTATUS(status);
    } else {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cerr << "Command exited abnormally or was signaled: " << command << std::endl;
    }
#else
    {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cerr << "execute_command_and_get_output is not fully implemented for Windows in this example." << std::endl;
        std::cerr << "Falling back to std::system without output capture for command: " << command << std::endl;
    }
    return_code = std::system(command.c_str()); // This won't capture output
    output_str = "Output capture not implemented on Windows in this example for std::system.";
#endif
    return {return_code, output_str};
}

// Worker task for merging files for a single symbol
std::vector<MergedFileInfo> process_symbol_task(
    const std::string& symbol,
    const fs::path& base_date_path,
    const std::vector<std::string>& venue_folders,
    const fs::path& merged_output_folder,
    std::mutex& console_mutex,
    size_t symbol_idx,
    size_t total_symbols) {

    {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cout << "\n[" << (symbol_idx + 1) << "/" << total_symbols << "] Processing symbol: " << symbol << std::endl;
    }
    
    std::vector<MergedFileInfo> results;
    auto merged_fills_path_opt = merge_files_for_symbol_by_timestamp(base_date_path, venue_folders, symbol, "book_fills", merged_output_folder, console_mutex);
    if (merged_fills_path_opt) {
        results.push_back({merged_fills_path_opt.value(), "fills"});
    }

    auto merged_tops_path_opt = merge_files_for_symbol_by_timestamp(base_date_path, venue_folders, symbol, "book_tops", merged_output_folder, console_mutex);
    if (merged_tops_path_opt) {
        results.push_back({merged_tops_path_opt.value(), "tops"});
    }
    return results;
}

// Worker task for running a single test
bool run_test_task(
    const MergedFileInfo& file_info,
    const fs::path& test_script_path,
    std::mutex& console_mutex) {
    
    std::string command_to_run;
    {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cout << "\nPreparing test for: " << file_info.path
                  << " (type: " << file_info.type << ")" << std::endl;
        
        std::ostringstream command_stream;
        command_stream << PYTHON_EXECUTABLE << " "
                       << "\"" << test_script_path.string() << "\""
                       << " --filepath " << "\"" << file_info.path.string() << "\""
                       << " --type " << file_info.type;
        command_to_run = command_stream.str();
        std::cout << "Executing: " << command_to_run << std::endl;
    }

    auto [return_code, output] = execute_command_and_get_output(command_to_run, console_mutex);
    
    {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cout << "--- Test Script Output for " << file_info.path.filename().string() << " ---" << std::endl;
        if (!output.empty()) {
            std::cout << output << std::endl;
        }
        
        if (return_code == 0) {
            std::cout << "PASS: Test script exited successfully for " << file_info.path.string() << "." << std::endl;
            return true;
        } else {
            std::cout << "FAIL: Test script exited with error code " << return_code 
                      << " for " << file_info.path.string() << "." << std::endl;
            return false;
        }
    }
}


int main() {
    std::mutex console_mutex;

    std::string date_str = get_user_input_date(console_mutex);
    fs::path base_date_path = fs::path("/home/vir") / date_str;
    fs::path merged_output_folder = base_date_path / "mergedbooks";
    
    fs::path current_executable_dir;
    try {
        current_executable_dir = fs::current_path();
    } catch (const fs::filesystem_error& e) {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cerr << "Warning: Could not get current path: " << e.what() << ". Assuming test script is accessible." << std::endl;
    }
    fs::path test_script_path = current_executable_dir / "test_merged_book.py";


    if (!fs::is_directory(base_date_path)) {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cerr << "Error: Date directory '" << base_date_path << "' does not exist." << std::endl;
        return 1;
    }

    try {
        fs::create_directories(merged_output_folder);
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cout << "Ensured output directory exists: " << merged_output_folder << std::endl;
    } catch (const fs::filesystem_error& e) {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cerr << "Error creating output directory '" << merged_output_folder << "': " << e.what() << std::endl;
        return 1;
    }

    std::vector<std::string> venue_folders = find_venue_folders(base_date_path, console_mutex);
    if (venue_folders.empty()) {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cout << "No venue folders found in '" << base_date_path << "'. Exiting." << std::endl;
        return 0;
    }
    {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cout << "Found venue folders: ";
        for (size_t i = 0; i < venue_folders.size(); ++i) {
            std::cout << venue_folders[i] << (i == venue_folders.size() - 1 ? "" : ", ");
        }
        std::cout << std::endl;
    }

    std::vector<std::string> all_symbols = extract_symbols_from_all_venues(base_date_path, venue_folders, console_mutex);
    if (all_symbols.empty()) {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cout << "No symbols found across any venues in '" << base_date_path << "'. Exiting." << std::endl;
        return 0;
    }
    {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cout << "Found " << all_symbols.size() << " unique symbols to process." << std::endl;
    }
    
    std::vector<std::future<std::vector<MergedFileInfo>>> symbol_futures;
    for (size_t i = 0; i < all_symbols.size(); ++i) {
        const auto& symbol = all_symbols[i];
        symbol_futures.push_back(
            std::async(std::launch::async, process_symbol_task,
                       symbol, base_date_path, std::ref(venue_folders), merged_output_folder,
                       std::ref(console_mutex), i, all_symbols.size())
        );
    }

    std::vector<MergedFileInfo> successfully_merged_files_info;
    for (auto& fut : symbol_futures) {
        std::vector<MergedFileInfo> symbol_results = fut.get();
        successfully_merged_files_info.insert(successfully_merged_files_info.end(),
                                              symbol_results.begin(), symbol_results.end());
    }


    {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cout << "\nBatch merging script finished." << std::endl;
    }

    // --- Random Testing Phase ---
    if (successfully_merged_files_info.empty()) {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cout << "\nNo merged files were created to test." << std::endl;
        return 0;
    }

    if (!fs::exists(test_script_path) || !fs::is_regular_file(test_script_path)) {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cerr << "\nError: Test script not found or not a file at " << test_script_path << ". Skipping testing phase." << std::endl;
        return 0;
    }

    size_t num_total_merged = successfully_merged_files_info.size();
    size_t num_files_to_test = std::max(1UL, static_cast<size_t>(num_total_merged * 0.05));
    if (num_files_to_test > num_total_merged) {
        num_files_to_test = num_total_merged;
    }
    
    {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cout << "\n--- Starting Random Testing Phase (calling " << test_script_path.filename().string() << ") ---" << std::endl;
        std::cout << "Will test " << num_files_to_test << " out of " << num_total_merged << " successfully merged files." << std::endl;
    }

    std::vector<MergedFileInfo> files_to_test_sample;
    std::mt19937 rng(std::random_device{}());
    std::sample(successfully_merged_files_info.begin(), 
                successfully_merged_files_info.end(),
                std::back_inserter(files_to_test_sample),
                num_files_to_test,
                rng);
    
    std::vector<std::future<bool>> test_futures;
    for (const auto& file_info : files_to_test_sample) {
        test_futures.push_back(
            std::async(std::launch::async, run_test_task,
                       file_info, test_script_path, std::ref(console_mutex))
        );
    }

    std::atomic<int> tests_passed_count = 0;
    for(auto& fut : test_futures) {
        if (fut.get()) {
            tests_passed_count++;
        }
    }
    
    bool overall_random_tests_passed = (static_cast<size_t>(tests_passed_count.load()) == files_to_test_sample.size());

    // Final summary output
    {
        std::lock_guard<std::mutex> lock(console_mutex);
        if (overall_random_tests_passed && !files_to_test_sample.empty()) {
            std::cout << "\n======================================================" << std::endl;
            std::cout << "All " << tests_passed_count.load() << " randomly selected external tests passed successfully!" << std::endl;
            std::cout << "======================================================" << std::endl;
        } else if (!files_to_test_sample.empty()) {
            std::cout << "\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << std::endl;
            std::cout << (tests_passed_count.load()) << " out of " << files_to_test_sample.size() 
                      << " randomly selected external tests passed. Some FAILED." << std::endl;
            std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!" << std::endl;
        } else if (files_to_test_sample.empty() && num_total_merged > 0) {
             std::cout << "\nNo files were selected for random testing (sample size was 0)." << std::endl;
        }
         else {
            std::cout << "\nNo tests were run as no files were selected or available for testing." << std::endl;
        }
    }

    return overall_random_tests_passed || files_to_test_sample.empty() ? 0 : 1;
}