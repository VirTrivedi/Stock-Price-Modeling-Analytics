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
#include <thread>
#include <mutex>
#include <atomic>
#include <future>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#pragma pack(push, 1)
struct FillsBarRecord {
    uint64_t timestamp_sec;
    double high;
    double low;
    double open;
    double close;
    int32_t volume;
};

struct TopsBarRecord {
    uint64_t timestamp_sec;
    double high;
    double low;
    double open;
    double close;
};
#pragma pack(pop)

extern const size_t MIN_DATA_LENGTH = 10;

namespace fs = std::filesystem;

std::vector<double> read_fills_bar_file(const std::string& file_path);
std::vector<double> read_tops_bar_file(const std::string& file_path);

std::map<std::string, bool> file_exists_cache;
std::mutex file_exists_mutex;
std::map<std::string, std::vector<double>> file_data_cache;
std::mutex file_cache_mutex;

// String utility functions
std::string to_upper(const std::string& input) {
    std::string result = input;
    std::transform(result.begin(), result.end(), result.begin(),
                  [](unsigned char c){ return ::toupper(c); });
    return result;
}

std::string to_lower(const std::string& input) {
    std::string result = input;
    std::transform(result.begin(), result.end(), result.begin(),
                  [](unsigned char c){ return std::tolower(c); });
    return result;
}

// Weighted correlation calculation
std::optional<double> calculate_weighted_correlation(
    const std::vector<std::optional<double>>& correlations,
    const std::vector<double>& weights) {
    
    if (correlations.empty() || correlations.size() != weights.size()) {
        return std::nullopt;
    }
    
    double sum_weighted = 0.0;
    double sum_weights = 0.0;
    
    for (size_t i = 0; i < correlations.size(); ++i) {
        if (correlations[i].has_value()) {
            sum_weighted += correlations[i].value() * weights[i];
            sum_weights += weights[i];
        }
    }
    
    if (sum_weights < 0.0000001) {
        return std::nullopt;
    }
    
    return sum_weighted / sum_weights;
}

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

// Check if a file exists and is not empty
bool file_exists_with_cache(const std::string& path) {
    {
        std::lock_guard<std::mutex> lock(file_exists_mutex);
        auto it = file_exists_cache.find(path);
        if (it != file_exists_cache.end()) {
            return it->second;
        }
    }
    
    bool exists = fs::exists(path) && fs::file_size(path) > 0;
    
    {
        std::lock_guard<std::mutex> lock(file_exists_mutex);
        file_exists_cache[path] = exists;
    }
    
    return exists;
}

// Check if all required files for a symbol contain sufficient data
bool is_symbol_valid_cpp(const std::string& base_path_for_feed, const std::string& symbol) {
    auto paths = generate_file_paths_cpp(base_path_for_feed, symbol);
    if (paths.empty()) return false;

    // First check if all files exist and have non-zero size
    for (const auto& [key, path] : paths) {
        if (!file_exists_with_cache(path)) {
            return false;
        }
    }

    // Check each file's data length individually with early return
    auto check_file = [](const std::string& path, bool is_fills) -> bool {
        auto data = is_fills ? read_fills_bar_file(path) : read_tops_bar_file(path);
        return data.size() >= MIN_DATA_LENGTH;
    };
    
    if (!check_file(paths.at("fills"), true)) return false;
    if (!check_file(paths.at("L1_bid"), false)) return false;
    if (!check_file(paths.at("L1_ask"), false)) return false;
    if (!check_file(paths.at("L2_bid"), false)) return false;
    if (!check_file(paths.at("L2_ask"), false)) return false;
    if (!check_file(paths.at("L3_bid"), false)) return false;
    if (!check_file(paths.at("L3_ask"), false)) return false;

    return true;
}

// Memory-mapped batch file reader
std::vector<double> read_file_mmap_cached(const std::string& file_path, bool is_fills) {
    // Check cache first
    {
        std::lock_guard<std::mutex> lock(file_cache_mutex);
        auto it = file_data_cache.find(file_path);
        if (it != file_data_cache.end()) {
            return it->second;
        }
    }
    
    std::vector<double> prices;
    int fd = open(file_path.c_str(), O_RDONLY);
    if (fd == -1) return prices;
    
    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        close(fd);
        return prices;
    }
    
    size_t file_size = sb.st_size;
    if (file_size == 0) {
        close(fd);
        return prices;
    }
    
    // Map the entire file
    void* mapped = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mapped == MAP_FAILED) {
        close(fd);
        return prices;
    }
    
    // Calculate record size and number based on file type
    size_t record_size = is_fills ? sizeof(FillsBarRecord) : sizeof(TopsBarRecord);
    size_t price_offset = is_fills ? offsetof(FillsBarRecord, close) : offsetof(TopsBarRecord, close);
    size_t num_records = file_size / record_size;
    
    prices.reserve(num_records);
    const char* data = static_cast<const char*>(mapped);
    
    // Extract prices in a single sweep
    for (size_t i = 0; i < num_records; ++i) {
        double price = *reinterpret_cast<const double*>(data + i * record_size + price_offset);
        prices.push_back(price);
    }
    
    munmap(mapped, file_size);
    close(fd);
    
    // Cache the result
    {
        std::lock_guard<std::mutex> lock(file_cache_mutex);
        // Only cache if not too large (prevent memory issues)
        if (prices.size() < 100000) {
            file_data_cache[file_path] = prices;
        }
    }
    
    return prices;
}

std::vector<double> read_fills_bar_file(const std::string& file_path) {
    return read_file_mmap_cached(file_path, true);
}

std::vector<double> read_tops_bar_file(const std::string& file_path) {
    return read_file_mmap_cached(file_path, false);
}


std::optional<double> calculate_file_correlation(const std::string& file1, const std::string& file2, bool is_fills) {
    auto data1 = is_fills ? read_fills_bar_file(file1) : read_tops_bar_file(file1);
    auto data2 = is_fills ? read_fills_bar_file(file2) : read_tops_bar_file(file2);
    
    if (data1.empty() || data2.empty()) {
        return std::nullopt;
    }
    
    // Use the smallest size
    size_t n = std::min(data1.size(), data2.size());
    if (n < 10) return std::nullopt;
    
    // Optimize calculation using vector operations
    double sum_x = 0, sum_y = 0;
    double sum_xy = 0, sum_x2 = 0, sum_y2 = 0;
    
    // Use block processing to improve cache locality
    const size_t BLOCK_SIZE = 128;
    
    for (size_t block = 0; block < n; block += BLOCK_SIZE) {
        size_t end = std::min(block + BLOCK_SIZE, n);
        
        for (size_t i = block; i < end; i++) {
            double x = data1[i];
            double y = data2[i];
            sum_x += x;
            sum_y += y;
            sum_xy += x * y;
            sum_x2 += x * x;
            sum_y2 += y * y;
        }
    }
    
    double denominator = std::sqrt((n * sum_x2 - sum_x * sum_x) * (n * sum_y2 - sum_y * sum_y));
    if (denominator < 0.0000001) return std::nullopt;
    
    double correlation = (n * sum_xy - sum_x * sum_y) / denominator;
    return correlation;
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
    std::mutex results_mutex;
    
    if (valid_symbols.size() < 2) {
        return results;
    }

    size_t total_pairs = valid_symbols.size() * (valid_symbols.size() - 1) / 2;
    std::atomic<size_t> current_pair = 0;
    
    // Determine number of threads to use
    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 12;
    if (num_threads > 2) num_threads -= 2;
    
    // Calculate optimal batch size based on the number of pairs and threads
    const size_t BATCH_SIZE = std::max<size_t>(32, std::min<size_t>(256, total_pairs / (num_threads * 16)));
    
    std::cout << "Processing with " << num_threads << " threads and batch size " << BATCH_SIZE << "..." << std::endl;
    
    // Create a thread pool
    std::vector<std::future<void>> futures;
    
    std::atomic<size_t> completed_pairs = 0;
    auto start_time = std::chrono::high_resolution_clock::now();
    size_t report_interval = 10000;
    std::mutex status_mutex;

    // Batch processing worker function for better load balancing
    
    auto worker = [&]() {
        std::vector<CorrelationResult> local_results;
        std::map<std::string, std::map<std::string, std::vector<double>>> symbol_data_cache;
        size_t local_completed = 0;
        
        while (true) {
            // Get next batch of pairs to process
            size_t batch_start = current_pair.fetch_add(BATCH_SIZE);
            if (batch_start >= total_pairs) break;
            
            size_t batch_end = std::min(batch_start + BATCH_SIZE, total_pairs);
            
            // Process batch of pairs
            for (size_t pair_idx = batch_start; pair_idx < batch_end; ++pair_idx) {
                // Convert linear index to i,j coordinates
                size_t i = 0;
                size_t temp_pair_idx = pair_idx;
                size_t row_size = valid_symbols.size() - 1;
                
                while (temp_pair_idx >= row_size) {
                    temp_pair_idx -= row_size;
                    i++;
                    row_size--;
                }
                size_t j = i + 1 + temp_pair_idx;
                
                const std::string& sym1 = valid_symbols[i];
                const std::string& sym2 = valid_symbols[j];
                
                // Generate file paths only once and reuse
                auto paths1 = generate_file_paths_cpp(base_path_for_feed, sym1);
                auto paths2 = generate_file_paths_cpp(base_path_for_feed, sym2);
                
                // Load data for each file type (fills, L1_bid, etc.)
                const std::vector<std::string> file_types = {"fills", "L1_bid", "L1_ask", "L2_bid", "L2_ask", "L3_bid", "L3_ask"};
                const std::vector<bool> is_fills_type = {true, false, false, false, false, false, false};
                
                std::vector<std::optional<double>> correlations;
                correlations.reserve(file_types.size());
                
                for (size_t idx = 0; idx < file_types.size(); ++idx) {
                    const std::string& type = file_types[idx];
                    bool is_fills = is_fills_type[idx];
                    
                    correlations.push_back(calculate_file_correlation(paths1.at(type), paths2.at(type), is_fills));
                }
                
                std::vector<double> weights(correlations.size(), 0.125);
                
                std::optional<double> overall_opt = calculate_weighted_correlation(correlations, weights);
                
                if (overall_opt.has_value()) {
                    local_results.push_back({sym1, sym2, std::round(overall_opt.value() * 10000.0) / 10000.0});
                }
                
                // Update local completion count
                local_completed++;
            }
            
            // After processing the batch, update the global counter
            size_t total_completed = completed_pairs.fetch_add(local_completed);
            total_completed += local_completed;
            local_completed = 0; // Reset for the next batch
            
            if (total_completed % report_interval == 0 || (total_completed + local_completed) % report_interval == 0) {
                std::lock_guard<std::mutex> status_lock(status_mutex);
                auto current_time = std::chrono::high_resolution_clock::now();
                auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(current_time - start_time).count();
                
                double progress = static_cast<double>(total_completed) / total_pairs * 100.0;
                double pairs_per_second = elapsed > 0 ? static_cast<double>(total_completed) / elapsed : 0;
                
                // Estimate completion time
                double remaining_pairs = total_pairs - total_completed;
                double estimated_seconds = pairs_per_second > 0 ? remaining_pairs / pairs_per_second : 0;
                
                int hours = static_cast<int>(estimated_seconds) / 3600;
                int minutes = (static_cast<int>(estimated_seconds) % 3600) / 60;
                int seconds = static_cast<int>(estimated_seconds) % 60;
                
                std::cout << "\n--- STATUS UPDATE ---" << std::endl;
                std::cout << "Completed: " << total_completed << " of " << total_pairs 
                        << " pairs (" << std::fixed << std::setprecision(2) << progress << "%)" << std::endl;
                std::cout << "Elapsed time: " << elapsed << " seconds" << std::endl;
                std::cout << "Processing speed: " << std::fixed << std::setprecision(2) 
                        << pairs_per_second << " pairs/second" << std::endl;
                std::cout << "Estimated time remaining: " << hours << "h " << minutes << "m " << seconds << "s" << std::endl;
                std::cout << "---------------------" << std::endl;
            }
        }
        
        // Add local results to global results
        std::lock_guard<std::mutex> lock(results_mutex);
        results.insert(results.end(), local_results.begin(), local_results.end());
    };

    // Launch worker threads
    for (unsigned int i = 0; i < num_threads; i++) {
        futures.push_back(std::async(std::launch::async, worker));
    }
    
    // Wait for all threads to complete
    for (auto& future : futures) {
        future.wait();
    }
    

    auto end_time = std::chrono::high_resolution_clock::now();
    auto total_seconds = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count();
        
    std::cout << "\n--- FINAL STATUS ---" << std::endl;
    std::cout << "Completed all " << total_pairs << " pairs" << std::endl;
    std::cout << "Total time: " << total_seconds << " seconds" << std::endl;
    if (total_seconds > 0) {
        std::cout << "Average processing speed: " << std::fixed << std::setprecision(2) 
                << static_cast<double>(total_pairs) / total_seconds << " pairs/second" << std::endl;
    }
    std::cout << "-------------------" << std::endl;

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

    fs::path base_folder = fs::path("/data") / date_str / to_lower(feed_str) / "bars";
    std::string base_path_for_feed = (base_folder / to_upper(feed_str)).string();

    if (!fs::exists(base_folder) || !fs::is_directory(base_folder)) {
        std::cerr << "Error: Base folder for bars not found or is not a directory: " << base_folder.string() << std::endl;
        return 1;
    }
    
    std::cout << "Finding symbols in " << base_folder.string() << "..." << std::endl;
    std::vector<std::string> all_symbols = extract_symbols_from_folder(base_folder);

    std::cout << "Found " << all_symbols.size() << " unique symbols. Validating data files in parallel..." << std::endl;
    std::vector<std::string> valid_symbols;
    std::vector<std::string> invalid_symbols;
    std::mutex valid_mutex, invalid_mutex;

    // Create a thread pool for validation
    unsigned int validation_threads = std::thread::hardware_concurrency();
    if (validation_threads == 0) validation_threads = 12;
    if (validation_threads > 2) validation_threads -= 2;
    validation_threads = std::min(validation_threads, static_cast<unsigned int>(all_symbols.size()));

    std::vector<std::future<void>> validation_futures;
    std::atomic<size_t> processed_count = 0;
    size_t total_count = all_symbols.size();
    auto validation_start_time = std::chrono::high_resolution_clock::now();

    // Create a validation worker function
    auto validation_worker = [&](size_t start, size_t end) {
        std::vector<std::string> local_valid, local_invalid;
        
        for (size_t i = start; i < end && i < all_symbols.size(); ++i) {
            const auto& symbol = all_symbols[i];
            
            if (is_symbol_valid_cpp(base_path_for_feed, symbol)) {
                local_valid.push_back(symbol);
            } else {
                local_invalid.push_back(symbol);
            }
            
            // Update counter and show progress
            size_t completed = processed_count.fetch_add(1) + 1;
            if (completed % 10 == 0 || completed == total_count) {
                double percent = static_cast<double>(completed) * 100.0 / total_count;
                std::cout << "Validating: " << completed << "/" << total_count 
                        << " symbols (" << std::fixed << std::setprecision(1) << percent << "%)   \r" << std::flush;
            }
        }
        
        // Add local results to global results with mutex protection
        {
            std::lock_guard<std::mutex> lock(valid_mutex);
            valid_symbols.insert(valid_symbols.end(), local_valid.begin(), local_valid.end());
        }
        {
            std::lock_guard<std::mutex> lock(invalid_mutex);
            invalid_symbols.insert(invalid_symbols.end(), local_invalid.begin(), local_invalid.end());
        }
    };

    // Distribute work to threads
    std::cout << "Validating with " << validation_threads << " threads..." << std::endl;
    size_t symbols_per_thread = (all_symbols.size() + validation_threads - 1) / validation_threads;

    for (unsigned int i = 0; i < validation_threads; i++) {
        size_t start = i * symbols_per_thread;
        size_t end = std::min(start + symbols_per_thread, all_symbols.size());
        validation_futures.push_back(std::async(std::launch::async, validation_worker, start, end));
    }

    // Wait for validation to complete
    for (auto& future : validation_futures) {
        future.wait();
    }

    auto validation_end_time = std::chrono::high_resolution_clock::now();
    auto validation_seconds = std::chrono::duration_cast<std::chrono::seconds>(validation_end_time - validation_start_time).count();

    std::cout << std::endl;
    std::cout << "Validation complete in " << validation_seconds << " seconds." << std::endl;
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