#include <iostream>
#include <string>
#include <vector>
#include <filesystem>
#include <cstdlib>
#include <sstream>
#include <algorithm>
#include <future>
#include <mutex>
#include <atomic>
#include <optional>
#include <deque>
#include <thread>

const std::string HISTBOOK_EXECUTABLE = "/home/vir/histbook/build/bin/HistBook";
const std::string PARSE_FILLS_EXECUTABLE = "./parse_book_fills"; 
const std::string PROCESS_TOPS_EXECUTABLE = "./process_tops";
const std::string PARSE_MERGED_TOPS_EXECUTABLE = "./parse_merged_tops";
const unsigned int MAX_CONCURRENT_TASKS = std::max(1u, std::thread::hardware_concurrency());
namespace fs = std::filesystem;

// Helper function to convert string to lowercase
std::string to_lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c){ return std::tolower(c); });
    return s;
}

// Helper function to convert string to uppercase
std::string to_upper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c){ return std::toupper(c); });
    return s;
}

// Helper function to split a string by a delimiter
std::vector<std::string> split_string(const std::string& s, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(s);
    while (std::getline(tokenStream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

// Function to execute a command and check its status
bool run_command(const std::string& command, std::mutex& console_mutex, const std::string& task_description) {
    {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cout << "Task [" << task_description << "]: Executing: " << command << std::endl;
    }
    int status = std::system(command.c_str());
    if (status != 0) {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cerr << "Task [" << task_description << "]: Error: Command failed with status " << status << ": " << command << std::endl;
        return false;
    }
    return true;
}

// Worker task for process_to_books
bool histbook_task(const fs::path& input_file_path, const fs::path& output_folder, std::mutex& console_mutex) {
    std::string file_name = input_file_path.filename().string();
    {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cout << "Processing raw file into book: " << file_name << std::endl;
    }
    
    std::ostringstream command_stream;
    command_stream << "\"" << HISTBOOK_EXECUTABLE << "\""
                   << " --outputpath \"" << output_folder.string() << "/\""
                   << " --inputpath \"" << input_file_path.string() << "\"";
    
    if (!run_command(command_stream.str(), console_mutex, "HistBook: " + file_name)) {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cerr << "Failed to process raw file with HistBook: " << file_name << std::endl;
        return false;
    }
    return true;
}

void process_to_books(const fs::path& base_folder_path) {
    std::cout << "\n--- Processing raw files to books ---" << std::endl;
    fs::path input_folder = base_folder_path;
    fs::path output_folder = base_folder_path / "books";
    std::mutex console_mutex;
    std::deque<std::future<bool>> futures;
    int success_count = 0;
    int failure_count = 0;

    try {
        fs::create_directories(output_folder);
    } catch (const fs::filesystem_error& e) {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cerr << "Error creating directory " << output_folder << ": " << e.what() << std::endl;
        return;
    }

    if (!fs::is_directory(input_folder)) {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cerr << "Error: " << input_folder << " is not a valid directory." << std::endl;
        return;
    }

    for (const auto& entry : fs::directory_iterator(input_folder)) {
        if (entry.is_regular_file()) {
            std::string file_name = entry.path().filename().string();
            if (file_name.rfind(".bin") != std::string::npos && file_name.find("book_events") != std::string::npos) {
                while (futures.size() >= MAX_CONCURRENT_TASKS) {
                    try {
                        if (futures.front().get()) {
                            success_count++;
                        } else {
                            failure_count++;
                        }
                    } catch (const std::exception& e) {
                        std::lock_guard<std::mutex> lock(console_mutex);
                        std::cerr << "Exception while getting future result (histbook_task): " << e.what() << std::endl;
                        failure_count++;
                    }
                    futures.pop_front();
                }
                futures.push_back(
                    std::async(std::launch::async, histbook_task, entry.path(), output_folder, std::ref(console_mutex))
                );
            }
        }
    }

    for (auto& fut : futures) {
        if (fut.get()) {
            success_count++;
        } else {
            failure_count++;
        }
    }
    std::cout << "--- Finished processing raw files to books. Success: " << success_count << ", Failed: " << failure_count << " ---" << std::endl;
}

// Generic worker task for generating bars
bool generate_bars_for_file_task(
    const fs::path& input_file_to_process,
    const std::string& bar_executable_path,
    const std::string& date_str,
    const std::string& symbol_str,
    const std::optional<std::string>& feed_for_executable,
    const std::string& log_file_type_description,
    std::mutex& console_mutex) {
    
    std::string processing_file_name = input_file_to_process.filename().string();
    std::ostringstream command_stream;
    std::string task_description_log;

    // Initial log output
    {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cout << "Preparing to generate bars from " << log_file_type_description << " file: " << processing_file_name << std::endl;
    }

    task_description_log = bar_executable_path + " for " + processing_file_name;
    command_stream << "\"" << bar_executable_path << "\""
                   << " " << date_str;
    if (feed_for_executable) {
        command_stream << " " << *feed_for_executable;
    }
    command_stream << " " << symbol_str;
    
    if (!run_command(command_stream.str(), console_mutex, task_description_log)) {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cerr << "Failed to generate bars from " << log_file_type_description << " file: " << processing_file_name << std::endl;
        return false;
    }
    return true;
}

// Processes files (either from 'books' or 'mergedbooks') to generate bars
void process_files_to_bars(
    const fs::path& context_path,
    const std::string& date_str,
    const std::string& feed_or_mode_str
) {
    bool is_merged_flow = (to_lower(feed_or_mode_str) == "mergedbooks");
    fs::path input_data_folder;
    fs::path output_bars_folder;
    std::string tops_executable;
    std::string fills_executable;

    if (is_merged_flow) {
        std::cout << "\n--- Processing MERGED book files to bars (TOPS ONLY) ---" << std::endl;
        input_data_folder = context_path / "mergedbooks";
        output_bars_folder = context_path / "mergedbooks" / "bars";
        tops_executable = PARSE_MERGED_TOPS_EXECUTABLE;
    } else {
        std::cout << "\n--- Processing book files from feed '" << feed_or_mode_str << "' to bars ---" << std::endl;
        input_data_folder = context_path / "books";
        output_bars_folder = context_path / "bars";
        tops_executable = PROCESS_TOPS_EXECUTABLE;
        fills_executable = PARSE_FILLS_EXECUTABLE;
    }

    std::mutex console_mutex;
    std::deque<std::future<bool>> futures;
    int success_count = 0;
    int failure_count = 0;

    try {
        fs::create_directories(output_bars_folder);
    } catch (const fs::filesystem_error& e) {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cerr << "Error creating bars output directory " << output_bars_folder << ": " << e.what() << std::endl;
    }

    if (!fs::is_directory(input_data_folder)) {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cerr << "Error: Input data directory " << input_data_folder << " is not valid." << std::endl;
        return;
    }

    for (const auto& entry : fs::directory_iterator(input_data_folder)) {
        if (!entry.is_regular_file()) continue;

        std::string file_name = entry.path().filename().string();
        if (file_name.rfind(".bin") == std::string::npos) continue;

        std::vector<std::string> name_parts = split_string(file_name, '.');
        std::string symbol;
        std::string current_bar_exe;
        std::string log_desc_prefix;
        std::optional<std::string> feed_arg_for_task;

        if (is_merged_flow) {
            if (name_parts.size() == 3 && name_parts[2] == "bin") {
                if (name_parts[0] == "merged_tops") {
                    symbol = name_parts[1];
                    current_bar_exe = tops_executable;
                    log_desc_prefix = "merged tops";
                } else if (name_parts[0] == "merged_fills") {
                    {
                        std::lock_guard<std::mutex> lock(console_mutex);
                        std::cout << "Skipping merged_fills file: " << file_name << std::endl;
                    }
                    continue;
                } else {
                    continue;
                }
            } else {
                 continue;
            }
        } else {
            if (name_parts.size() == 4 && name_parts[0] == to_upper(feed_or_mode_str) && name_parts[3] == "bin") {
                symbol = name_parts[2];
                feed_arg_for_task = feed_or_mode_str;
                if (name_parts[1] == "book_tops") {
                    current_bar_exe = tops_executable;
                    log_desc_prefix = "book tops";
                } else if (name_parts[1] == "book_fills") {
                    current_bar_exe = fills_executable;
                    log_desc_prefix = "book fills";
                } else {
                    continue;
                }
            } else {
                continue;
            }
        }
        
        if (!symbol.empty() && !current_bar_exe.empty()) {
            while (futures.size() >= MAX_CONCURRENT_TASKS) {
                try {
                    if (futures.front().get()) {
                        success_count++;
                    } else {
                        failure_count++;
                    }
                } catch (const std::exception& e) {
                    std::lock_guard<std::mutex> lock(console_mutex);
                    std::cerr << "Exception while getting future result (generate_bars_task): " << e.what() << std::endl;
                    failure_count++;
                }
                futures.pop_front();
            }
            futures.push_back(
                std::async(std::launch::async, generate_bars_for_file_task,
                           entry.path(), current_bar_exe, date_str, symbol,
                           feed_arg_for_task, log_desc_prefix,
                           std::ref(console_mutex))
            );
        }
    }

    while (!futures.empty()) {
        try {
            if (futures.front().get()) {
                success_count++;
            } else {
                failure_count++;
            }
        } catch (const std::exception& e) {
            std::lock_guard<std::mutex> lock(console_mutex);
            std::cerr << "Exception while getting future result (generate_bars_task cleanup): " << e.what() << std::endl;
            failure_count++;
        }
        futures.pop_front();
    }

    std::cout << "--- Finished processing to bars. Success: " << success_count << ", Failed: " << failure_count 
              << ". Bar files should be in " << output_bars_folder.string() << " ---" << std::endl;
}

int main() {
    std::string date_str, feed_str;

    std::cout << "Enter file date (yearMonthDay): ";
    std::cin >> date_str;
    std::cout << "Enter file feed (e.g., iex, bats, or 'mergedbooks'): ";
    std::cin >> feed_str;

    fs::path top_level_date_path = fs::path("/home/vir") / date_str;

    if (to_lower(feed_str) == "mergedbooks") {
        fs::path mergedbooks_input_dir = top_level_date_path / "mergedbooks";
        if (fs::is_directory(mergedbooks_input_dir)) {
            std::cout << "Mode: Processing 'mergedbooks'. Skipping HistBook stage." << std::endl;
            process_files_to_bars(top_level_date_path, date_str, "mergedbooks");
        } else {
            std::cerr << "Error: Merged books directory " << mergedbooks_input_dir.string() << " not found." << std::endl;
            return 1;
        }
    } else {
        fs::path specific_feed_path = top_level_date_path / to_lower(feed_str);
        if (fs::is_directory(specific_feed_path)) {
            // Step 1: Process raw files into books using HistBook
            process_to_books(specific_feed_path);
            
            // Step 2: Process books into bars
            fs::path books_dir_for_feed = specific_feed_path / "books";
            if (fs::is_directory(books_dir_for_feed)) {
                process_files_to_bars(specific_feed_path, date_str, feed_str);
            } else {
                std::cerr << "Books directory (" << books_dir_for_feed << ") not found for feed " << feed_str 
                          << ". Skipping bar generation from books." << std::endl;
            }
        } else {
            std::cerr << "Error: Specific feed directory " << specific_feed_path.string() << " is not a valid directory." << std::endl;
            return 1;
        }
    }

    return 0;
}