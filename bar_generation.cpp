#include <iostream>
#include <string>
#include <vector>
#include <filesystem>
#include <cstdlib>
#include <sstream>
#include <algorithm>

const std::string HISTBOOK_EXECUTABLE = "/home/vir/histbook/build/bin/HistBook";
const std::string PARSE_FILLS_EXECUTABLE = "./parse_book_fills"; 
const std::string PROCESS_TOPS_EXECUTABLE = "./process_tops";
namespace fs = std::filesystem;

// Helper function to convert string to lowercase
std::string to_lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c){ return std::tolower(c); });
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
bool run_command(const std::string& command) {
    std::cout << "Executing: " << command << std::endl;
    int status = std::system(command.c_str());
    if (status != 0) {
        std::cerr << "Error: Command failed with status " << status << ": " << command << std::endl;
        return false;
    }
    return true;
}

void process_to_books(const fs::path& base_folder_path) {
    std::cout << "\n--- Processing raw files to books ---" << std::endl;
    fs::path input_folder = base_folder_path;
    fs::path output_folder = base_folder_path / "books";

    try {
        fs::create_directories(output_folder);
    } catch (const fs::filesystem_error& e) {
        std::cerr << "Error creating directory " << output_folder << ": " << e.what() << std::endl;
        return;
    }

    if (!fs::is_directory(input_folder)) {
        std::cerr << "Error: " << input_folder << " is not a valid directory." << std::endl;
        return;
    }

    for (const auto& entry : fs::directory_iterator(input_folder)) {
        if (entry.is_regular_file()) {
            std::string file_name = entry.path().filename().string();
            if (file_name.rfind(".bin") != std::string::npos && file_name.find("book_events") != std::string::npos) {
                fs::path input_file_path = entry.path();
                std::cout << "Processing raw file into book: " << file_name << std::endl;
                
                std::ostringstream command_stream;
                command_stream << "\"" << HISTBOOK_EXECUTABLE << "\""
                               << " --outputpath \"" << output_folder.string() << "/\""
                               << " --inputpath \"" << input_file_path.string() << "\"";
                
                if (!run_command(command_stream.str())) {
                    std::cerr << "Failed to process raw file: " << file_name << std::endl;
                }
            }
        }
    }
    std::cout << "--- Finished processing raw files to books ---" << std::endl;
}

void process_to_bars(const fs::path& base_folder_path, const std::string& date, const std::string& feed) {
    std::cout << "\n--- Processing book files to bars ---" << std::endl;
    fs::path input_folder = base_folder_path / "books";
    fs::path output_folder = base_folder_path / "bars";

    try {
        fs::create_directories(output_folder);
    } catch (const fs::filesystem_error& e) {
        std::cerr << "Error creating directory " << output_folder << ": " << e.what() << std::endl;
        return;
    }

    if (!fs::is_directory(input_folder)) {
        std::cerr << "Error: " << input_folder << " is not a valid directory for book files." << std::endl;
        return;
    }

    for (const auto& entry : fs::directory_iterator(input_folder)) {
        if (entry.is_regular_file()) {
            std::string file_name = entry.path().filename().string();
            if (file_name.rfind(".bin") != std::string::npos) {
                std::vector<std::string> name_parts = split_string(file_name, '.');
                if (name_parts.size() < 3) {
                    std::cout << "Skipping file with unexpected name format: " << file_name << std::endl;
                    continue;
                }
                std::string symbol = name_parts[2]; 
                                
                std::ostringstream command_stream;
                std::string file_name_lower = to_lower(file_name);

                if (file_name_lower.find("fills") != std::string::npos) {
                    std::cout << "Processing fills file: " << file_name << std::endl;
                    command_stream << "\"" << PARSE_FILLS_EXECUTABLE << "\""
                                   << " " << date
                                   << " " << feed
                                   << " " << symbol;
                     if (!run_command(command_stream.str())) {
                        std::cerr << "Failed to process fills file: " << file_name << std::endl;
                    }
                } else if (file_name_lower.find("tops") != std::string::npos) {
                    std::cout << "Processing tops file: " << file_name << std::endl;
                    command_stream << "\"" << PROCESS_TOPS_EXECUTABLE << "\""
                                   << " " << date
                                   << " " << feed
                                   << " " << symbol;
                    if (!run_command(command_stream.str())) {
                        std::cerr << "Failed to process tops file: " << file_name << std::endl;
                    }
                } else {
                    std::cout << "Skipping unrecognized file type: " << file_name << std::endl;
                }
            }
        }
    }
    std::cout << "--- Finished processing book files to bars. Processed files should be in " << output_folder.string() << " ---" << std::endl;
}

int main() {
    std::string date, feed;

    std::cout << "Enter file date (yearMonthDay): ";
    std::cin >> date;
    std::cout << "Enter file feed: ";
    std::cin >> feed;

    fs::path base_folder_path = fs::path("/home/vir") / date / to_lower(feed);

    if (fs::is_directory(base_folder_path)) {
        // Step 1: Process raw files into books
        process_to_books(base_folder_path);
        // Step 2: Process books into bars
        process_to_bars(base_folder_path, date, feed);
    } else {
        std::cerr << "Error: " << base_folder_path.string() << " is not a valid directory." << std::endl;
        return 1;
    }

    return 0;
}