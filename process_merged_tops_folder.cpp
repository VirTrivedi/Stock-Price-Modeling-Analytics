#include <iostream>
#include <string>
#include <vector>
#include <regex>
#include <cstdlib>
#include <sstream>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <limits.h>
#include <future>
#include <mutex>
#include <functional>
#include <atomic>

// Helper function to check if a path is a directory
bool is_directory(const std::string& path) {
    struct stat statbuf;
    if (stat(path.c_str(), &statbuf) != 0) {
        return false;
    }
    return S_ISDIR(statbuf.st_mode);
}

// Helper function to check if a path exists
bool path_exists(const std::string& path) {
    struct stat statbuf;
    return (stat(path.c_str(), &statbuf) == 0);
}

// Helper function to check if a path is a regular file
bool is_regular_file(const std::string& path) {
    struct stat statbuf;
    if (stat(path.c_str(), &statbuf) != 0) {
        return false;
    }
    return S_ISREG(statbuf.st_mode);
}

// Helper function to create a directory
bool create_directory_simple(const std::string& path) {
    if (path_exists(path)) {
        return is_directory(path);
    }
    if (mkdir(path.c_str(), 0755) == 0) {
        return true;
    }
    return false;
}

// Helper function to get an absolute path
std::string get_absolute_path_simple(const std::string& path_str) {
    if (path_str.empty()) return "";
    if (path_str[0] == '/') return path_str; // Already absolute

    char cwd_buf[PATH_MAX];
    if (getcwd(cwd_buf, sizeof(cwd_buf)) != NULL) {
        std::string current_working_dir(cwd_buf);
        if (!current_working_dir.empty() && current_working_dir.back() == '/') {
             return current_working_dir + path_str;
        }
        return current_working_dir + "/" + path_str;
    }
    return path_str;
}

void print_usage(const char* prog_name) {
    std::cerr << "Usage: " << prog_name
              << " --input-folder <path>"
              << " --output-folder <path>"
              << " --executable-path <path_to_process_merged_tops_executable>"
              << std::endl;
}

// Worker function to process a single file
bool process_file_task(
    const std::string& executable_path,
    const std::string& input_filepath,
    const std::string& output_filepath,
    const std::string& original_filename,
    std::mutex& console_mutex) {

    std::ostringstream command_stream;
    command_stream << "\"" << executable_path << "\""
                   << " --input-file " << "\"" << input_filepath << "\""
                   << " --output-file " << "\"" << output_filepath << "\"";
    std::string command = command_stream.str();

    {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cout << "\nProcessing file: " << original_filename << std::endl;
        std::cout << "  Input: " << input_filepath << std::endl;
        std::cout << "  Output: " << output_filepath << std::endl;
        std::cout << "  Executing: " << command << std::endl;
    }

    int return_code = std::system(command.c_str());

    if (return_code == 0) {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cout << "  Successfully processed " << original_filename << std::endl;
        return true;
    } else {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cerr << "  Error processing " << original_filename << ". Executable returned code: " << return_code << std::endl;
        return false;
    }
}

int main(int argc, char* argv[]) {
    std::string input_folder_path_str;
    std::string output_folder_path_str;
    std::string executable_path_str;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--input-folder" && i + 1 < argc) {
            input_folder_path_str = argv[++i];
        } else if (arg == "--output-folder" && i + 1 < argc) {
            output_folder_path_str = argv[++i];
        } else if (arg == "--executable-path" && i + 1 < argc) {
            executable_path_str = argv[++i];
        }
    }

    if (input_folder_path_str.empty() || output_folder_path_str.empty() || executable_path_str.empty()) {
        print_usage(argv[0]);
        return 1;
    }

    if (!input_folder_path_str.empty() && input_folder_path_str.back() == '/') {
        input_folder_path_str.pop_back();
    }
    if (!output_folder_path_str.empty() && output_folder_path_str.back() == '/') {
        output_folder_path_str.pop_back();
    }


    if (!is_directory(input_folder_path_str)) {
        std::cerr << "Error: Input folder not found or is not a directory: " << input_folder_path_str << std::endl;
        return 1;
    }

    if (!path_exists(executable_path_str) || !is_regular_file(executable_path_str)) {
        std::string current_program_dir_str;
        std::string arg0_str = argv[0];
        size_t last_slash_idx = arg0_str.rfind('/');
        if (std::string::npos != last_slash_idx) {
            current_program_dir_str = arg0_str.substr(0, last_slash_idx);
        } else {
             char cwd_buf[PATH_MAX];
             if (getcwd(cwd_buf, sizeof(cwd_buf)) != NULL) {
                current_program_dir_str = cwd_buf;
             }
        }
        
        std::string exe_filename_str = executable_path_str;
        last_slash_idx = executable_path_str.rfind('/');
        if (std::string::npos != last_slash_idx) {
            exe_filename_str = executable_path_str.substr(last_slash_idx + 1);
        }
        if (!current_program_dir_str.empty()) {
            current_program_dir_str += "/";
        }

        std::string potential_exe_path_str = current_program_dir_str + exe_filename_str;

        if (path_exists(potential_exe_path_str) && is_regular_file(potential_exe_path_str)) {
            executable_path_str = potential_exe_path_str;
            std::cout << "Info: Resolved executable path to: " << executable_path_str << std::endl;
        } else {
            std::cerr << "Error: Executable not found or is not a file: " << executable_path_str
                      << " (also checked " << potential_exe_path_str << ")" << std::endl;
            return 1;
        }
    }

    if (!create_directory_simple(output_folder_path_str)) {
        std::cerr << "Error creating output folder (or it's not a directory): " << output_folder_path_str << std::endl;
        return 1;
    }
    std::cout << "Output folder: " << get_absolute_path_simple(output_folder_path_str) << std::endl;

    std::regex file_pattern("^merged_tops\\.([a-zA-Z0-9_]+)\\.bin$");
    int processed_count = 0;
    int skipped_count = 0;
    std::mutex console_mutex;

    std::cout << "\nProcessing files from: " << get_absolute_path_simple(input_folder_path_str) << std::endl;
    std::cout << "Using executable: " << get_absolute_path_simple(executable_path_str) << std::endl;

    std::vector<std::future<bool>> futures;

    DIR* dir;
    struct dirent* ent;
    if ((dir = opendir(input_folder_path_str.c_str())) != NULL) {
        while ((ent = readdir(dir)) != NULL) {
            std::string filename = ent->d_name;
            std::string full_path_to_entry = input_folder_path_str + "/" + filename;

            if (filename == "." || filename == "..") continue;

            if (!is_regular_file(full_path_to_entry)) {
                continue;
            }

            std::smatch match;
            if (std::regex_match(filename, match, file_pattern)) {
                if (match.size() == 2) {
                    std::string symbol = match[1].str();
                    std::string input_filepath_str = full_path_to_entry;
                    std::string output_filename_str = "processed_tops." + symbol + ".bin";
                    std::string output_filepath_str = output_folder_path_str + "/" + output_filename_str;

                    futures.push_back(
                        std::async(std::launch::async, 
                                   process_file_task,
                                   executable_path_str,
                                   input_filepath_str,
                                   output_filepath_str,
                                   filename,
                                   std::ref(console_mutex)
                        )
                    );
                }
            }
        }
        closedir(dir);
    } else {
        std::lock_guard<std::mutex> lock(console_mutex);
        std::cerr << "Error: Could not open input directory: " << input_folder_path_str << std::endl;
        return 1;
    }

    for (auto& fut : futures) {
        if (fut.get()) {
            processed_count++;
        } else {
            skipped_count++;
        }
    }

    std::cout << "\nBatch processing complete." << std::endl;
    std::cout << "Successfully processed: " << processed_count << " files." << std::endl;
    std::cout << "Skipped or failed: " << skipped_count << " files." << std::endl;

    return 0;
}