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
    // Mode 0755 (rwxr-xr-x)
    if (mkdir(path.c_str(), 0755) == 0) {
        return true;
    }
    return false; // mkdir failed
}

// Helper function to get an absolute path
std::string get_absolute_path_simple(const std::string& path_str) {
    if (path_str.empty()) return "";
    if (path_str[0] == '/') return path_str;

    char cwd_buf[PATH_MAX];
    if (getcwd(cwd_buf, sizeof(cwd_buf)) != NULL) {
        std::string current_working_dir(cwd_buf);
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

        std::string potential_exe_path_str = current_program_dir_str + "/" + exe_filename_str;

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

    std::cout << "\nProcessing files from: " << get_absolute_path_simple(input_folder_path_str) << std::endl;
    std::cout << "Using executable: " << get_absolute_path_simple(executable_path_str) << std::endl;

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

                    std::cout << "\nFound matching file: " << filename << std::endl;
                    std::cout << "  Input: " << input_filepath_str << std::endl;
                    std::cout << "  Output: " << output_filepath_str << std::endl;

                    std::ostringstream command_stream;
                    command_stream << "\"" << executable_path_str << "\""
                                   << " --input-file " << "\"" << input_filepath_str << "\""
                                   << " --output-file " << "\"" << output_filepath_str << "\"";
                    std::string command = command_stream.str();

                    std::cout << "  Executing: " << command << std::endl;
                    
                    int return_code = std::system(command.c_str());

                    if (return_code == 0) {
                        std::cout << "  Successfully processed " << filename << std::endl;
                        processed_count++;
                    } else {
                        std::cerr << "  Error processing " << filename << ". Executable returned code: " << return_code << std::endl;
                        skipped_count++;
                    }
                }
            }
        }
        closedir(dir);
    } else {
        std::cerr << "Error: Could not open input directory: " << input_folder_path_str << std::endl;
        return 1;
    }

    std::cout << "\nBatch processing complete." << std::endl;
    std::cout << "Successfully processed: " << processed_count << " files." << std::endl;
    std::cout << "Skipped or failed: " << skipped_count << " files." << std::endl;

    return 0;
}