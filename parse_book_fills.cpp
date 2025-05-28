#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <chrono>
#include <cstdint>
#include <algorithm>
#include <limits>

// Define the header format (little-endian)
#pragma pack(push, 1)
struct FileHeader {
    uint64_t feed_id;
    uint32_t dateint;
    uint32_t number_of_fills;
    uint64_t symbol_idx;
};

// Define the data record format (little-endian)
struct DataRecord {
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

// Define the binary format for storing bars
struct BarRecord {
    uint64_t timestamp_sec;
    double high;
    double low;
    double open;
    double close;
    int32_t volume;
};
#pragma pack(pop)

const size_t HEADER_SIZE = sizeof(FileHeader);
const size_t DATA_SIZE = sizeof(DataRecord);
const size_t BAR_SIZE = sizeof(BarRecord);

// Function to read the file header
uint32_t read_header(std::ifstream& file, FileHeader& header) {
    file.read(reinterpret_cast<char*>(&header), HEADER_SIZE);
    if (file.gcount() < HEADER_SIZE) {
        std::cerr << "Error: File is too small to contain a valid header or read error." << std::endl;
        return 0;
    }

    std::cout << "Header Information:" << std::endl;
    std::cout << "  Feed ID: " << header.feed_id << std::endl;
    std::cout << "  Date (int): " << header.dateint << std::endl;
    std::cout << "  Number of Fills: " << header.number_of_fills << std::endl;
    std::cout << "  Symbol Index: " << header.symbol_idx << std::endl;

    return header.number_of_fills;
}

// Function to write a bar to the binary file
void write_bar(std::ofstream& outFile, 
               std::chrono::system_clock::time_point bar_time_utc, 
               double high_price, double low_price, double open_price, double close_price, 
               int32_t total_volume) {
    if (!outFile.is_open() || !outFile.good()) {
        std::cerr << "Error: Output file stream is not valid for writing bar." << std::endl;
        return;
    }
    BarRecord bar;
    bar.timestamp_sec = std::chrono::duration_cast<std::chrono::seconds>(bar_time_utc.time_since_epoch()).count();
    bar.high = high_price;
    bar.low = low_price;
    bar.open = open_price;
    bar.close = close_price;
    bar.volume = total_volume;
    outFile.write(reinterpret_cast<const char*>(&bar), BAR_SIZE);
}

// Function to read data records and generate bars
void read_data_and_generate_bars(std::ifstream& inputFile, uint32_t number_of_fills, std::ofstream& outputFile) {
    std::cout << "\nProcessing Book Fill Snapshots..." << std::endl;
    
    DataRecord data_record;

    std::chrono::system_clock::time_point current_bar_tp_utc{};
    double bar_open_price = 0.0;
    double bar_high_price = -std::numeric_limits<double>::infinity();
    double bar_low_price = std::numeric_limits<double>::infinity();
    double bar_close_price = 0.0;
    int32_t bar_total_volume = 0;

    for (uint32_t i = 0; i < number_of_fills; ++i) {
        inputFile.read(reinterpret_cast<char*>(&data_record), DATA_SIZE);
        if (inputFile.gcount() < DATA_SIZE) {
            std::cerr << "Warning: Reached end of file earlier than expected or read error at record " << i << "." << std::endl;
            break;
        }
        
        uint64_t raw_timestamp_ns = data_record.ts;
        double current_trade_price = static_cast<double>(data_record.trade_price) / 1e9;
        uint32_t current_trade_qty = data_record.trade_qty;

        auto trade_tp_utc = std::chrono::system_clock::time_point(std::chrono::duration_cast<std::chrono::system_clock::duration>(std::chrono::nanoseconds(raw_timestamp_ns)));
        auto this_bar_tp_utc = std::chrono::floor<std::chrono::seconds>(trade_tp_utc);

        if (current_bar_tp_utc.time_since_epoch().count() != 0 && this_bar_tp_utc != current_bar_tp_utc) {
            if (bar_total_volume > 0) {
                 write_bar(outputFile, current_bar_tp_utc, bar_high_price, bar_low_price, bar_open_price, bar_close_price, bar_total_volume);
            }
            current_bar_tp_utc = this_bar_tp_utc;
            bar_open_price = current_trade_price;
            bar_high_price = current_trade_price;
            bar_low_price = current_trade_price;
            bar_close_price = current_trade_price;
            bar_total_volume = current_trade_qty;
        
        } else if (current_bar_tp_utc.time_since_epoch().count() == 0) {
            current_bar_tp_utc = this_bar_tp_utc;
            bar_open_price = current_trade_price;
            bar_high_price = current_trade_price;
            bar_low_price = current_trade_price;
            bar_close_price = current_trade_price;
            bar_total_volume = current_trade_qty;
        } else {
            bar_high_price = std::max(bar_high_price, current_trade_price);
            bar_low_price = std::min(bar_low_price, current_trade_price);
            bar_close_price = current_trade_price;
            bar_total_volume += current_trade_qty;
        }
    }

    if (current_bar_tp_utc.time_since_epoch().count() != 0 && bar_total_volume > 0) {
        write_bar(outputFile, current_bar_tp_utc, bar_high_price, bar_low_price, bar_open_price, bar_close_price, bar_total_volume);
    }
    if (!outputFile.good()) {
         std::cerr << "Error occurred during writing output file." << std::endl;
    }
}


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


int main(int argc, char *argv[]) {
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " <date> <feed> <symbol>" << std::endl;
        return 1;
    }

    std::string date = argv[1];
    std::string feed = argv[2];
    std::string symbol = argv[3];

    // Convert symbol to uppercase
    std::transform(symbol.begin(), symbol.end(), symbol.begin(), ::toupper);

    // Construct file paths (adjust base path as needed)
    std::string base_path_input = "/home/vir/" + date + "/" + to_lower(feed) + "/books/" + to_upper(feed) + ".book_fills." + to_upper(symbol) + ".bin";
    std::string base_path_output = "/home/vir/" + date + "/" + to_lower(feed) + "/bars/" + to_upper(feed) + ".fills_bars." + to_upper(symbol) + ".bin";
    
    std::ifstream input_file(base_path_input, std::ios::binary);
    if (!input_file.is_open()) {
        std::cerr << "Error: Could not open input file: " << base_path_input << std::endl;
        return 1;
    }

    std::ofstream output_file(base_path_output, std::ios::binary | std::ios::trunc);
    if (!output_file.is_open()) {
        std::cerr << "Error: Could not open output file for writing: " << base_path_output << std::endl;
        input_file.close();
        return 1;
    }
    
    std::cout << "\nSaving bars to " << base_path_output << " (Overwriting if exists)..." << std::endl;

    FileHeader header;
    uint32_t number_of_fills = read_header(input_file, header);

    if (number_of_fills > 0 && input_file.good()) {
        read_data_and_generate_bars(input_file, number_of_fills, output_file);
        std::cout << "Bars saved to " << base_path_output << std::endl;
    } else if (number_of_fills == 0) {
        std::cout << "No fills to process based on header." << std::endl;
    } else {
        std::cerr << "Could not process fills due to header read issue or 0 fills." << std::endl;
    }

    input_file.close();
    output_file.close();

    return 0;
}