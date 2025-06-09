#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <string>
#include <iomanip>
#include <ctime>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <algorithm>
#include <thread>

#pragma pack(push, 1)

struct MergedFileHeader {
    uint64_t feed_id;
    uint32_t dateint;
    uint32_t count;
    uint64_t symbol_idx;
};

struct TopLevelData {
    int64_t bid_price;
    int64_t ask_price;
    uint32_t bid_qty;
    uint32_t ask_qty;
};

struct TopsDataRecord {
    uint64_t ts;
    uint64_t seqno;
    TopLevelData levels[3];
};
#pragma pack(pop)

struct Bar {
    uint64_t timestamp;
    double open;
    double high;
    double low;
    double close;
};

// Function to read the main header of the merged file
bool read_main_header(std::ifstream &file, MergedFileHeader &header) {
    file.read(reinterpret_cast<char *>(&header), sizeof(MergedFileHeader));
    if (static_cast<size_t>(file.gcount()) < sizeof(MergedFileHeader)) {
        std::cerr << "Error: Merged file is too small to contain a valid main header." << std::endl;
        return false;
    }

    std::cout << "Merged File Header Information:" << std::endl;
    std::cout << "  Feed ID (from header): " << header.feed_id << std::endl;
    std::cout << "  Date (int): " << header.dateint << std::endl;
    std::cout << "  Number of Records: " << header.count << std::endl;
    std::cout << "  Symbol Index (from header): " << header.symbol_idx << std::endl;

    return true;
}

// Function to read data from merged tops file
void read_merged_data(std::ifstream &file, uint32_t number_of_records, std::vector<uint64_t> &timestamps,
    std::vector<std::vector<double>> &bid_prices, std::vector<std::vector<double>> &ask_prices) {
    
    bid_prices.assign(3, std::vector<double>());
    ask_prices.assign(3, std::vector<double>());

    for (uint32_t rec_idx = 0; rec_idx < number_of_records; ++rec_idx) {
        uint64_t original_feed_id_for_record;
        file.read(reinterpret_cast<char*>(&original_feed_id_for_record), sizeof(uint64_t));
        if (static_cast<size_t>(file.gcount()) < sizeof(uint64_t)) {
            std::cerr << "Error reading original feed_id for record " << rec_idx << std::endl;
            break;
        }

        TopsDataRecord current_tops_record;
        file.read(reinterpret_cast<char *>(&current_tops_record), sizeof(TopsDataRecord));
        if (static_cast<size_t>(file.gcount()) < sizeof(TopsDataRecord)) {
            std::cerr << "Error reading TopsDataRecord " << rec_idx << std::endl;
            break;
        }

        timestamps.push_back(current_tops_record.ts);

        for (int level = 0; level < 3; ++level) {
            if (current_tops_record.levels[level].bid_price != 0 && current_tops_record.levels[level].bid_qty != 0) {
                bid_prices[level].push_back(static_cast<double>(current_tops_record.levels[level].bid_price) / 1e9);
            } else {
                bid_prices[level].push_back(NAN);
            }

            if (current_tops_record.levels[level].ask_price != 0 && current_tops_record.levels[level].ask_qty != 0) {
                ask_prices[level].push_back(static_cast<double>(current_tops_record.levels[level].ask_price) / 1e9);
            } else {
                ask_prices[level].push_back(NAN);
            }
        }
    }
}

// Function to create and store bars
void create_and_store_bars(const std::vector<uint64_t> &timestamps, const std::vector<double> &prices,
                           const std::string &output_file, uint64_t &last_timestamp_written) {
    std::map<uint64_t, Bar> bars;

    for (size_t i = 0; i < timestamps.size(); ++i) {
        if (std::isnan(prices[i])) {
            continue;
        }

        uint64_t bar_time_sec = timestamps[i] / 1000000000ULL;

        if (bars.find(bar_time_sec) == bars.end()) {
            if (last_timestamp_written != 0 && bar_time_sec < last_timestamp_written + 1) {
            }
            bars[bar_time_sec] = {bar_time_sec, prices[i], prices[i], prices[i], prices[i]};
        } else {
            bars[bar_time_sec].high = std::max(bars[bar_time_sec].high, prices[i]);
            bars[bar_time_sec].low = std::min(bars[bar_time_sec].low, prices[i]);
            bars[bar_time_sec].close = prices[i];
        }
    }

    std::ofstream output(output_file, std::ios::binary | std::ios::trunc);
    if (!output.is_open()) {
        std::cerr << "Error: Could not open output file: " << output_file << std::endl;
        return;
    }

    uint64_t temp_last_ts = 0;
    for (const auto &entry : bars) {
        const Bar &bar = entry.second;
        output.write(reinterpret_cast<const char *>(&bar), sizeof(Bar));
        temp_last_ts = bar.timestamp;
    }
    last_timestamp_written = temp_last_ts;

    output.close();
}

// Function to process and store bars
void process_and_store_all_bars(const std::vector<uint64_t> &timestamps,
    const std::vector<std::vector<double>> &bid_prices,
    const std::vector<std::vector<double>> &ask_prices,
    const std::string &output_file_path_base, const std::string &symbol) {
    
    std::vector<uint64_t> last_bid_timestamps_written(3, 0);
    std::vector<uint64_t> last_ask_timestamps_written(3, 0);

    auto process_level = [&](int level) {
        std::string bid_bar_file = output_file_path_base + "bid_bars_L" + std::to_string(level + 1) + "." + symbol + ".bin";
        std::string ask_bar_file = output_file_path_base + "ask_bars_L" + std::to_string(level + 1) + "." + symbol + ".bin";

        if (!bid_prices[level].empty()) {
             create_and_store_bars(timestamps, bid_prices[level], bid_bar_file, last_bid_timestamps_written[level]);
        }
        if (!ask_prices[level].empty()) {
            create_and_store_bars(timestamps, ask_prices[level], ask_bar_file, last_ask_timestamps_written[level]);
        }
    };

    std::vector<std::thread> threads;
    for (int level = 0; level < 3; ++level) {
        threads.emplace_back(process_level, level);
    }

    for (auto &thread : threads) {
        thread.join();
    }
}

// Function to process the merged file
void process_merged_file(const std::string &date, const std::string &symbol_arg) {
    std::string symbol = symbol_arg;
    std::transform(symbol.begin(), symbol.end(), symbol.begin(), ::toupper);

    std::string input_file_path = "/home/vir/" + date + "/mergedbooks/merged_tops." + symbol + ".bin";
    std::string output_file_path_base = "/home/vir/" + date + "/mergedbooks/bars/MERGEDBOOKS.";

    std::ifstream input_file(input_file_path, std::ios::binary);
    if (!input_file.is_open()) {
        std::cerr << "Error: Merged tops file not found: " << input_file_path << std::endl;
        return;
    }

    MergedFileHeader main_header;
    if (!read_main_header(input_file, main_header)) {
        return;
    }

    if (main_header.count == 0) {
        std::cout << "No records to process in " << input_file_path << std::endl;
        return;
    }

    std::vector<uint64_t> timestamps;
    std::vector<std::vector<double>> bid_prices, ask_prices;
    read_merged_data(input_file, main_header.count, timestamps, bid_prices, ask_prices);
    input_file.close();

    if (timestamps.empty()) {
        std::cout << "No valid data read from " << input_file_path << std::endl;
        return;
    }

    process_and_store_all_bars(timestamps, bid_prices, ask_prices, output_file_path_base, symbol);
    std::cout << "Finished processing merged tops for symbol " << symbol << ". Bars stored with base: " << output_file_path_base << std::endl;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: ./parse_merged_tops <date> <symbol>" << std::endl;
        return 1;
    }

    std::string date = argv[1];
    std::string symbol = argv[2];
    
    process_merged_file(date, symbol);

    return 0;
}