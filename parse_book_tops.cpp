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

// Define the header format
struct Header {
    uint64_t feed_id;
    uint32_t dateint;
    uint32_t number_of_tops;
    uint64_t symbol_idx;
};

// Define the book top format
struct BookTop {
    uint64_t ts;
    uint64_t seqno;
    int64_t bid_price[3];
    int64_t ask_price[3];
    uint32_t bid_qty[3];
    uint32_t ask_qty[3];
};

// Define the bar format
struct Bar {
    uint64_t timestamp;
    double open;
    double high;
    double low;
    double close;
};

// Function to read the header
bool read_header(std::ifstream &file, Header &header) {
    file.read(reinterpret_cast<char *>(&header), sizeof(Header));
    if (file.gcount() < sizeof(Header)) {
        std::cerr << "Error: File is too small to contain a valid header." << std::endl;
        return false;
    }

    std::cout << "Header Information:" << std::endl;
    std::cout << "  Feed ID: " << header.feed_id << std::endl;
    std::cout << "  Date (int): " << header.dateint << std::endl;
    std::cout << "  Number of Tops: " << header.number_of_tops << std::endl;
    std::cout << "  Symbol Index: " << header.symbol_idx << std::endl;

    return true;
}

// Function to read data
void read_data(std::ifstream &file, uint32_t number_of_tops, std::vector<uint64_t> &timestamps,
               std::vector<std::vector<double>> &bid_prices, std::vector<std::vector<double>> &ask_prices) {
    bid_prices.resize(3);
    ask_prices.resize(3);

    for (uint32_t i = 0; i < number_of_tops; ++i) {
        BookTop book_top;
        file.read(reinterpret_cast<char *>(&book_top), sizeof(BookTop));
        if (file.gcount() < sizeof(BookTop)) {
            std::cerr << "Warning: Reached end of file earlier than expected." << std::endl;
            break;
        }

        timestamps.push_back(book_top.ts);

        for (int level = 0; level < 3; ++level) {
            if (book_top.bid_price[level] != 0 && book_top.bid_qty[level] != 0) {
                bid_prices[level].push_back(book_top.bid_price[level] / 1e9);
            } else {
                bid_prices[level].push_back(NAN);
            }

            if (book_top.ask_price[level] != 0 && book_top.ask_qty[level] != 0) {
                ask_prices[level].push_back(book_top.ask_price[level] / 1e9);
            } else {
                ask_prices[level].push_back(NAN);
            }
        }
    }
}

// Function to create and store bars
void create_and_store_bars(const std::vector<uint64_t> &timestamps, const std::vector<double> &prices,
                           const std::string &output_file, uint64_t &last_timestamp) {
    std::map<uint64_t, Bar> bars;

    for (size_t i = 0; i < timestamps.size(); ++i) {
        if (std::isnan(prices[i])) {
            continue;
        }

        uint64_t bar_time = timestamps[i] / 1000000000; // Convert nanoseconds to seconds

        if (last_timestamp != 0 && bar_time <= last_timestamp + 1) {
            continue;
        }

        if (bars.find(bar_time) == bars.end()) {
            bars[bar_time] = {bar_time, prices[i], prices[i], prices[i], prices[i]};
        } else {
            bars[bar_time].high = std::max(bars[bar_time].high, prices[i]);
            bars[bar_time].low = std::min(bars[bar_time].low, prices[i]);
            bars[bar_time].close = prices[i];
        }
    }

    std::ofstream output(output_file, std::ios::binary | std::ios::trunc);
    if (!output.is_open()) {
        std::cerr << "Error: Could not open output file: " << output_file << std::endl;
        return;
    }

    for (const auto &entry : bars) {
        const Bar &bar = entry.second;
        output.write(reinterpret_cast<const char *>(&bar), sizeof(Bar));
        last_timestamp = bar.timestamp;
    }

    output.close();
}

// Function to process and store bars
void process_and_store_bars(const std::vector<uint64_t> &timestamps,
    const std::vector<std::vector<double>> &bid_prices,
    const std::vector<std::vector<double>> &ask_prices,
    const std::string &output_file_path_base, const std::string &symbol) {

    uint64_t last_bid_timestamps[3] = {0, 0, 0};
    uint64_t last_ask_timestamps[3] = {0, 0, 0};

    for (int level = 0; level < 3; ++level) {
        // Construct file paths for bid and ask bars
        std::string bid_bar_file = output_file_path_base + "bid_bars_L" + std::to_string(level + 1) + "." + symbol + ".bin";
        std::string ask_bar_file = output_file_path_base + "ask_bars_L" + std::to_string(level + 1) + "." + symbol + ".bin";

        // Process and store bid bars
        create_and_store_bars(timestamps, bid_prices[level], bid_bar_file, last_bid_timestamps[level]);

        // Process and store ask bars
        create_and_store_bars(timestamps, ask_prices[level], ask_bar_file, last_ask_timestamps[level]);
    }
}

// Function to process the file
void process_file(const std::string &date, const std::string &feed, const std::string &symbol) {
    // Convert feed to uppercase for the second occurrence
    std::string feed_upper = feed;
    std::transform(feed_upper.begin(), feed_upper.end(), feed_upper.begin(), ::toupper);

    std::string input_file_path = "/home/vir/" + date + "/" + feed + "/books/" + feed_upper + ".book_tops." + symbol + ".bin";
    std::string output_file_path_base = "/home/vir/" + date + "/" + feed + "/bars/" + feed_upper + ".";

    std::ifstream input_file(input_file_path, std::ios::binary);
    if (!input_file.is_open()) {
        std::cerr << "Error: File not found: " << input_file_path << std::endl;
        return;
    }

    Header header;
    if (!read_header(input_file, header)) {
        return;
    }

    std::vector<uint64_t> timestamps;
    std::vector<std::vector<double>> bid_prices, ask_prices;
    read_data(input_file, header.number_of_tops, timestamps, bid_prices, ask_prices);

    process_and_store_bars(timestamps, bid_prices, ask_prices, output_file_path_base, symbol);
}

int main(int argc, char *argv[]) {
    if (argc != 4) {
        std::cerr << "Usage: ./process_tops <date> <feed> <symbol>" << std::endl;
        return 1;
    }

    std::string date = argv[1];
    std::string feed = argv[2];
    std::string symbol = argv[3];

    // Convert symbol to uppercase
    std::transform(symbol.begin(), symbol.end(), symbol.begin(), ::toupper);
    
    process_file(date, feed, symbol);

    return 0;
}