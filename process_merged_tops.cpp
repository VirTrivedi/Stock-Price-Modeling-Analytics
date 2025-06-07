#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <map>
#include <algorithm>
#include <iomanip>

// --- Constants ---
const size_t INPUT_FILE_HEADER_SIZE = 24;
const size_t MERGED_ENTRY_PREFIX_FEED_ID_SIZE = 8;
const size_t TOPS_RECORD_SIZE_EXPECTED = 88;
const size_t MERGED_TOPS_FULL_ENTRY_SIZE = MERGED_ENTRY_PREFIX_FEED_ID_SIZE + TOPS_RECORD_SIZE_EXPECTED;

const size_t OUTPUT_FILE_HEADER_SIZE = 24;
const size_t SNAPSHOT_HEADER_SIZE_EXPECTED = 10;
const size_t LEVEL_HEADER_SIZE_EXPECTED = 9;
const size_t VENUE_AT_LEVEL_SIZE_EXPECTED = 12;

const int NUM_LEVELS_TO_SNAPSHOT = 3;
const uint64_t PROCESSED_SNAPSHOT_FILE_FEED_ID = 0;

#pragma pack(push, 1)

struct InputFileHeader {
    uint64_t feed_id;
    uint32_t dateint;
    uint32_t total_record_count;
    uint64_t symbol_idx;
};

struct TopLevelData {
    int64_t bid_price;
    int64_t ask_price;
    uint32_t bid_qty;
    uint32_t ask_qty;
};

struct TopsRecord {
    uint64_t ts;
    uint64_t seqno;
    TopLevelData level1;
    TopLevelData level2;
    TopLevelData level3;
};
static_assert(sizeof(TopLevelData) == 24, "TopLevelData size mismatch");
static_assert(sizeof(TopsRecord) == TOPS_RECORD_SIZE_EXPECTED, "TopsRecord size mismatch");

struct OutputFileHeader {
    uint64_t feed_id;
    uint32_t dateint;
    uint32_t num_snapshots;
    uint64_t symbol_idx;
};

struct SnapshotHeaderWrite {
    uint64_t timestamp;
    uint8_t num_bid_levels;
    uint8_t num_ask_levels;
};
static_assert(sizeof(SnapshotHeaderWrite) == SNAPSHOT_HEADER_SIZE_EXPECTED, "SnapshotHeaderWrite size mismatch");

struct LevelHeaderWrite {
    int64_t price_at_level;
    uint8_t num_venues;
};
static_assert(sizeof(LevelHeaderWrite) == LEVEL_HEADER_SIZE_EXPECTED, "LevelHeaderWrite size mismatch");

struct VenueAtLevelWrite {
    uint32_t quantity_from_venue;
    uint64_t feed_id_of_original_venue;
};
static_assert(sizeof(VenueAtLevelWrite) == VENUE_AT_LEVEL_SIZE_EXPECTED, "VenueAtLevelWrite size mismatch");

#pragma pack(pop)

struct VenueData {
    uint32_t quantity;
    uint64_t feed_id;

    bool operator<(const VenueData& other) const {
        if (feed_id != other.feed_id) return feed_id < other.feed_id;
        return quantity < other.quantity;
    }
    bool operator==(const VenueData& other) const {
        return quantity == other.quantity && feed_id == other.feed_id;
    }
};

struct SnapshotLevel {
    int64_t price;
    std::vector<VenueData> venues;

    bool operator==(const SnapshotLevel& other) const {
        return price == other.price && venues == other.venues;
    }
     bool operator!=(const SnapshotLevel& other) const {
        return !(*this == other);
    }
};

struct ParsedTopsLevelData {
    int64_t l1bp = 0, l1ap = 0; uint32_t l1bq = 0, l1aq = 0;
    int64_t l2bp = 0, l2ap = 0; uint32_t l2bq = 0, l2aq = 0;
    int64_t l3bp = 0, l3ap = 0; uint32_t l3bq = 0, l3aq = 0;
};


std::pair<std::vector<SnapshotLevel>, std::vector<SnapshotLevel>>
create_snapshot(const std::map<uint64_t, ParsedTopsLevelData>& latest_quotes_map) {
    std::map<int64_t, std::vector<VenueData>> bids_accumulator;
    std::map<int64_t, std::vector<VenueData>> asks_accumulator;

    for (const auto& pair : latest_quotes_map) {
        uint64_t original_feed_id = pair.first;
        const ParsedTopsLevelData& rec_data = pair.second;

        // Level 1
        if (rec_data.l1bp != 0 && rec_data.l1bq > 0) bids_accumulator[rec_data.l1bp].push_back({rec_data.l1bq, original_feed_id});
        if (rec_data.l1ap != 0 && rec_data.l1aq > 0) asks_accumulator[rec_data.l1ap].push_back({rec_data.l1aq, original_feed_id});
        // Level 2
        if (rec_data.l2bp != 0 && rec_data.l2bq > 0) bids_accumulator[rec_data.l2bp].push_back({rec_data.l2bq, original_feed_id});
        if (rec_data.l2ap != 0 && rec_data.l2aq > 0) asks_accumulator[rec_data.l2ap].push_back({rec_data.l2aq, original_feed_id});
        // Level 3
        if (rec_data.l3bp != 0 && rec_data.l3bq > 0) bids_accumulator[rec_data.l3bp].push_back({rec_data.l3bq, original_feed_id});
        if (rec_data.l3ap != 0 && rec_data.l3aq > 0) asks_accumulator[rec_data.l3ap].push_back({rec_data.l3aq, original_feed_id});
    }

    std::vector<SnapshotLevel> final_bid_levels;
    // Sort bid prices in descending order
    std::vector<int64_t> bid_prices;
    for(const auto& pair : bids_accumulator) bid_prices.push_back(pair.first);
    std::sort(bid_prices.rbegin(), bid_prices.rend());

    for (int64_t price : bid_prices) {
        if (final_bid_levels.size() >= NUM_LEVELS_TO_SNAPSHOT) break;
        std::vector<VenueData>& venues_at_price = bids_accumulator[price];
        if (!venues_at_price.empty()) {
            std::sort(venues_at_price.begin(), venues_at_price.end());
            final_bid_levels.push_back({price, venues_at_price});
        }
    }
    
    std::vector<SnapshotLevel> final_ask_levels;
    // Sort ask prices in ascending order
    std::vector<int64_t> ask_prices;
    for(const auto& pair : asks_accumulator) ask_prices.push_back(pair.first);
    std::sort(ask_prices.begin(), ask_prices.end());

    for (int64_t price : ask_prices) {
        if (final_ask_levels.size() >= NUM_LEVELS_TO_SNAPSHOT) break;
         std::vector<VenueData>& venues_at_price = asks_accumulator[price];
        if (!venues_at_price.empty()) {
            std::sort(venues_at_price.begin(), venues_at_price.end());
            final_ask_levels.push_back({price, venues_at_price});
        }
    }
    return {final_bid_levels, final_ask_levels};
}

void write_snapshot(std::ofstream& f_out, uint64_t snapshot_ts, 
                    const std::vector<SnapshotLevel>& bid_levels, 
                    const std::vector<SnapshotLevel>& ask_levels) {
    SnapshotHeaderWrite sh_write;
    sh_write.timestamp = snapshot_ts;
    sh_write.num_bid_levels = static_cast<uint8_t>(bid_levels.size());
    sh_write.num_ask_levels = static_cast<uint8_t>(ask_levels.size());
    f_out.write(reinterpret_cast<const char*>(&sh_write), sizeof(SnapshotHeaderWrite));

    for (const auto& level_data : bid_levels) {
        LevelHeaderWrite lh_write;
        lh_write.price_at_level = level_data.price;
        lh_write.num_venues = static_cast<uint8_t>(level_data.venues.size());
        f_out.write(reinterpret_cast<const char*>(&lh_write), sizeof(LevelHeaderWrite));
        for (const auto& venue : level_data.venues) {
            VenueAtLevelWrite val_write;
            val_write.quantity_from_venue = venue.quantity;
            val_write.feed_id_of_original_venue = venue.feed_id;
            f_out.write(reinterpret_cast<const char*>(&val_write), sizeof(VenueAtLevelWrite));
        }
    }
    for (const auto& level_data : ask_levels) {
        LevelHeaderWrite lh_write;
        lh_write.price_at_level = level_data.price;
        lh_write.num_venues = static_cast<uint8_t>(level_data.venues.size());
        f_out.write(reinterpret_cast<const char*>(&lh_write), sizeof(LevelHeaderWrite));
        for (const auto& venue : level_data.venues) {
            VenueAtLevelWrite val_write;
            val_write.quantity_from_venue = venue.quantity;
            val_write.feed_id_of_original_venue = venue.feed_id;
            f_out.write(reinterpret_cast<const char*>(&val_write), sizeof(VenueAtLevelWrite));
        }
    }
}


int main(int argc, char* argv[]) {
    std::string input_filepath;
    std::string output_filepath;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--input-file" && i + 1 < argc) {
            input_filepath = argv[++i];
        } else if (arg == "--output-file" && i + 1 < argc) {
            output_filepath = argv[++i];
        }
    }

    if (input_filepath.empty() || output_filepath.empty()) {
        std::cerr << "Usage: " << argv[0] << " --input-file <path> --output-file <path>" << std::endl;
        return 1;
    }

    std::ifstream f_in(input_filepath, std::ios::binary);
    if (!f_in) {
        std::cerr << "Error: Input file not found or cannot be opened: " << input_filepath << std::endl;
        return 1;
    }

    std::ofstream f_out(output_filepath, std::ios::binary | std::ios::trunc);
    if (!f_out) {
        std::cerr << "Error: Output file cannot be opened: " << output_filepath << std::endl;
        return 1;
    }
    
    InputFileHeader input_header;
    f_in.read(reinterpret_cast<char*>(&input_header), sizeof(InputFileHeader));
    if (static_cast<size_t>(f_in.gcount()) < sizeof(InputFileHeader)) {
        std::cerr << "Error: Input file '" << input_filepath << "' is too small to contain a valid header." << std::endl;
        return 1;
    }
    std::cout << "Input file ('" << input_filepath << "') header: DateInt=" << input_header.dateint 
              << ", SymbolIdx=" << input_header.symbol_idx 
              << ", TotalRecords=" << input_header.total_record_count << std::endl;

    // Write placeholder for the main output file header
    OutputFileHeader output_header_placeholder = {0};
    f_out.write(reinterpret_cast<const char*>(&output_header_placeholder), sizeof(OutputFileHeader));

    std::map<uint64_t, ParsedTopsLevelData> latest_venue_quotes;
    std::vector<SnapshotLevel> last_written_bids;
    std::vector<SnapshotLevel> last_written_asks;
    
    uint32_t total_input_records_read = 0;
    uint32_t num_snapshots_written = 0;

    char entry_buffer[MERGED_TOPS_FULL_ENTRY_SIZE];

    while (f_in.read(entry_buffer, MERGED_TOPS_FULL_ENTRY_SIZE)) {
        if (static_cast<size_t>(f_in.gcount()) < MERGED_TOPS_FULL_ENTRY_SIZE) {
            std::cerr << "Warning: Encountered an incomplete final entry in '" << input_filepath << "'. Skipping." << std::endl;
            break;
        }
        total_input_records_read++;
        if (total_input_records_read > 0 && total_input_records_read % 10000 == 0) {
            std::cout << "  Processed " << total_input_records_read << " input records..." << std::endl;
        }

        uint64_t original_source_feed_id = *reinterpret_cast<uint64_t*>(entry_buffer);
        TopsRecord* current_tops_record = reinterpret_cast<TopsRecord*>(entry_buffer + MERGED_ENTRY_PREFIX_FEED_ID_SIZE);
        
        ParsedTopsLevelData& venue_data = latest_venue_quotes[original_source_feed_id];
        venue_data.l1bp = current_tops_record->level1.bid_price; venue_data.l1ap = current_tops_record->level1.ask_price;
        venue_data.l1bq = current_tops_record->level1.bid_qty;   venue_data.l1aq = current_tops_record->level1.ask_qty;
        venue_data.l2bp = current_tops_record->level2.bid_price; venue_data.l2ap = current_tops_record->level2.ask_price;
        venue_data.l2bq = current_tops_record->level2.bid_qty;   venue_data.l2aq = current_tops_record->level2.ask_qty;
        venue_data.l3bp = current_tops_record->level3.bid_price; venue_data.l3ap = current_tops_record->level3.ask_price;
        venue_data.l3bq = current_tops_record->level3.bid_qty;   venue_data.l3aq = current_tops_record->level3.ask_qty;

        auto snapshot_pair = create_snapshot(latest_venue_quotes);
        std::vector<SnapshotLevel>& current_bids = snapshot_pair.first;
        std::vector<SnapshotLevel>& current_asks = snapshot_pair.second;

        if (!current_bids.empty() || !current_asks.empty()) {
            if (current_bids != last_written_bids || current_asks != last_written_asks) {
                write_snapshot(f_out, current_tops_record->ts, current_bids, current_asks);
                num_snapshots_written++;
                last_written_bids = current_bids;
                last_written_asks = current_asks;
            }
        }
    }
    
    f_in.close();

    // Write the final main header
    f_out.seekp(0, std::ios::beg);
    OutputFileHeader final_output_header;
    final_output_header.feed_id = PROCESSED_SNAPSHOT_FILE_FEED_ID;
    final_output_header.dateint = input_header.dateint;
    final_output_header.num_snapshots = num_snapshots_written;
    final_output_header.symbol_idx = input_header.symbol_idx;
    f_out.write(reinterpret_cast<const char*>(&final_output_header), sizeof(OutputFileHeader));
    
    f_out.close();

    std::cout << "Successfully generated snapshot file: '" << output_filepath << "' with " << num_snapshots_written << " snapshots." << std::endl;

    return 0;
}