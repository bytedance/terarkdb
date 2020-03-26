#include <boost/filesystem.hpp>

#include "key_path_analysis.hpp"

using namespace rocksdb;

namespace rocksdb {
extern const uint64_t kPlainTableMagicNumber;
extern const uint64_t kLegacyPlainTableMagicNumber;
}  // namespace rocksdb

namespace terark {

static void EscapeInternal(const Slice& input, std::string* output,
                           bool always_escape) {
  const char* src = input.data();
  const char* src_end = input.data() + input.size();
  char buffer[5];
  int blen = 5;
  for (; src < src_end; src++) {
    unsigned char c = *src;
    if (always_escape) {
      snprintf(buffer, blen, "\\x%02X", c);
      output->append(buffer, 4);
      continue;
    }
    switch (c) {
      case '\n':
        output->append("\\n");
        break;
      case '\r':
        output->append("\\r");
        break;
      case '\t':
        output->append("\\t");
        break;
      case '\"':
        output->append("\\\"");
        break;
      case '\'':
        output->append("\\\'");
        break;
      case '\\':
        output->append("\\\\");
        break;
      default:
        if (c < ' ' || c > '~') {
          snprintf(buffer, blen, "\\x%02X", c);
          output->append(buffer, 4);
        } else {
          output->push_back(c);
        }
        break;
    }
  }
}

std::string Escape(const Slice& src) {
  std::string dest;
  dest.reserve(src.size() * 4 + 1);
  EscapeInternal(src, &dest, false);
  return dest;
}

/**
 * Print table types and their magic number.
 */
void KeyPathAnalysis::printTableType(const uint64_t magic_number) {
  if (magic_number == kTerarkZipTableMagicNumber) {
    std::cout << "Magic Number Table Type: "
              << "kTerarkZipTableMagicNumber" << std::endl;
  } else if (magic_number == kBlockBasedTableMagicNumber) {
    std::cout << "Magic Number Table Type: "
              << "kBlockBasedTableMagicNumber" << std::endl;
  } else if (magic_number == kLegacyBlockBasedTableMagicNumber) {
    std::cout << "Magic Number Table Type: "
              << "kLegacyBlockBasedTableMagicNumber" << std::endl;
  } else if (magic_number == rocksdb::kPlainTableMagicNumber) {
    std::cout << "Magic Number Table Type: "
              << "kPlainTableMagicNumber" << std::endl;
  } else if (magic_number == rocksdb::kLegacyPlainTableMagicNumber) {
    std::cout << "Magic Number Table Type: "
              << "kLegacyPlainTableMagicNumber" << std::endl;
  } else {
    std::cout << "Magic Number Table Type: "
              << "Unknown" << std::endl;
  }
}

uint64_t KeyPathAnalysis::GetMagicNumber(const std::string& sst_fname) {
  // std::cout << "GetMagicNumber(" << sst_fname << ")..." << std::endl;
  uint64_t magic_number = Footer::kInvalidTableMagicNumber;
  Footer footer;

  std::unique_ptr<RandomAccessFile> file;
  uint64_t file_size = 0;
  Status s = options_.env->NewRandomAccessFile(sst_fname, &file, envOptions_);
  if (s.ok()) {
    s = options_.env->GetFileSize(sst_fname, &file_size);
    // std::cout << "SST FileSize : " << file_size << std::endl;
  } else {
    std::cout << "GetMagicNumber(" << sst_fname << "), GetFileSize Failed!"
              << std::endl;
  }

  file_reader_.reset(new RandomAccessFileReader(std::move(file), sst_fname));
  s = ReadFooterFromFile(file_reader_.get(), nullptr, file_size, &footer);

  if (s.ok()) {
    magic_number = footer.table_magic_number();
    // std::cout << "Magic Number: " << magic_number << std::endl;
    // printTableType(magic_number);
  } else {
    std::cout << "GetMagicNumber(" << sst_fname
              << "), Read Magic Number Failed!" << std::endl;
  }
  return magic_number;
}

Status KeyPathAnalysis::GetTableReader(const std::string& sst_fname) {
  auto magic_number = this->GetMagicNumber(sst_fname);
  std::unique_ptr<RandomAccessFile> file;

  // TererkZipTable have to use mmap to read sst files
  envOptions_.use_mmap_reads = true;
  options_.env->NewRandomAccessFile(sst_fname, &file, envOptions_);
  file_reader_.reset(new RandomAccessFileReader(std::move(file), sst_fname));
  options_.comparator = &internal_comparator_;

  // For old sst format, ReadTableProperties might fail but file can be read
  TableProperties* table_properties = nullptr;
  uint64_t file_size = 0;
  auto s = options_.env->GetFileSize(sst_fname, &file_size);
  // std::cout << "Try ReadTableProperties, file_size = " << file_size
  //          << std::endl;
  s = rocksdb::ReadTableProperties(file_reader_.get(), file_size, magic_number,
                                   ioptions_, &table_properties);
  if (s.ok()) {
    table_properties_.reset(table_properties);
    // TODO init options based on different magic number
    TerarkZipConfigFromEnv(options_, options_);
  } else {
    std::cout << "Not able to read table properties" << std::endl;
    return s;
  }

  // std::cout << "Creating Table Reader by options..." << std::endl;
  auto readerOptions =
      TableReaderOptions(ioptions_, nullptr, envOptions_, internal_comparator_);
  s = options_.table_factory->NewTableReader(
      readerOptions, std::move(file_reader_), file_size, &table_reader_);
  if (s.ok()) {
    // std::cout << "Finish TableReader Creation for" << sst_fname << std::endl;
  } else {
    std::cout << "Failed to Build TableReader for sst file: " << sst_fname
              << std::endl;
    std::cout << "Status: " << s.getState() << std::endl;
    return Status::Aborted();
  }
  return s;
}

void KeyPathAnalysis::Get(const std::string& sst_fname, const Slice& key) {
  auto s = GetTableReader(sst_fname);

  LazyBuffer val;
  rocksdb::GetContext ctx(options_.comparator, options_.merge_operator.get(),
                          nullptr, nullptr, GetContext::GetState::kNotFound,
                          key, &val, nullptr, nullptr, nullptr, nullptr,
                          nullptr, nullptr, nullptr);
  auto table_properties = table_reader_->GetTableProperties();
  std::cout << "Table Entries: " << table_properties_->num_entries << ", ";
  std::cout << "Table CF Name: " << table_properties_->column_family_name
            << std::endl;
  s = table_reader_->Get(rocksdb::ReadOptions(), key, &ctx, nullptr, false);
  if (s.ok()) {
    std::cout << "KEY FOUND, KEY = " << key.ToString(true) << std::endl;
  } else {
    std::cout << "KEY NOT FOUND!" << std::endl;
  }
}

void KeyPathAnalysis::Seek(const std::string& sst_fname, const Slice& key) {
  auto s = GetTableReader(sst_fname);
  auto it = table_reader_->NewIterator(rocksdb::ReadOptions(), nullptr);
  // std::cout << "Table Entries: " << table_properties_->num_entries <<
  // std::endl; std::cout << "Table CF Name: " <<
  // table_properties_->column_family_name
  //          << std::endl;

  // default sequence number is 0
  InternalKey ikey(key, 0, kTypeValue);
  it->Seek(ikey.Encode());

  // std::cout << "seek key: " << ikey.Encode().ToString(true) << std::endl;
  if (!it->Valid()) {
    std::cout << "seek fail, can't seek target key." << std::endl;
    // std::cout << "it->key()=" << it->key().ToString(true) << std::endl;
    return;
  } else {
    rocksdb::ParsedInternalKey parsed_key;
    rocksdb::ParseInternalKey(it->key(), &parsed_key);

    if (memcmp(parsed_key.user_key.data(), key.data(), key.size() - 8) != 0) {
      return;
    }
    std::cout << "found seek_ukey=" << parsed_key.user_key.ToString()
              << " | seq=" << parsed_key.sequence << std::endl;
    std::cout << "found Escape(seek_ukey)="
              << Escape(parsed_key.user_key.ToString()) << std::endl;
    auto value = it->value();
    std::cout << "found seek_value=" << value.ToString() << std::endl;
  }

  it->Next();
  while (it->Valid()) {
    std::cout << "\tstep forward:" << std::endl;
    rocksdb::ParsedInternalKey parsed_key;
    rocksdb::ParseInternalKey(it->key(), &parsed_key);
    std::cout << "\t\tseek_ukey=" << parsed_key.user_key.ToString(false)
              << " | seq=" << parsed_key.sequence << std::endl;
    if (memcmp(parsed_key.user_key.data(), key.data(), key.size() - 8) != 0) {
      return;
    }
    it->Next();
  }
}

/**
 * List all keys in target sst file, including older versions.
 */
void KeyPathAnalysis::ListKeys(const std::string& sst_fname, bool print_val) {
  auto s = GetTableReader(sst_fname);
  auto it = table_reader_->NewIterator(rocksdb::ReadOptions(), nullptr);
  std::cout << "Print all keys: " << std::endl;

  int cnt = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    cnt += 1;
    // auto key = it->key();
    rocksdb::ParsedInternalKey parsed_key;
    rocksdb::ParseInternalKey(it->key(), &parsed_key);
    std::cout << "len=" << parsed_key.user_key.size()
              << " | seq=" << parsed_key.sequence
              << " | type=" << parsed_key.type << " | "
              << parsed_key.user_key.ToString(true) << " | "
              << Escape(parsed_key.user_key) << " | "
              << parsed_key.user_key.ToString() << std::endl;
  }
  std::cout << "total key count: " << cnt << std::endl;
}
}  // namespace terark

/**
 * Print SST's detailed information, including:
 * 1) listkeys
 * 2) get(key)
 * 3) seek(key)
 */
void PrintHelp() {
  std::cout << "usage:" << std::endl;
  std::cout << "\t./key_path_analysis listkeys $target_sst" << std::endl;
  std::cout << "\t./key_path_analysis getkey $target_sst $target_key [--dir]"
            << std::endl;
  std::cout << "\t./key_path_analysis seekkey $target_sst $target_key [--dir]"
            << std::endl;
}

int main(const int argc, const char** argv) {
  const char* target_sst = nullptr;
  const char* target_key = nullptr;
  if (argc < 3) {
    PrintHelp();
    return 1;
  }

  const char bytekv_key[] =
      "\351\003\000\000\000\000\000\000@"
      "zXOSdEuh\tMFE0e\000\000\000\005\000\000\000\000\000\000\000\000";
  const int bytekv_sz = sizeof(bytekv_key) - 1;

  /*
  Slice
  hex("E90300000000000040777A656138365834097233344D3773306508EA146DF8C850A160");
  std::string decoded_hex;
  hex.DecodeHex(&decoded_hex);
  Slice key_slice(decoded_hex);
  */
  Slice key_slice(bytekv_key, bytekv_sz);

  setenv("TerarkZipTable_localTempDir", "./", true);
  std::unique_ptr<terark::KeyPathAnalysis> kp(new terark::KeyPathAnalysis());
  target_sst = argv[2];

  if (memcmp(argv[1], "listkeys", 8) == 0) {
    kp->ListKeys(target_sst, false);
  } else if (memcmp(argv[1], "getkey", 6) == 0) {
    target_key = argv[3];
    assert(target_key);
    // if input target is a directory
    if (argv[4] && strncmp("--dir", argv[4], 5) == 0) {
      std::cout << "Get(" << key_slice.ToString(true) << ") from " << target_sst
                << std::endl;
      boost::filesystem::recursive_directory_iterator p(target_sst), end;
      while (p != end) {
        auto fname = p->path().string().c_str();
        // std::cout << "checking file: " << fname << std::endl;
        if (strncmp(boost::filesystem::extension(fname).c_str(), ".sst", 4) ==
            0) {
          std::cout << "checking sst file: " << fname << std::endl;
          kp->Get(fname, key_slice);
        }
        ++p;
      }
    } else {
      std::cout << "Get from single file" << std::endl;
      kp->Get(target_sst, key_slice);
    }

  } else if (memcmp(argv[1], "seekkey", 7) == 0) {
    target_key = argv[3];
    assert(target_key);
    // if input target is a directory
    if (argv[4] && strncmp("--dir", argv[4], 5) == 0) {
      std::cout << "Seek(" << target_key << ") from " << target_sst
                << std::endl;
      for (auto& p :
           boost::filesystem::recursive_directory_iterator(target_sst)) {
        auto fname = p.path().string().c_str();
        // std::cout << "checking file: " << fname << std::endl;
        if (strncmp(boost::filesystem::extension(fname).c_str(), ".sst", 4) ==
            0) {
          std::cout << "checking sst file: " << fname << std::endl;
          // kp->Seek(fname, target_key);
          kp->Seek(fname, key_slice);
        }
      }
    } else {
      std::cout << "Seek(" << target_key << ") from single file " << target_sst
                << std::endl;
      kp->Seek(target_sst, key_slice);
    }
  } else {
    std::cout << "Unsupported Operation!" << std::endl;
    PrintHelp();
    return 10;
  }
  return 0;
}