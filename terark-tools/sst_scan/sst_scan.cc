#include "sst_scan.hpp"

#include <boost/filesystem.hpp>
#include <stack>
#include <unordered_map>

using namespace rocksdb;

namespace rocksdb {
extern const uint64_t kPlainTableMagicNumber;
extern const uint64_t kLegacyPlainTableMagicNumber;
}  // namespace rocksdb

namespace terark {

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
  } else if (magic_number == kPlainTableMagicNumber) {
    std::cout << "Magic Number Table Type: "
              << "kPlainTableMagicNumber" << std::endl;
  } else if (magic_number == kLegacyPlainTableMagicNumber) {
    std::cout << "Magic Number Table Type: "
              << "kLegacyPlainTableMagicNumber" << std::endl;
  } else {
    std::cout << "Magic Number Table Type: "
              << "Unknown" << std::endl;
  }
}

uint64_t KeyPathAnalysis::GetMagicNumber(const std::string& sst_fname) {
  if (DEBUG_INFO)
    std::cout << "GetMagicNumber(" << sst_fname << ")..." << std::endl;
  uint64_t magic_number = Footer::kInvalidTableMagicNumber;
  Footer footer;

  std::unique_ptr<RandomAccessFile> file;
  uint64_t file_size = 0;
  Status s = options_.env->NewRandomAccessFile(sst_fname, &file, envOptions_);
  if (s.ok()) {
    s = options_.env->GetFileSize(sst_fname, &file_size);
    if (DEBUG_INFO) std::cout << "SST FileSize : " << file_size << std::endl;
  } else {
    std::cout << "GetMagicNumber(" << sst_fname << "), GetFileSize Failed!"
              << std::endl;
  }

  file_reader_.reset(new RandomAccessFileReader(std::move(file), sst_fname));
  s = ReadFooterFromFile(file_reader_.get(), nullptr, file_size, &footer);

  if (s.ok()) {
    magic_number = footer.table_magic_number();
    if (DEBUG_INFO) {
      std::cout << "Magic Number: " << magic_number << std::endl;
      printTableType(magic_number);
    }
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
  if (DEBUG_INFO)
    std::cout << "Try ReadTableProperties, file_size = " << file_size
              << std::endl;
  s = ReadTableProperties(file_reader_.get(), file_size, magic_number,
                          ioptions_, &table_properties);
  if (s.ok()) {
    table_properties_.reset(table_properties);
    // TODO init options based on different magic number
    TerarkZipConfigFromEnv(options_, options_);
  } else {
    std::cout << "Not able to read table properties" << std::endl;
    return s;
  }

  if (DEBUG_INFO)
    std::cout << "Creating Table Reader by options..." << std::endl;
  auto readerOptions =
      TableReaderOptions(ioptions_, nullptr, envOptions_, internal_comparator_);
  s = options_.table_factory->NewTableReader(
      readerOptions, std::move(file_reader_), file_size, &table_reader_);
  if (s.ok()) {
    if (DEBUG_INFO)
      std::cout << "Finish TableReader Creation for" << sst_fname << std::endl;
  } else {
    std::cout << "Failed to Build TableReader for sst file: " << sst_fname
              << std::endl;
    std::cout << "Status: " << s.ToString() << std::endl;
    return Status::Aborted();
  }
  return s;
}

/**
 * If you are using the keys printed from listkeys, please add
 * `FFFFFFFFFFFFFFFF`(seq + type) behind the key to get the value.
 * 
 * `Get` will return the largest seq record <= `seq + type`
 */
void KeyPathAnalysis::Get(const std::string& sst_fname, const Slice& key) {
  auto s = GetTableReader(sst_fname);

  LazyBuffer val;
  SequenceNumber context_seq;
  GetContext ctx(options_.comparator, options_.merge_operator.get(), nullptr,
                 nullptr, GetContext::GetState::kNotFound, ExtractUserKey(key),
                 &val, nullptr, nullptr, nullptr, nullptr, nullptr,
                 &context_seq, nullptr);

  if (DEBUG_INFO) {
    std::cout << "Table Entries: " << table_properties_->num_entries << ", ";
    std::cout << "Table CF Name: " << table_properties_->column_family_name
              << std::endl;
  }

  s = table_reader_->Get(ReadOptions(), key, &ctx, nullptr, false);
  if (!s.ok()) {
    std::cout << "Get Failed :" << s.ToString() << std::endl;
  }
  if (ctx.State() == GetContext::kFound || ctx.State() == GetContext::kMerge ||
      ctx.State() == GetContext::kDeleted) {
    ParsedInternalKey pik;
    pik.user_key = ExtractUserKey(key);
    pik.sequence = context_seq;
    if (ctx.State() == GetContext::kDeleted) {
      pik.type = kTypeDeletion;
    } else if (ctx.State() == GetContext::kFound) {
      pik.type = ctx.is_index() ? kTypeValueIndex : kTypeValue;
    } else {
      pik.type = ctx.is_index() ? kTypeMergeIndex : kTypeMerge;
    }
    std::cout << "Get Found !" << std::endl;
    std::cout << "  Key = " << pik.DebugString(true) << std::endl;
    s = val.fetch();
    if (s.ok()) {
      std::cout << "  Value = " << val.slice().ToString(true) << std::endl;
    } else {
      std::cout << "  Value Fetch Failed :" << s.ToString() << std::endl;
    }
  } else if (ctx.State() == GetContext::kNotFound) {
    std::cout << "Key Not Found !" << std::endl;
  } else if (ctx.State() == GetContext::kCorrupt) {
    s = std::move(ctx).CorruptReason();
    std::cout << "Get Failed :" << s.ToString() << std::endl;
  } else {
    assert(false);
  }
}

void KeyPathAnalysis::Seek(const std::string& sst_fname, const Slice& key) {
  auto s = GetTableReader(sst_fname);
  auto it = table_reader_->NewIterator(ReadOptions(), nullptr);

  std::cout << "Table Entries: " << table_properties_->num_entries << ", ";
  std::cout << "Table CF Name: " << table_properties_->column_family_name
            << std::endl;

  InternalKey ikey;
  ParsedInternalKey pik;
  ikey.SetMinPossibleForUserKey(key);
  std::cout << "Seek : " << Slice(key).ToString(true) << std::endl;
  bool found = false;
  for (it->Seek(ikey.Encode());
       it->Valid() && ParseInternalKey(it->key(), &pik) && pik.user_key == key;
       it->Next()) {
    found = true;

    std::cout << "Seek Found !" << std::endl;
    std::cout << "  Key = " << pik.DebugString(true) << std::endl;
    auto val = it->value();
    s = val.fetch();
    if (s.ok()) {
      std::cout << "  Value = " << val.slice().ToString(true) << std::endl;
    } else {
      std::cout << "  Value Fetch Failed :" << s.ToString() << std::endl;
    }
  }
  s = it->status();
  if (!s.ok()) {
    std::cout << "Seek Failed : " << s.ToString() << std::endl;
  } else if (!found) {
    std::cout << "Seek Not Found !" << std::endl;
  }
}

/**
 * List all keys in target sst file, including older versions.
 */
void KeyPathAnalysis::ListKeys(const std::string& sst_fname, bool print_val) {
  auto s = GetTableReader(sst_fname);
  auto it = table_reader_->NewIterator(ReadOptions(), nullptr);
  std::cout << "Print All Keys: " << std::endl;

  ParsedInternalKey pik;
  int cnt = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    cnt += 1;
    ParseInternalKey(it->key(), &pik);
    std::cout << "  Key = " << pik.DebugString(true) << std::endl;
    if (print_val) {
      auto val = it->value();
      s = val.fetch();
      if (s.ok()) {
        std::cout << "  Value = " << val.slice().ToString(true) << std::endl;
      } else {
        std::cout << "  Value Fetch Failed :" << s.ToString() << std::endl;
      }
    }
  }
  s = it->status();
  if (!s.ok()) {
    std::cout << "List Failed : " << s.ToString() << std::endl;
  }
  std::cout << "Total Key Count : " << cnt << std::endl;
}

void KeyPathAnalysis::ListAllEmptyValues(const std::string& sst_fname) {
  std::string last_key_prefix;
  int key_size = 0;

  auto s = GetTableReader(sst_fname);
  auto it = table_reader_->NewIterator(ReadOptions(), nullptr);
  // std::cout << "List ALl Empty Values: " << std::endl;

  ParsedInternalKey pik;
  int cnt = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    cnt += 1;
    ParseInternalKey(it->key(), &pik);

    auto val = it->value();
    s = val.fetch();
    if (s.ok()) {
      if (val.size() > 0) {
        std::string current_key_prefix = std::string(it->key().data(), 4);
        // std::cout << "prefix " << ToHex(current_key_prefix.c_str(), 4) <<
        // std::endl;

        if (key_size == 0 || current_key_prefix == last_key_prefix) {
          key_size++;
          last_key_prefix = current_key_prefix;
        } else {
          // process last prefix
          std::cout << sst_fname
                    << "  Key Prefix = " << ToHex(last_key_prefix.c_str(), 4)
                    << ", Count = " << key_size << std::endl;
          if (mp.find(last_key_prefix) != mp.end()) {
            mp[last_key_prefix] += key_size;
          } else {
            mp[last_key_prefix] = key_size;
          }
          // start new round
          last_key_prefix = current_key_prefix;
          key_size = 1;
        }
      }
    } else {
      std::cout << "  Value Fetch Failed :" << s.ToString() << std::endl;
    }
  }

  if (key_size > 0) {
    std::cout << sst_fname
              << "  Key Prefix = " << ToHex(last_key_prefix.c_str(), 4)
              << ", Count = " << key_size << std::endl;
    if (mp.find(last_key_prefix) != mp.end()) {
      mp[last_key_prefix] += key_size;
    } else {
      mp[last_key_prefix] = key_size;
    }
    key_size = 0;
  }

  s = it->status();
  if (!s.ok()) {
    std::cout << "List Failed : " << s.ToString() << std::endl;
  }
  // std::cout << "Total Key Count : " << cnt << std::endl;
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
  std::cout << "  ./sst_scan listkeys $target_sst "
               "[--dir|file] [--val]"
            << std::endl;
  std::cout << "  ./sst_scan getkey $target_sst $target_key "
               "[--dir|file] [--hex]"
            << std::endl;
  std::cout << "  ./sst_scan seekkey $target_sst $target_key "
               "[--dir|file] [--hex]"
            << std::endl;
  std::cout << "  ./sst_scan listemptyvalues $target_sst "
               "[--dir|file] [--hex] (temp, may corrupt)"
            << std::endl;
}

int main(const int argc, const char** argv) {
  setenv("TerarkZipTable_localTempDir", "./", true);
  const char* target_sst = nullptr;
  const char* target_key = nullptr;
  const char* command = nullptr;
  bool dir = false;
  bool hex = false;
  bool val = false;

  if (argc < 3) {
    PrintHelp();
    return 1;
  }
  for (int argv_i = 3; argv[argv_i]; ++argv_i) {
    if (strncmp("--dir", argv[argv_i], 5) == 0) {
      dir = true;
    }
    if (strncmp("--hex", argv[argv_i], 5) == 0) {
      hex = true;
    }
    if (strncmp("--val", argv[argv_i], 5) == 0) {
      val = true;
    }
  }

  std::cout << "dir = " << dir << ", hex = " << hex << ", val = " << val
            << std::endl;

  command = argv[1];
  target_sst = argv[2];
  target_key = argv[3];

  Slice target_key_slice(target_key);
  std::string tmp;
  if (hex) {
    target_key_slice.DecodeHex(&tmp);
    target_key_slice = Slice(tmp);
  }

  std::unique_ptr<terark::KeyPathAnalysis> kp(new terark::KeyPathAnalysis());

  if (memcmp(command, "listkeys", 8) == 0) {
    if (dir) {
      for (auto& p : boost::filesystem::directory_iterator(target_sst)) {
        auto fname = p.path().string().c_str();
        if (strncmp(boost::filesystem::extension(fname).c_str(), ".sst", 4) ==
            0) {
          std::cout << "Checking sst file: " << fname << std::endl;
          kp->ListKeys(fname, val);
        }
      }
    } else {
      kp->ListKeys(target_sst, val);
    }
  } else if (memcmp(command, "getkey", 6) == 0) {
    kp->DEBUG_INFO = false;
    assert(target_key);
    std::cout << "Get(" << target_key << ") from " << target_sst << std::endl;
    // if input target is a directory
    if (dir) {
      for (auto& p : boost::filesystem::directory_iterator(target_sst)) {
        auto fname = p.path().string().c_str();
        if (strncmp(boost::filesystem::extension(fname).c_str(), ".sst", 4) ==
            0) {
          // std::cout << "Checking sst file: " << fname << std::endl;
          kp->Get(fname, target_key_slice);
        }
      }
    } else {
      kp->Get(target_sst, target_key_slice);
    }

  } else if (memcmp(argv[1], "seekkey", 7) == 0) {
    std::cout << "Seek(" << target_key << ") from " << target_sst << std::endl;
    // if input target is a directory
    if (dir) {
      for (auto& p : boost::filesystem::directory_iterator(target_sst)) {
        auto fname = p.path().string().c_str();
        if (strncmp(boost::filesystem::extension(fname).c_str(), ".sst", 4) ==
            0) {
          std::cout << "Checking sst file: " << fname << std::endl;
          kp->Seek(fname, target_key_slice);
        }
      }
    } else {
      kp->Seek(target_sst, target_key_slice);
    }
  } else if (memcmp(argv[1], "listemptyvalues", 15) == 0) {
    kp->DEBUG_INFO = false;
    if (dir) {
      int i = 0;
      for (auto& p : boost::filesystem::directory_iterator(target_sst)) {
        std::cout << "file count : " << ++i << std::endl;
        auto fname = p.path().string().c_str();
        if (strncmp(boost::filesystem::extension(fname).c_str(), ".sst", 4) ==
            0) {
          // std::cout << "Checking sst file: " << fname << std::endl;
          kp->ListAllEmptyValues(fname);
        }
      }
    } else {
      kp->ListAllEmptyValues(target_sst);
    }

    // print all aggregates
    for (auto& it : kp->mp) {
      std::cout << "------ total aggragated -----" << std::endl;
      std::cout << "  Key Prefix = " << terark::ToHex(it.first.c_str(), 4)
                << ", Count = " << it.second << std::endl;
    }

  } else {
    std::cout << "Unsupported Operation!" << std::endl;
    PrintHelp();
    return 10;
  }
  return 0;
}
