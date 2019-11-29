#include "manifest.hpp"

using namespace rocksdb;

namespace rocksdb {
extern const uint64_t kPlainTableMagicNumber;
extern const uint64_t kLegacyPlainTableMagicNumber;
}  // namespace rocksdb

namespace terark {

struct LogReporter : public log::Reader::Reporter {
  Status* status;
  virtual void Corruption(size_t sz /*bytes*/, const Status& s) override {
    if (this->status->ok()) *this->status = s;
    std::cout << "corruption, size = " << sz << std::endl;
  }
};

void ManifestAnalysis::ListCFNames(
    std::unique_ptr<SequentialFileReader>& file_reader,
    std::map<uint32_t, std::string>& column_family_names) {
  // default column family is always implicitly there
  column_family_names.insert({0, kDefaultColumnFamilyName});
  LogReporter reporter;
  Status s;
  reporter.status = &s;
  std::cout << "Construct manifest file log reader" << std::endl;
  log::Reader reader(nullptr, std::move(file_reader), &reporter, true, 0,
                     false);

  Slice record;
  std::string scratch;
  std::cout << "print all version edits..." << std::endl;
  while (reader.ReadRecord(&record, &scratch) && s.ok()) {
    VersionEdit edit;
    s = edit.DecodeFrom(record);
    if (!s.ok()) {
      std::cout << "Decode from manifest record, failed!" << std::endl;
      break;
    }
    std::cout << edit.DebugString() << std::endl;

    //   if (edit.is_column_family_add_) {
    //     if (column_family_names.find(edit.column_family_) !=
    //         column_family_names.end()) {
    //       s = Status::Corruption("Manifest adding the same column family
    //       twice"); break;
    //     }
    //     column_family_names.insert(
    //         {edit.column_family_, edit.column_family_name_});
    //   } else if (edit.is_column_family_drop_) {
    //     if (column_family_names.find(edit.column_family_) ==
    //         column_family_names.end()) {
    //       s = Status::Corruption(
    //           "Manifest - dropping non-existing column family");
    //       break;
    //     }
    //     column_family_names.erase(edit.column_family_);
    //   }
  }
}

void ManifestAnalysis::Validate(const std::string& manifest_fname) {
  std::unique_ptr<SequentialFileReader> manifest_reader;
  {
    std::unique_ptr<SequentialFile> manifest_file;
    auto s = options_.env->NewSequentialFile(manifest_fname, &manifest_file,
                                             envOptions_);
    if (!s.ok()) {
      std::cout << "Open Manifest File Error!" << std::endl;
      return;
    }

    manifest_reader.reset(
        new SequentialFileReader(std::move(manifest_file), manifest_fname));
  }

  std::cout << "Create SequentialFileReader for manifest corectly."
            << std::endl;

  uint64_t current_manifest_file_size;
  uint64_t current_manifest_edit_count = 0;
  auto s =
      options_.env->GetFileSize(manifest_fname, &current_manifest_file_size);
  if (!s.ok()) {
    std::cout << "GetFileSize failed!" << std::endl;
    return;
  }

  // List all column families
  std::map<uint32_t, std::string> column_family_names;
  ListCFNames(manifest_reader, column_family_names);
}
}  // namespace terark

void PrintHelp() {
  std::cout << "usage:" << std::endl;
  std::cout << "\t./manifest validate [manifest_file]" << std::endl;
}

int main(const int argc, const char** argv) {
  const char* manifest_fname = nullptr;
  if (argc < 3) {
    PrintHelp();
    return 1;
  }
  setenv("TerarkZipTable_localTempDir", "./", true);
  std::unique_ptr<terark::ManifestAnalysis> ma(new terark::ManifestAnalysis());
  manifest_fname = argv[2];

  if (memcmp(argv[1], "validate", 8) == 0) {
    ma->Validate(manifest_fname);
  } else {
    std::cout << "Unsupported Operation!" << std::endl;
    PrintHelp();
    return 10;
  }
  return 0;
}
