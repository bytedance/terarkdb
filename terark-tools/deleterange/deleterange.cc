#include "deleterange.hpp"
#include <boost/range/iterator_range.hpp>
#include "boost/filesystem.hpp"

namespace terark {

using namespace boost::filesystem;

int CountRangeDeletions(const char* sst_fname, int& deletion_cnt) {
  auto s = DeleteRange::GetTableReader(sst_fname, deletion_cnt);
  if (!s.ok()) {
    std::cout << "file open error!" << sst_fname << std::endl;
  }
}

void ScanRangeDeletions(const char* db_path) {
  std::cout << "sst files from: " << db_path << std::endl;
  boost::filesystem::path p(db_path);
  int deletions = 0;
  for (auto& entry : boost::make_iterator_range(directory_iterator(p), {})) {
    auto ext = boost::filesystem::extension(entry);
    if (ext == ".sst") {
      std::cout << entry << std::endl;
      CountRangeDeletions(entry.path().string().data(), deletions);
    }
  }
  std::cout << "=====================" << std::endl;
  std::cout << "deletion count: " << deletions << std::endl;
}
}  // namespace terark

void PrintHelp() {
  std::cout << "usage:" << std::endl;
  std::cout << "./deleterange [target_dir]" << std::endl;
}

int main(const int argc, const char** argv) {
  const char* target_dir = nullptr;
  if (argc < 2) {
    PrintHelp();
    return 1;
  }
  setenv("TerarkZipTable_localTempDir", "./", true);

  target_dir = argv[1];
  terark::ScanRangeDeletions(target_dir);

  return 0;
}