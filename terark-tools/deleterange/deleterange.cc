#include "deleterange.hpp"

#ifdef WITH_BOOSTLIB
#include <boost/range/iterator_range.hpp>

#include "boost/filesystem.hpp"
#endif
namespace terark {

#ifdef WITH_BOOSTLIB
using namespace boost::filesystem;
#endif

int CountRangeDeletions(const char* sst_fname, int& deletion_cnt) {
  auto s = DeleteRange::GetTableReader(sst_fname, deletion_cnt);
  if (!s.ok()) {
    std::cout << "file open error!" << sst_fname << std::endl;
  }
}

void ScanRangeDeletions(const char* db_path) {
  std::cout << "sst files from: " << db_path << std::endl;
#ifdef WITH_BOOSTLIB
  boost::filesystem::path p(db_path);
#endif
  int deletions = 0;
  int cnt = 0;
#ifdef WITH_BOOSTLIB
  for (auto& entry : boost::make_iterator_range(directory_iterator(p), {})) {
    auto ext = boost::filesystem::extension(entry);
    if (ext == ".sst") {
      std::cout << "file cnt: " << ++cnt << ", deletion count: " << deletions
                << ", file" << entry << std::endl;
      CountRangeDeletions(entry.path().string().data(), deletions);
    }
  }
#endif
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