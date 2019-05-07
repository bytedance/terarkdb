#include "terark_zip_common.h"
#include "terark_zip_table.h"
#include "terark_zip_index.h"
#include <terark/io/byte_swap.hpp>
#include <terark/util/throw.hpp>
#include <terark/util/mmap.hpp>
#include <stdlib.h>
#include <ctime>
#ifdef _MSC_VER
# include <io.h>
#else
# include <sys/types.h>
# include <sys/stat.h>
# include <fcntl.h>
# include <cxxabi.h>
#endif

namespace rocksdb {

const char* StrDateTimeNow() {
  static thread_local char buf[64];
  time_t rawtime;
  time(&rawtime);
  struct tm* timeinfo = localtime(&rawtime);
  strftime(buf, sizeof(buf), "%F %T", timeinfo);
  return buf;
}

std::string demangle(const char* name) {
#ifdef _MSC_VER
  return name;
#else
  int status = -4; // some arbitrary value to eliminate the compiler warning
  terark::AutoFree<char> res(abi::__cxa_demangle(name, NULL, NULL, &status));
  return (status == 0) ? res.p : name;
#endif
}

void AutoDeleteFile::Delete() {
  if (!fpath.empty()) {
    ::remove(fpath.c_str());
    fpath.clear();
  }
}
AutoDeleteFile::~AutoDeleteFile() {
  if (!fpath.empty()) {
    ::remove(fpath.c_str());
  }
}

TempFileDeleteOnClose::~TempFileDeleteOnClose() {
  if (fp)
    this->close();
}

/// this->path is temporary filename template such as: /some/dir/tmpXXXXXX
void TempFileDeleteOnClose::open_temp() {
  if (!terark::fstring(path).endsWith("XXXXXX")) {
    THROW_STD(invalid_argument,
              "ERROR: path = \"%s\", must ends with \"XXXXXX\"", path.c_str());
  }
#if _MSC_VER
  if (int err = _mktemp_s(&path[0], path.size() + 1)) {
    THROW_STD(invalid_argument, "ERROR: _mktemp_s(%s) = %s"
        , path.c_str(), strerror(err));
  }
  this->open();
#else
  int fd = mkstemp(&path[0]);
  if (fd < 0) {
    int err = errno;
    THROW_STD(invalid_argument, "ERROR: mkstemp(%s) = %s", path.c_str(), strerror(err));
  }
  this->dopen(fd);
#endif
}
void TempFileDeleteOnClose::open() {
  fp.open(path.c_str(), "wb+");
  fp.disbuf();
  writer.attach(&fp);
}
void TempFileDeleteOnClose::dopen(int fd) {
  fp.dopen(fd, "wb+");
  fp.disbuf();
  writer.attach(&fp);
}
void TempFileDeleteOnClose::close() {
  assert(nullptr != fp);
  writer.resetbuf();
  fp.close();
  ::remove(path.c_str());
}
void TempFileDeleteOnClose::complete_write() {
  writer.flush_buffer();
  fp.rewind();
}

class TerarkKeyIndexReaderBase : public TerarkKeyReader {
protected:
  terark::MmapWholeFile mmap;
  std::unique_ptr<TerarkIndex> index;
  void* buffer;
  TerarkIndex::Iterator* iter;
  bool move_next;
public:
  TerarkKeyIndexReaderBase(fstring fileName, size_t fileBegin, size_t fileEnd) {
    terark::MmapWholeFile(fileName).swap(mmap);
    index = TerarkIndex::LoadMemory(mmap.memory().substr(fileBegin, fileEnd - fileBegin));
    buffer = malloc(index->IteratorSize());
    iter = index->NewIterator(buffer);
  }
  ~TerarkKeyIndexReaderBase() {
    iter->~Iterator();
    free(buffer);
    index.reset();
    terark::MmapWholeFile().swap(mmap);
  }

  void rewind() override final {
    move_next = false;
  }
};

template<bool reverse>
class TerarkKeyIndexReader : public TerarkKeyIndexReaderBase {
public:
  using TerarkKeyIndexReaderBase::TerarkKeyIndexReaderBase;
  fstring next() override final {
    move_next = move_next ? reverse ? iter->SeekToLast() : iter->SeekToFirst() : reverse ? iter->Prev() : iter->Next();
    assert(move_next);
    return iter->key();
  }
};

class TerarkKeyFileReader : public TerarkKeyReader {
  const valvec<std::shared_ptr<FilePair>>& files;
  size_t index;
  NativeDataInput<InputBuffer> reader;
  valvec<byte_t> buffer;
  terark::var_uint64_t shared;
  FileStream stream;
  bool attach;
public:
  TerarkKeyFileReader(const valvec<std::shared_ptr<FilePair>>& _files, bool _attach) : files(_files), attach(_attach) {}

  fstring next() override final {
    if (reader.eof()) {
      FileStream* fp;
      if (attach) {
        fp = &files[++index]->key.fp;
      }
      else {
        stream.open(files[++index]->key.path, "rb");
        stream.disbuf();
        fp = &stream;
      }
      fp->rewind();
      reader.attach(fp);
    }
    reader >> shared;
    buffer.risk_set_size(shared);
    reader.load_add(buffer);
    return buffer;
  }
  void rewind() override final {
    index = 0;
    FileStream* fp;
    if (attach) {
      fp = &files.front()->key.fp;
    }
    else {
      stream.open(files.front()->key.path, "rb");
      stream.disbuf();
      fp = &stream;
    }
    fp->rewind();
    reader.attach(fp);
    shared = 0;
  }
};

TerarkKeyReader* TerarkKeyReader::MakeReader(fstring fileName, size_t fileBegin, size_t fileEnd, bool reverse) {
  if (reverse) {
    return new TerarkKeyIndexReader<true>(fileName, fileBegin, fileEnd);
  }
  else {
    return new TerarkKeyIndexReader<false>(fileName, fileBegin, fileEnd);
  }
}
TerarkKeyReader* TerarkKeyReader::MakeReader(const valvec<std::shared_ptr<FilePair>>& files, bool attach) {
  return new TerarkKeyFileReader(files, attach);
}

TerarkValueReader::TerarkValueReader(const valvec<std::shared_ptr<FilePair>>& _files) : files(_files) {}

void TerarkValueReader::checkEOF() {
  if (reader.eof()) {
    FileStream* fp = &files[++index]->value.fp;
    fp->rewind();
    reader.attach(fp);
  }
}

uint64_t TerarkValueReader::readUInt64() {
  checkEOF();
  return reader.load_as<uint64_t>();
}
void TerarkValueReader::appendBuffer(valvec<byte_t>* buffer) {
  checkEOF();
  reader.load_add(*buffer);
}

void TerarkValueReader::rewind() {
  index = 0;
  FileStream* fp = &files.front()->value.fp;
  fp->rewind();
  reader.attach(fp);
}

} // namespace rocksdb

