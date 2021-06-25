#include <rocksdb/env.h>

#include "rocksdb/terark_namespace.h"
#include "utilities/ioprof/ioprof.h"
#ifdef WITH_BOOSTLIB
#include <boost/current_function.hpp>
#else
#endif
#ifndef BOOST_CURRENT_FUNCTION
#define BOOST_CURRENT_FUNCTION "(unknown)"
#endif

namespace TERARKDB_NAMESPACE {

class IOProfSequentialFile : public SequentialFileWrapper {
 public:
  IOProfSequentialFile(SequentialFile* file) : SequentialFileWrapper(file) {}
  Status Read(size_t n, Slice* result, char* scratch) override {
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    return SequentialFileWrapper::Read(n, result, scratch);
  }

  Status PositionedRead(uint64_t offset, size_t n, Slice* result,
                        char* scratch) override {
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    return SequentialFileWrapper::PositionedRead(offset, n, result, scratch);
  }
};

class IOProfRandomAccessFile : public RandomAccessFileWrapper {
 public:
  IOProfRandomAccessFile(RandomAccessFile* file)
      : RandomAccessFileWrapper(file) {}
  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    return RandomAccessFileWrapper::Read(offset, n, result, scratch);
  };

  Status Prefetch(uint64_t offset, size_t n) override {
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    return RandomAccessFileWrapper::Prefetch(offset, n);
  }
};

class IOProfRandomRWFile : public RandomRWFileWrapper {
 public:
  IOProfRandomRWFile(RandomRWFile* file) : RandomRWFileWrapper(file) {}
  Status Write(uint64_t offset, const Slice& data) override {
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    return RandomRWFileWrapper::Write(offset, data);
  };

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    return RandomRWFileWrapper::Read(offset, n, result, scratch);
  };

  Status Flush() override {
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    return RandomRWFileWrapper::Flush();
  }

  Status Sync() override {
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    return RandomRWFileWrapper::Sync();
  }

  Status Fsync() override {
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    return RandomRWFileWrapper::Fsync();
  }
};

class IOProfWritableFile : public WritableFileWrapper {
 public:
  IOProfWritableFile(WritableFile* file) : WritableFileWrapper(file) {}
  Status Append(const Slice& data) final {
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    return WritableFileWrapper::Append(data);
  }
  Status PositionedAppend(const Slice& data, uint64_t offset) override {
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    return WritableFileWrapper::PositionedAppend(data, offset);
  }
  Status Truncate(uint64_t size) override {
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    return WritableFileWrapper::Truncate(size);
  }
  Status Flush() override {
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    return WritableFileWrapper::Flush();
  }
  Status Sync() override {
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    return WritableFileWrapper::Sync();
  }
  Status Fsync() override {
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    return WritableFileWrapper::Fsync();
  }
  Status RangeSync(uint64_t offset, uint64_t nbytes) override {
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    return WritableFileWrapper::RangeSync(offset, nbytes);
  }
  void PrepareWrite(size_t offset, size_t len) override {
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    WritableFileWrapper::PrepareWrite(offset, len);
  }

  Status Allocate(uint64_t offset, uint64_t len) override {
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    return WritableFileWrapper::Allocate(offset, len);
  }
};

class IOProfEnv : public EnvWrapper {
 public:
  IOProfEnv(Env* base_env) : EnvWrapper(base_env) {}

  Status NewSequentialFile(const std::string& f,
                           std::unique_ptr<SequentialFile>* r,
                           const EnvOptions& options) override {
    std::unique_ptr<SequentialFile> rt;
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    Status s = EnvWrapper::NewSequentialFile(f, &rt, options);
    if (s.ok()) {
      r->reset(new IOProfSequentialFile(rt.release()));
    }
    return s;
  }

  Status NewRandomAccessFile(const std::string& f,
                             std::unique_ptr<RandomAccessFile>* r,
                             const EnvOptions& options) override {
    std::unique_ptr<RandomAccessFile> rt;
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    Status s = EnvWrapper::NewRandomAccessFile(f, &rt, options);
    if (s.ok()) {
      r->reset(new IOProfRandomAccessFile(rt.release()));
    }
    return s;
  }

  Status NewWritableFile(const std::string& f, std::unique_ptr<WritableFile>* r,
                         const EnvOptions& options) override {
    std::unique_ptr<WritableFile> rt;
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    Status s = EnvWrapper::NewWritableFile(f, &rt, options);
    if (s.ok()) {
      r->reset(new IOProfWritableFile(rt.release()));
    }
    return s;
  }

  Status NewRandomRWFile(const std::string& f, std::unique_ptr<RandomRWFile>* r,
                         const EnvOptions& options) override {
    std::unique_ptr<RandomRWFile> rt;
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    Status s = EnvWrapper::NewRandomRWFile(f, &rt, options);
    if (s.ok()) {
      r->reset(new IOProfRandomRWFile(rt.release()));
    }
    return s;
  }

  Status DeleteFile(const std::string& f) override {
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    return EnvWrapper::DeleteFile(f);
  }

  Status NewDirectory(const std::string& name,
                      std::unique_ptr<Directory>* result) override {
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    return EnvWrapper::NewDirectory(name, result);
  }

  Status CreateDir(const std::string& name) override {
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    return EnvWrapper::CreateDir(name);
  }

  Status CreateDirIfMissing(const std::string& d) override {
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    return EnvWrapper::CreateDirIfMissing(d);
  }

  Status DeleteDir(const std::string& d) override {
    IOProfiler::Scope _scope_(BOOST_CURRENT_FUNCTION);
    return EnvWrapper::DeleteDir(d);
  }
};

Env* NewIOProfEnv(Env* base_env) { return new IOProfEnv(base_env); }

}  // namespace TERARKDB_NAMESPACE