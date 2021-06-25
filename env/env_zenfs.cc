#ifdef LIBZBD
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/terark_namespace.h"
#include "third-party/zenfs/fs/fs_zenfs.h"
#include "third-party/zenfs/fs/zbd_zenfs.h"

namespace TERARKDB_NAMESPACE {

class ZenfsEnv : public EnvWrapper {
   public:
  // Initialize an EnvWrapper that delegates all calls to *t
  explicit ZenfsEnv(Env* t) : target_(t) {}
  ~EnvWrapper() override;

  Status InitZenfs(const std::string& zdb_path, const std::string& aux_path) {
    std::shared_ptr<Logger> logger;
    if (!((s = NewLogger("", &logger)).ok())) {
      return s;
    }
    ZonedBlockDevice* zbd = new ZonedBlockDevice(zdb_path, logger);
    if (!((s = zbd->Open()).ok())) {
      return s;
    }
    fs_ = new ZenFS(zbd, GetLegacyFileSystem(target_), logger);
    return Status::OK();
  }

  // Return the target to which this Env forwards all calls
  Env* target() const { return target_; }

  // The following text is boilerplate that forwards all methods to target()
  Status RegisterDbPaths(const std::vector<std::string>& paths) override {
    return fs_->RegisterDbPaths(paths);
  }

  Status UnregisterDbPaths(const std::vector<std::string>& paths) override {
    return fs_->UnregisterDbPaths(paths);
  }

  Status NewSequentialFile(const std::string& f,
                           std::unique_ptr<SequentialFile>* r,
                           const EnvOptions& options) override {
    return fs_->NewSequentialFile(f, r, options);
  }
  Status NewRandomAccessFile(const std::string& f,
                             std::unique_ptr<RandomAccessFile>* r,
                             const EnvOptions& options) override {
    return target_->NewRandomAccessFile(f, r, options);
  }
  Status NewWritableFile(const std::string& f, std::unique_ptr<WritableFile>* r,
                         const EnvOptions& options) override {
    return target_->NewWritableFile(f, r, options);
  }
  Status ReopenWritableFile(const std::string& fname,
                            std::unique_ptr<WritableFile>* result,
                            const EnvOptions& options) override {
    return target_->ReopenWritableFile(fname, result, options);
  }
  Status ReuseWritableFile(const std::string& fname,
                           const std::string& old_fname,
                           std::unique_ptr<WritableFile>* r,
                           const EnvOptions& options) override {
    return target_->ReuseWritableFile(fname, old_fname, r, options);
  }
  Status NewRandomRWFile(const std::string& fname,
                         std::unique_ptr<RandomRWFile>* result,
                         const EnvOptions& options) override {
    return target_->NewRandomRWFile(fname, result, options);
  }
  Status NewMemoryMappedFileBuffer(
      const std::string& fname,
      std::unique_ptr<MemoryMappedFileBuffer>* result) override {
    return target_->NewMemoryMappedFileBuffer(fname, result);
  }
  Status NewDirectory(const std::string& name,
                      std::unique_ptr<Directory>* result) override {
    return target_->NewDirectory(name, result);
  }
  Status FileExists(const std::string& f) override {
    return target_->FileExists(f);
  }
  Status GetChildren(const std::string& dir,
                     std::vector<std::string>* r) override {
    return target_->GetChildren(dir, r);
  }
  Status GetChildrenFileAttributes(
      const std::string& dir, std::vector<FileAttributes>* result) override {
    return target_->GetChildrenFileAttributes(dir, result);
  }
  Status DeleteFile(const std::string& f) override {
    return target_->DeleteFile(f);
  }
  Status Truncate(const std::string& fname, size_t size) override {
    return target_->Truncate(fname, size);
  }
  Status CreateDir(const std::string& d) override {
    return target_->CreateDir(d);
  }
  Status CreateDirIfMissing(const std::string& d) override {
    return target_->CreateDirIfMissing(d);
  }
  Status DeleteDir(const std::string& d) override {
    return target_->DeleteDir(d);
  }
  Status GetFileSize(const std::string& f, uint64_t* s) override {
    return target_->GetFileSize(f, s);
  }

  Status GetFileModificationTime(const std::string& fname,
                                 uint64_t* file_mtime) override {
    return target_->GetFileModificationTime(fname, file_mtime);
  }

  Status RenameFile(const std::string& s, const std::string& t) override {
    return target_->RenameFile(s, t);
  }

  Status LinkFile(const std::string& s, const std::string& t) override {
    return target_->LinkFile(s, t);
  }

  Status NumFileLinks(const std::string& fname, uint64_t* count) override {
    return target_->NumFileLinks(fname, count);
  }

  Status AreFilesSame(const std::string& first, const std::string& second,
                      bool* res) override {
    return target_->AreFilesSame(first, second, res);
  }

  Status LockFile(const std::string& f, FileLock** l) override {
    return target_->LockFile(f, l);
  }

  Status UnlockFile(FileLock* l) override { return target_->UnlockFile(l); }

  Status IsDirectory(const std::string& path, bool* is_dir) override {
    return target_->IsDirectory(path, is_dir);
  }

  Status LoadLibrary(const std::string& lib_name,
                     const std::string& search_path,
                     std::shared_ptr<DynamicLibrary>* result) override {
    return target_->LoadLibrary(lib_name, search_path, result);
  }

  void Schedule(void (*f)(void* arg), void* a, Priority pri,
                void* tag = nullptr, void (*u)(void* arg) = nullptr) override {
    return target_->Schedule(f, a, pri, tag, u);
  }

  int UnSchedule(void* tag, Priority pri) override {
    return target_->UnSchedule(tag, pri);
  }

  void StartThread(void (*f)(void*), void* a) override {
    return target_->StartThread(f, a);
  }
  void WaitForJoin() override { return target_->WaitForJoin(); }
  unsigned int GetThreadPoolQueueLen(Priority pri = LOW) const override {
    return target_->GetThreadPoolQueueLen(pri);
  }
  Status GetTestDirectory(std::string* path) override {
    return target_->GetTestDirectory(path);
  }
  Status NewLogger(const std::string& fname,
                   std::shared_ptr<Logger>* result) override {
    return target_->NewLogger(fname, result);
  }
  uint64_t NowMicros() override { return target_->NowMicros(); }
  uint64_t NowNanos() override { return target_->NowNanos(); }
  uint64_t NowCPUNanos() override { return target_->NowCPUNanos(); }

  void SleepForMicroseconds(int micros) override {
    target_->SleepForMicroseconds(micros);
  }
  Status GetHostName(char* name, uint64_t len) override {
    return target_->GetHostName(name, len);
  }
  Status GetCurrentTime(int64_t* unix_time) override {
    return target_->GetCurrentTime(unix_time);
  }
  Status GetAbsolutePath(const std::string& db_path,
                         std::string* output_path) override {
    return target_->GetAbsolutePath(db_path, output_path);
  }
  void SetBackgroundThreads(int num, Priority pri) override {
    return target_->SetBackgroundThreads(num, pri);
  }
  int GetBackgroundThreads(Priority pri) override {
    return target_->GetBackgroundThreads(pri);
  }

  Status SetAllowNonOwnerAccess(bool allow_non_owner_access) override {
    return target_->SetAllowNonOwnerAccess(allow_non_owner_access);
  }

  void IncBackgroundThreadsIfNeeded(int num, Priority pri) override {
    return target_->IncBackgroundThreadsIfNeeded(num, pri);
  }

  void LowerThreadPoolIOPriority(Priority pool) override {
    target_->LowerThreadPoolIOPriority(pool);
  }

  void LowerThreadPoolCPUPriority(Priority pool) override {
    target_->LowerThreadPoolCPUPriority(pool);
  }

  Status LowerThreadPoolCPUPriority(Priority pool, CpuPriority pri) override {
    return target_->LowerThreadPoolCPUPriority(pool, pri);
  }

  std::string TimeToString(uint64_t time) override {
    return target_->TimeToString(time);
  }

  Status GetThreadList(std::vector<ThreadStatus>* thread_list) override {
    return target_->GetThreadList(thread_list);
  }

  ThreadStatusUpdater* GetThreadStatusUpdater() const override {
    return target_->GetThreadStatusUpdater();
  }

  uint64_t GetThreadID() const override { return target_->GetThreadID(); }

  std::string GenerateUniqueId() override {
    return target_->GenerateUniqueId();
  }

  EnvOptions OptimizeForLogRead(const EnvOptions& env_options) const override {
    return target_->OptimizeForLogRead(env_options);
  }
  EnvOptions OptimizeForManifestRead(
      const EnvOptions& env_options) const override {
    return target_->OptimizeForManifestRead(env_options);
  }
  EnvOptions OptimizeForLogWrite(const EnvOptions& env_options,
                                 const DBOptions& db_options) const override {
    return target_->OptimizeForLogWrite(env_options, db_options);
  }
  EnvOptions OptimizeForManifestWrite(
      const EnvOptions& env_options) const override {
    return target_->OptimizeForManifestWrite(env_options);
  }
  EnvOptions OptimizeForCompactionTableWrite(
      const EnvOptions& env_options,
      const ImmutableDBOptions& immutable_ops) const override {
    return target_->OptimizeForCompactionTableWrite(env_options, immutable_ops);
  }
  EnvOptions OptimizeForCompactionTableRead(
      const EnvOptions& env_options,
      const ImmutableDBOptions& db_options) const override {
    return target_->OptimizeForCompactionTableRead(env_options, db_options);
  }
  EnvOptions OptimizeForBlobFileRead(
      const EnvOptions& env_options,
      const ImmutableDBOptions& db_options) const override {
    return target_->OptimizeForBlobFileRead(env_options, db_options);
  }
  Status GetFreeSpace(const std::string& path, uint64_t* diskfree) override {
    return target_->GetFreeSpace(path, diskfree);
  }
  void SanitizeEnvOptions(EnvOptions* env_opts) const override {
    target_->SanitizeEnvOptions(env_opts);
  }

 private:
  Env* target_;
  FileSystem* fs_;
};

Status NewZenfsEnv(Env** zenfs_env, const std::string& zdb_path, const std::string& aux_path, Env* base_env) {
    assert(zdb_path.length() > 0);
    assert(aux_path.length() > 0);
    assert(base_env != nullptr);
    auto env = new ZenfsEnv(base_env);
    Status s = env->InitZenfs(zdb_path, aux_path);
    *zenfs_env = s.ok() ? env : nullptr;
    return s;
}

}

#endif