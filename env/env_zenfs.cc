#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/terark_namespace.h"

#ifdef WITH_ZENFS
#include "third-party/zenfs/fs/fs_zenfs.h"
#include "util/zbd_stat.h"
#include "third-party/zenfs/fs/zbd_zenfs.h"
#include "utilities/trace/bytedance_metrics_histogram.h"

namespace TERARKDB_NAMESPACE {

class ZenfsSequentialFile : public SequentialFile {
 public:
  ZenfsSequentialFile(std::unique_ptr<FSSequentialFile>&& target)
      : target_(std::move(target)) {}

  virtual Status Read(size_t n, Slice* result, char* scratch) override {
    return target_->Read(n, IOOptions(), result, scratch, nullptr);
  }
  virtual Status Skip(uint64_t n) override { return target_->Skip(n); };

  virtual bool use_direct_io() const { return target_->use_direct_io(); }

  virtual size_t GetRequiredBufferAlignment() const {
    return target_->GetRequiredBufferAlignment();
  }

  virtual Status InvalidateCache(size_t offset, size_t length) {
    return target_->InvalidateCache(offset, length);
  }

  virtual Status PositionedRead(uint64_t offset, size_t n, Slice* result,
                                char* scratch) {
    return target_->PositionedRead(offset, n, IOOptions(), result, scratch,
                                   nullptr);
  }

 private:
  std::unique_ptr<FSSequentialFile> target_;
};

class ZenfsRandomAccessFile : public RandomAccessFile {
 public:
  ZenfsRandomAccessFile(std::unique_ptr<FSRandomAccessFile>&& target)
      : target_(std::move(target)) {}

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    return target_->Read(offset, n, IOOptions(), result, scratch, nullptr);
  }
  Status MultiRead(FSReadRequest* reqs, size_t num_reqs) override {
    return target_->MultiRead(reqs, num_reqs, IOOptions(), nullptr);
  }
  Status Prefetch(uint64_t offset, size_t n) override {
    return target_->Prefetch(offset, n, IOOptions(), nullptr);
  }
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }
  void Hint(AccessPattern pattern) override {
    target_->Hint((FSRandomAccessFile::AccessPattern)pattern);
  }
  bool use_direct_io() const override { return target_->use_direct_io(); }
  bool use_aio_reads() const final { return false; }
  bool is_mmap_open() const final { return false; }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  Status InvalidateCache(size_t offset, size_t length) override {
    return target_->InvalidateCache(offset, length);
  }
  virtual intptr_t FileDescriptor() const final { return -1; }

 private:
  std::unique_ptr<FSRandomAccessFile> target_;
};

class ZenfsWritableFile : public WritableFile {
 public:
  explicit ZenfsWritableFile(std::unique_ptr<FSWritableFile>&& target)
      : target_(std::move(target)) {}

  Status Append(const Slice& data) override {
    return target_->Append(data, IOOptions(), nullptr);
  }
  Status PositionedAppend(const Slice& data, uint64_t offset) override {
    return target_->PositionedAppend(data, offset, IOOptions(), nullptr);
  }
  Status Truncate(uint64_t size) override {
    return target_->Truncate(size, IOOptions(), nullptr);
  }
  // RocksDB may sync data after a file was closed (page cache enabled).
  // But for ZenFS, it will release active zone resource and reject any
  // futher writes.
  //
  // To address this problem we added a `Frozen` API which can notify ZenFS
  // it doesn't need this file again, so ZenFS could safely close it and since
  // ZenFS doesn't have page cache so later data sync would take no effect.
  //
  // For other filesystems(e.g. ext4) the logic is still reminds since Frozen()
  // will do nothing on page cache enabled filesystems.
  Status Frozen() override { return target_->Close(IOOptions(), nullptr); }
  Status Close() override { return target_->Close(IOOptions(), nullptr); }
  Status Flush() override { return target_->Flush(IOOptions(), nullptr); }
  Status Sync() override { return target_->Sync(IOOptions(), nullptr); }
  Status Fsync() override { return target_->Fsync(IOOptions(), nullptr); }
  bool IsSyncThreadSafe() const override { return target_->IsSyncThreadSafe(); }

  bool use_direct_io() const override { return target_->use_direct_io(); }

  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }

  void SetIOPriority(Env::IOPriority pri) override {
    target_->SetIOPriority(pri);
  }

  Env::IOPriority GetIOPriority() override { return target_->GetIOPriority(); }

  void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override {
    target_->SetWriteLifeTimeHint(hint);
  }

  Env::WriteLifeTimeHint GetWriteLifeTimeHint() override {
    return target_->GetWriteLifeTimeHint();
  }

  uint64_t GetFileSize() override {
    return target_->GetFileSize(IOOptions(), nullptr);
  }

  void SetPreallocationBlockSize(size_t size) override {
    target_->SetPreallocationBlockSize(size);
  }

  void GetPreallocationStatus(size_t* block_size,
                              size_t* last_allocated_block) override {
    target_->GetPreallocationStatus(block_size, last_allocated_block);
  }

  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }

  Status InvalidateCache(size_t offset, size_t length) override {
    return target_->InvalidateCache(offset, length);
  }

  Status RangeSync(uint64_t offset, uint64_t nbytes) override {
    return target_->RangeSync(offset, nbytes, IOOptions(), nullptr);
  }

  void PrepareWrite(size_t offset, size_t len) override {
    target_->PrepareWrite(offset, len, IOOptions(), nullptr);
  }

  Status Allocate(uint64_t offset, uint64_t len) override {
    return target_->Allocate(offset, len, IOOptions(), nullptr);
  }

 private:
  std::unique_ptr<FSWritableFile> target_;
};

class ZenfsDirectory : public Directory {
 public:
  explicit ZenfsDirectory(std::unique_ptr<FSDirectory>&& target)
      : target_(std::move(target)) {}

  Status Fsync() override { return target_->Fsync(IOOptions(), nullptr); }
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }

 private:
  std::unique_ptr<FSDirectory> target_;
};

class ZenfsEnv : public EnvWrapper {
 public:
  // Initialize an EnvWrapper that delegates all calls to *t
  explicit ZenfsEnv(Env* t) : EnvWrapper(t), target_(t) {}

  Status InitZenfs(
      const std::string& zdb_path, std::string bytedance_tags_,
      std::shared_ptr<MetricsReporterFactory> metrics_reporter_factory_) {
    //auto metrics = std::make_shared<NoZenFSMetrics>();
    auto metrics = std::make_shared<BDZenFSMetrics>(metrics_reporter_factory_, bytedance_tags_, nullptr);
    return NewZenFS(&fs_, zdb_path, metrics);
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
    std::unique_ptr<FSSequentialFile> file;
    IOStatus s = fs_->NewSequentialFile(f, options, &file, nullptr);
    if (s.ok()) {
      r->reset(new ZenfsSequentialFile(std::move(file)));
    }
    return s;
  }

  Status NewRandomAccessFile(const std::string& f,
                             std::unique_ptr<RandomAccessFile>* r,
                             const EnvOptions& options) override {
    std::unique_ptr<FSRandomAccessFile> file;
    IOStatus s = fs_->NewRandomAccessFile(f, options, &file, nullptr);
    if (s.ok()) {
      r->reset(new ZenfsRandomAccessFile(std::move(file)));
    }
    return s;
  }

  Status NewWritableFile(const std::string& f, std::unique_ptr<WritableFile>* r,
                         const EnvOptions& options) override {
    std::unique_ptr<FSWritableFile> file;

    // Generally, we should let our user to decide whether a file is WAL
    // or not. However, current TerarkDB environment doesn't provide such
    // capability to hint. Therefore, we simply check the suffix of filename
    // here.
    FileOptions foptions(options);
    static const std::string log_end = ".log";
    if (f.size() > log_end.size()) {
      bool is_wal = std::equal(log_end.rbegin(), log_end.rend(), f.rbegin());
      foptions.io_options.type = is_wal ? IOType::kWAL : IOType::kUnknown;
    }

    IOStatus s = fs_->NewWritableFile(f, foptions, &file, nullptr);
    if (s.ok()) {
      r->reset(new ZenfsWritableFile(std::move(file)));
    }
    return s;
  }

  Status ReopenWritableFile(const std::string& fname,
                            std::unique_ptr<WritableFile>* result,
                            const EnvOptions& options) override {
    return Status::NotSupported(
        "ReopenWritableFile  is not implemented in this FileSystem");
  }

  Status ReuseWritableFile(const std::string& fname,
                           const std::string& old_fname,
                           std::unique_ptr<WritableFile>* r,
                           const EnvOptions& options) override {
    return Status::NotSupported(
        "ReuseWritableFile is not implemented in this FileSystem");
  }

  Status NewRandomRWFile(const std::string& fname,
                         std::unique_ptr<RandomRWFile>* result,
                         const EnvOptions& options) override {
    return Status::NotSupported(
        "RandomRWFile is not implemented in this FileSystem");
  }

  Status NewMemoryMappedFileBuffer(
      const std::string& fname,
      std::unique_ptr<MemoryMappedFileBuffer>* result) override {
    return Status::NotSupported(
        "MemoryMappedFileBuffer is not implemented in this FileSystem");
  }
  Status NewDirectory(const std::string& name,
                      std::unique_ptr<Directory>* result) override {
    std::unique_ptr<FSDirectory> dir;
    Status s = fs_->NewDirectory(name, IOOptions(), &dir, nullptr);
    if (s.ok()) {
      result->reset(new ZenfsDirectory(std::move(dir)));
    }
    return s;
  }

  Status FileExists(const std::string& f) override {
    return fs_->FileExists(f, IOOptions(), nullptr);
  }

  Status GetChildren(const std::string& dir,
                     std::vector<std::string>* r) override {
    return fs_->GetChildren(dir, IOOptions(), r, nullptr);
  }

  Status GetChildrenFileAttributes(
      const std::string& dir, std::vector<FileAttributes>* result) override {
    return fs_->GetChildrenFileAttributes(dir, IOOptions(), result, nullptr);
  }

  Status DeleteFile(const std::string& f) override {
    return fs_->DeleteFile(f, IOOptions(), nullptr);
  }

  Status Truncate(const std::string& fname, size_t size) override {
    return fs_->Truncate(fname, size, IOOptions(), nullptr);
  }

  Status CreateDir(const std::string& d) override {
    return fs_->CreateDir(d, IOOptions(), nullptr);
  }

  Status CreateDirIfMissing(const std::string& d) override {
    return fs_->CreateDirIfMissing(d, IOOptions(), nullptr);
  }

  Status DeleteDir(const std::string& d) override {
    return fs_->DeleteDir(d, IOOptions(), nullptr);
  }

  Status GetFileSize(const std::string& f, uint64_t* s) override {
    return fs_->GetFileSize(f, IOOptions(), s, nullptr);
  }

  Status GetFileModificationTime(const std::string& fname,
                                 uint64_t* file_mtime) override {
    return fs_->GetFileModificationTime(fname, IOOptions(), file_mtime,
                                        nullptr);
  }

  Status RenameFile(const std::string& s, const std::string& t) override {
    return fs_->RenameFile(s, t, IOOptions(), nullptr);
  }

  Status LinkFile(const std::string& s, const std::string& t) override {
    return fs_->LinkFile(s, t, IOOptions(), nullptr);
  }

  Status NumFileLinks(const std::string& fname, uint64_t* count) override {
    return fs_->NumFileLinks(fname, IOOptions(), count, nullptr);
  }

  Status AreFilesSame(const std::string& first, const std::string& second,
                      bool* res) override {
    return fs_->AreFilesSame(first, second, IOOptions(), res, nullptr);
  }

  Status LockFile(const std::string& f, FileLock** l) override {
    return fs_->LockFile(f, IOOptions(), l, nullptr);
  }

  Status UnlockFile(FileLock* l) override {
    return fs_->UnlockFile(l, IOOptions(), nullptr);
  }

  Status IsDirectory(const std::string& path, bool* is_dir) override {
    return fs_->IsDirectory(path, IOOptions(), is_dir, nullptr);
  }

  Status LoadLibrary(const std::string& lib_name,
                     const std::string& search_path,
                     std::shared_ptr<DynamicLibrary>* result) override {
    return target_->LoadLibrary(lib_name, search_path, result);
  }

  void Schedule(void (*f)(void* arg), void* a, Priority pri,
                void* tag = nullptr, void (*u)(void* arg) = nullptr) override {
    target_->Schedule(f, a, pri, tag, u);
  }

  int UnSchedule(void* tag, Priority pri) override {
    return target_->UnSchedule(tag, pri);
  }

  void StartThread(void (*f)(void*), void* a) override {
    target_->StartThread(f, a);
  }
  void WaitForJoin() override { return target_->WaitForJoin(); }
  unsigned int GetThreadPoolQueueLen(Priority pri = LOW) const override {
    return target_->GetThreadPoolQueueLen(pri);
  }
  Status GetTestDirectory(std::string* path) override {
    return fs_->GetTestDirectory(IOOptions(), path, nullptr);
  }
  Status NewLogger(const std::string& fname,
                   std::shared_ptr<Logger>* result) override {
    return fs_->NewLogger(fname, IOOptions(), result, nullptr);
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
    target_->SetBackgroundThreads(num, pri);
  }
  int GetBackgroundThreads(Priority pri) override {
    return target_->GetBackgroundThreads(pri);
  }

  Status SetAllowNonOwnerAccess(bool allow_non_owner_access) override {
    return target_->SetAllowNonOwnerAccess(allow_non_owner_access);
  }

  void IncBackgroundThreadsIfNeeded(int num, Priority pri) override {
    target_->IncBackgroundThreadsIfNeeded(num, pri);
  }

  void LowerThreadPoolIOPriority(Priority pool) override {
    target_->LowerThreadPoolIOPriority(pool);
  }

  void LowerThreadPoolCPUPriority(Priority pool) override {
    target_->LowerThreadPoolCPUPriority(pool);
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
    return fs_->OptimizeForLogWrite(env_options, db_options);
  }
  EnvOptions OptimizeForManifestWrite(
      const EnvOptions& env_options) const override {
    return fs_->OptimizeForManifestWrite(env_options);
  }
  EnvOptions OptimizeForCompactionTableWrite(
      const EnvOptions& env_options,
      const ImmutableDBOptions& immutable_ops) const override {
    return fs_->OptimizeForCompactionTableWrite(env_options, immutable_ops);
  }
  EnvOptions OptimizeForCompactionTableRead(
      const EnvOptions& env_options,
      const ImmutableDBOptions& db_options) const override {
    return fs_->OptimizeForCompactionTableRead(env_options, db_options);
  }
  EnvOptions OptimizeForBlobFileRead(
      const EnvOptions& env_options,
      const ImmutableDBOptions& db_options) const override {
    return fs_->OptimizeForBlobFileRead(env_options, db_options);
  }
  Status GetFreeSpace(const std::string& path, uint64_t* diskfree) override {
    return fs_->GetFreeSpace(path, IOOptions(), diskfree, nullptr);
  }
  void SanitizeEnvOptions(EnvOptions* env_opts) const override {
    target_->SanitizeEnvOptions(env_opts);
  }

  Status GetZbdDiskSpaceInfo(uint64_t& total_size, uint64_t& avail_size,
                             uint64_t& used_size) {
    return Status::NotSupported("GetZbdDiskSpaceInfo is not implemented.");
  }

  void GetStat(BDZenFSStat& stat) {
    auto zen_fs = dynamic_cast<ZenFS*>(fs_);
    ZenFSSnapshot snapshot;
    ZenFSSnapshotOptions options;

    options.zbd_.enabled_ = 0;
    options.zone_.enabled_ = 1;
    options.zone_file_.enabled_ = 1;
    options.zone_extent_.enabled_ = 1;
    options.trigger_report_ = 0;
    options.as_lock_free_as_possible_ = 1;

    zen_fs->GetZenFSSnapshot(snapshot, options);
    stat.SetStat(snapshot, options);
  }

  void GetZenFSSnapshot(ZenFSSnapshot& snapshot, const ZenFSSnapshotOptions& options) {
    auto zen_fs = dynamic_cast<ZenFS*>(fs_);
    zen_fs->GetZenFSSnapshot(snapshot, options);
  }
  
  void Set_metrics_tag(std::string tag) { metrics_tag_ = tag; }
  std::string MetricsTag() { return metrics_tag_; }
 private:
  Env* target_;
  FileSystem* fs_;
  std::string metrics_tag_;
};

Status NewZenfsEnv(
    Env** zenfs_env, const std::string& zdb_path, std::string bytedance_tags_,
    std::shared_ptr<MetricsReporterFactory> metrics_reporter_factory_) {
  assert(zdb_path.length() > 0);
  auto env = new ZenfsEnv(Env::Default());
  env->Set_metrics_tag(zdb_path);
  Status s =
      env->InitZenfs(zdb_path, bytedance_tags_, metrics_reporter_factory_);
  *zenfs_env = s.ok() ? env : nullptr;
  return s;
}

void GetStat(Env* env, BDZenFSStat& stat) {
  auto zen_env = dynamic_cast<ZenfsEnv*>(env);
  if (zen_env)
    zen_env->GetStat(stat);
}

void GetZenFSSnapshot(Env* env, ZenFSSnapshot& snapshot, const ZenFSSnapshotOptions& options) {
  auto zen_env = dynamic_cast<ZenfsEnv*>(env);
  if (!zen_env) return;
  zen_env->GetZenFSSnapshot(snapshot, options);
}

std::string MetricsTag(Env* env) {
  auto zen_env = dynamic_cast<ZenfsEnv*>(env);
  if (!zen_env) return "";
  return zen_env->MetricsTag();
}

}  // namespace TERARKDB_NAMESPACE

#else

namespace TERARKDB_NAMESPACE {

Status NewZenfsEnv(
    Env** zenfs_env, const std::string& zdb_path, std::string bytedance_tags_,
    std::shared_ptr<MetricsReporterFactory> metrics_reporter_factory_) {
  *zenfs_env = nullptr;
  return Status::NotSupported("ZenFSEnv is not implemented.");
}

Status GetZbdDiskSpaceInfo(Env* env, uint64_t& total_size, uint64_t& avail_size,
                           uint64_t& used_size) {
  return Status::NotSupported("GetZbdDiskSpaceInfo is not implemented.");
}

void GetStat(Env* env, BDZenFSStat& stat) {}
void GetZenFSSnapshot(Env* env, ZenFSSnapshot& snapshot, const ZenFSSnapshotOptions& options) {}

}  // namespace TERARKDB_NAMESPACE

#endif
