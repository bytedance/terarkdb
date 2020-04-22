#pragma once

#include "rocksdb/utilities/stackable_db.h"

namespace rocksdb {

class JuxtaposeDB : public StackableDB {
  DB* db_ref_;
  std::unordered_map<ColumnFamilyHandle*, ColumnFamilyHandle*> cfh_map_;
  std::unordered_map<const Snapshot*, const Snapshot*> snapshot_map_;

 public:
  JuxtaposeDB(DB* db, DB* db_ref) : StackableDB(db), db_ref_(db_ref) {
    cfh_map_[db_->DefaultColumnFamily()] = db_ref_->DefaultColumnFamily();
  }

  using StackableDB::Close;
  virtual Status Close() override {
    cfh_map_.erase(db_->DefaultColumnFamily());
    assert(cfh_map_.empty());
    assert(snapshot_map_.empty());
    Status db_status = db_->Close();
    Status ref_status = db_ref_->Close();
    assert(db_status == ref_status);
    delete db_ref_;
    return db_status;
  }

  using StackableDB::CreateColumnFamily;
  Status CreateColumnFamily(const ColumnFamilyOptions& options,
                            const ColumnFamilyOptions& ref_options,
                            const std::string& column_family_name,
                            ColumnFamilyHandle** handle) {
    Status db_status =
        db_->CreateColumnFamily(options, column_family_name, handle);
    ColumnFamilyHandle* new_handle;
    Status ref_status = db_ref_->CreateColumnFamily(
        ref_options, column_family_name, &new_handle);
    assert(db_status == ref_status);
    cfh_map_[*handle] = new_handle;
    return db_status;
  }

  using StackableDB::CreateColumnFamilies;
  Status CreateColumnFamilies(
      const ColumnFamilyOptions& options,
      const ColumnFamilyOptions& ref_options,
      const std::vector<std::string>& column_family_names,
      std::vector<ColumnFamilyHandle*>* handles) {
    Status db_status =
        db_->CreateColumnFamilies(options, column_family_names, handles);
    std::vector<ColumnFamilyHandle*> new_handles;
    Status ref_status = db_ref_->CreateColumnFamilies(
        ref_options, column_family_names, &new_handles);
    assert(db_status == ref_status);
    assert(new_handles.size() == handles->size());
    for (auto i = 0; i < handles->size(); ++i) {
      cfh_map_[(*handles)[i]] = new_handles[i];
    }
    return db_status;
  }

  using StackableDB::CreateColumnFamilies;
  virtual Status CreateColumnFamilies(
      const std::vector<ColumnFamilyDescriptor>& column_families,
      std::vector<ColumnFamilyHandle*>* handles) override {
    Status db_status = db_->CreateColumnFamilies(column_families, handles);
    std::vector<ColumnFamilyHandle*> new_handles;
    Status ref_status =
        db_ref_->CreateColumnFamilies(column_families, &new_handles);
    assert(db_status == ref_status);
    assert(new_handles.size() == handles->size());
    for (auto i = 0; i < handles->size(); ++i) {
      cfh_map_[(*handles)[i]] = new_handles[i];
    }
    return db_status;
  }

  using StackableDB::DropColumnFamily;
  virtual Status DropColumnFamily(ColumnFamilyHandle* column_family) override {
    Status db_status = db_->DropColumnFamily(column_family);
    Status ref_status = db_ref_->DropColumnFamily(cfh_map_[column_family]);
    cfh_map_.erase(column_family);
    assert(db_status == ref_status);
    return db_status;
  }

  using StackableDB::DropColumnFamilies;
  virtual Status DropColumnFamilies(
      const std::vector<ColumnFamilyHandle*>& column_families) override {
    Status db_status = db_->DropColumnFamilies(column_families);
    std::vector<ColumnFamilyHandle*> ref_column_families;
    for (auto& h : column_families) {
      ref_column_families.push_back(cfh_map_[h]);
    }
    Status ref_status = db_ref_->DropColumnFamilies(ref_column_families);
    for (auto& h : column_families) {
      cfh_map_.erase(h);
    }
    assert(db_status == ref_status);
    return db_status;
  }

  using StackableDB::DestroyColumnFamilyHandle;
  virtual Status DestroyColumnFamilyHandle(
      ColumnFamilyHandle* column_family) override {
    Status db_status = db_->DestroyColumnFamilyHandle(column_family);
    Status ref_status =
        db_ref_->DestroyColumnFamilyHandle(cfh_map_[column_family]);
    cfh_map_.erase(column_family);
    assert(db_status == ref_status);
    return db_status;
  }

  using StackableDB::Put;
  virtual Status Put(const WriteOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     const Slice& val) override {
    Status db_status = db_->Put(options, column_family, key, val);
    Status ref_status =
        db_ref_->Put(options, cfh_map_[column_family], key, val);
    assert(db_status == ref_status);
    return db_status;
  }

  using StackableDB::DefaultColumnFamily;
  virtual Status Put(const WriteOptions& options, const Slice& key,
                     const Slice& val) override {
    Status db_status = db_->Put(options, db_->DefaultColumnFamily(), key, val);
    Status ref_status =
        db_ref_->Put(options, db_ref_->DefaultColumnFamily(), key, val);
    assert(db_status == ref_status);
    return db_status;
  }

  using StackableDB::Get;
  virtual Status Get(const ReadOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& key,
                     LazyBuffer* value) override {
    Status db_status = db_->Get(options, column_family, key, value);
    LazyBuffer ref_value;
    ReadOptions options_ref = options;
    if (options.snapshot != nullptr) {
      options_ref.snapshot = snapshot_map_[options.snapshot];
    }
    Status ref_status =
        db_ref_->Get(options, cfh_map_[column_family], key, &ref_value);
    if (db_status.ok() && ref_status.ok()) {
      assert(value->ToString() == ref_value.ToString());
    }
    assert(db_status == ref_status);
    return db_status;
  }

  using StackableDB::MultiGet;
  virtual std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<Slice>& keys,
      std::vector<std::string>* values) override {
    std::vector<Status> db_status =
        db_->MultiGet(options, column_family, keys, values);
    std::vector<ColumnFamilyHandle*> ref_column_family;
    for (auto& h : column_family) {
      ref_column_family.push_back(cfh_map_[h]);
    }
    ReadOptions options_ref = options;
    if (options.snapshot != nullptr) {
      options_ref.snapshot = snapshot_map_[options.snapshot];
    }
    std::vector<std::string> ref_values;
    std::vector<Status> ref_status =
        db_ref_->MultiGet(options_ref, ref_column_family, keys, &ref_values);
    assert(db_status == ref_status);
    assert(*values == ref_values);
    return db_status;
  }

  using StackableDB::IngestExternalFile;
  virtual Status IngestExternalFile(
      ColumnFamilyHandle* column_family,
      const std::vector<std::string>& external_files,
      const IngestExternalFileOptions& options) override {
    IngestExternalFileOptions ref_options = options;
    ref_options.move_files = false;
    Status ref_status = db_ref_->IngestExternalFile(
        cfh_map_[column_family], external_files, ref_options);
    Status db_status =
        db_->IngestExternalFile(column_family, external_files, options);
    assert(db_status == ref_status);
    return db_status;
  }

  using StackableDB::Delete;
  virtual Status Delete(const WriteOptions& wopts,
                        ColumnFamilyHandle* column_family,
                        const Slice& key) override {
    Status db_status = db_->Delete(wopts, column_family, key);
    Status ref_status = db_ref_->Delete(wopts, cfh_map_[column_family], key);
    assert(db_status == ref_status);
    return db_status;
  }

  using StackableDB::SingleDelete;
  virtual Status SingleDelete(const WriteOptions& wopts,
                              ColumnFamilyHandle* column_family,
                              const Slice& key) override {
    Status db_status = db_->SingleDelete(wopts, column_family, key);
    Status ref_status =
        db_ref_->SingleDelete(wopts, cfh_map_[column_family], key);
    assert(db_status == ref_status);
    return db_status;
  }

  using StackableDB::Merge;
  virtual Status Merge(const WriteOptions& options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       const Slice& value) override {
    Status db_status = db_->Merge(options, column_family, key, value);
    Status ref_status =
        db_ref_->Merge(options, cfh_map_[column_family], key, value);
    assert(db_status == ref_status);
    return db_status;
  }

  using StackableDB::Write;
  virtual Status Write(const WriteOptions& opts, WriteBatch* updates) override {
    Status db_status = db_->Write(opts, updates);
    Status ref_status = db_ref_->Write(opts, updates);
    assert(db_status == ref_status);
    return db_status;
  }

  using StackableDB::NewIterator;

  class JuxtaposeDBIterator : public Iterator {
    JuxtaposeDB* rep_;
    Iterator* iter;
    Iterator* iter_ref;

   public:
    explicit JuxtaposeDBIterator(const ReadOptions& opts,
                                 ColumnFamilyHandle* column_family,
                                 JuxtaposeDB* rep)
        : rep_(rep) {
      iter = rep->db_->NewIterator(opts, column_family);
      ReadOptions opts_ref = opts;
      if (opts.snapshot != nullptr) {
        opts_ref.snapshot = rep->snapshot_map_[opts.snapshot];
      }
      iter_ref =
          rep->db_ref_->NewIterator(opts_ref, rep->cfh_map_[column_family]);
    }

    ~JuxtaposeDBIterator() {
      delete iter;
      delete iter_ref;
    }

    bool Valid() const override {
      assert(iter->Valid() == iter_ref->Valid());
      return iter->Valid();
    }

    void SeekToFirst() override {
      iter->SeekToFirst();
      iter_ref->SeekToFirst();
    }

    void SeekToLast() override {
      iter->SeekToLast();
      iter_ref->SeekToFirst();
    }

    void Seek(const Slice& target) override {
      iter->Seek(target);
      iter_ref->Seek(target);
    }

    void SeekForPrev(const Slice& target) override {
      iter->SeekForPrev(target);
      iter_ref->SeekForPrev(target);
    }

    void Next() override {
      iter->Next();
      iter_ref->Next();
    }

    void Prev() override {
      iter->Prev();
      iter_ref->Prev();
    }

    Slice key() const override {
      assert(iter->key() == iter_ref->key());
      return iter->key();
    }

    Slice value() const override {
      assert(iter->value() == iter_ref->value());
      return iter->value();
    }

    Status status() const override {
      assert(iter->status() == iter_ref->status());
      return iter->status();
    }

    Status GetProperty(std::string prop_name, std::string* prop) override {
      Status iter_status = iter->GetProperty(prop_name, prop);
      std::string prop_ref;
      Status iter_ref_status = iter_ref->GetProperty(prop_name, &prop_ref);
      assert(iter_status == iter_ref_status);
      assert(*prop == prop_ref);
      return iter_status;
    }
  };

  virtual Iterator* NewIterator(const ReadOptions& opts,
                                ColumnFamilyHandle* column_family) override {
    return new JuxtaposeDBIterator(opts, column_family, this);
  }

  using StackableDB::GetSnapshot;
  virtual const Snapshot* GetSnapshot() override {
    const Snapshot* s = db_->GetSnapshot();
    const Snapshot* s_ref = db_ref_->GetSnapshot();
    snapshot_map_[s] = s_ref;
    return s;
  }

  using StackableDB::ReleaseSnapshot;
  virtual void ReleaseSnapshot(const Snapshot* snapshot) override {
    db_->ReleaseSnapshot(snapshot);
    db_ref_->ReleaseSnapshot(snapshot_map_[snapshot]);
    snapshot_map_.erase(snapshot);
  }

  using StackableDB::CompactRange;
  virtual Status CompactRange(const CompactRangeOptions& options,
                              ColumnFamilyHandle* column_family,
                              const Slice* begin, const Slice* end) override {
    Status db_status = db_->CompactRange(options, column_family, begin, end);
    Status ref_status =
        db_ref_->CompactRange(options, cfh_map_[column_family], begin, end);
    assert(db_status == ref_status);
    return db_status;
  }

  using StackableDB::Flush;
  virtual Status Flush(const FlushOptions& fopts,
                       ColumnFamilyHandle* column_family) override {
    Status db_status = db_->Flush(fopts, column_family);
    Status ref_status = db_ref_->Flush(fopts, cfh_map_[column_family]);
    assert(db_status == ref_status);
    return db_status;
  }

  virtual Status Flush(
      const FlushOptions& fopts,
      const std::vector<ColumnFamilyHandle*>& column_families) override {
    Status db_status = db_->Flush(fopts, column_families);
    std::vector<ColumnFamilyHandle*> ref_column_families;
    for (auto& h : column_families) {
      ref_column_families.push_back(cfh_map_[h]);
    }
    Status ref_status = db_ref_->Flush(fopts, ref_column_families);
    assert(db_status == ref_status);
    return db_status;
  }

  using StackableDB::SyncWAL;
  virtual Status SyncWAL() override {
    Status db_status = db_->SyncWAL();
    Status ref_status = db_ref_->SyncWAL();
    assert(db_status == ref_status);
    return db_status;
  }

  using StackableDB::FlushWAL;
  virtual Status FlushWAL(bool sync) override {
    Status db_status = db_->FlushWAL(sync);
    Status ref_status = db_ref_->FlushWAL(sync);
    assert(db_status == ref_status);
    return db_status;
  }

  using StackableDB::SetOptions;
  virtual Status SetOptions(ColumnFamilyHandle* column_family_handle,
                            const std::unordered_map<std::string, std::string>&
                                new_options) override {
    Status db_status = db_->SetOptions(column_family_handle, new_options);
    Status ref_status =
        db_ref_->SetOptions(cfh_map_[column_family_handle], new_options);
    assert(db_status == ref_status);
    return db_status;
  }

  using StackableDB::ResetStats;
  virtual Status ResetStats() override {
    Status db_status = db_->ResetStats();
    Status ref_status = db_ref_->ResetStats();
    assert(db_status == ref_status);
    return db_status;
  }

  using StackableDB::SuggestCompactRange;
  virtual Status SuggestCompactRange(ColumnFamilyHandle* column_family,
                                     const Slice* begin,
                                     const Slice* end) override {
    Status db_status = db_->SuggestCompactRange(column_family, begin, end);
    Status ref_status =
        db_ref_->SuggestCompactRange(cfh_map_[column_family], begin, end);
    assert(db_status == ref_status);
    return db_status;
  }
};

}  // namespace rocksdb