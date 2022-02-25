//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_builder.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include <algorithm>
#include <atomic>
#include <functional>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "db/internal_stats.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "port/port.h"
#include "rocksdb/terark_namespace.h"
#include "util/c_style_callback.h"
#include "util/chash_map.h"
#include "util/chash_set.h"

#define ROCKS_VERSION_BUILDER_DEBUG 0

namespace TERARKDB_NAMESPACE {

bool NewestFirstBySeqNo(FileMetaData* a, FileMetaData* b) {
  if (a->fd.largest_seqno != b->fd.largest_seqno) {
    return a->fd.largest_seqno > b->fd.largest_seqno;
  }
  if (a->fd.smallest_seqno != b->fd.smallest_seqno) {
    return a->fd.smallest_seqno > b->fd.smallest_seqno;
  }
  // Break ties by file number
  return a->fd.GetNumber() > b->fd.GetNumber();
}

namespace {
bool BySmallestKey(FileMetaData* a, FileMetaData* b,
                   const InternalKeyComparator* cmp) {
  int r = cmp->Compare(a->smallest, b->smallest);
  if (r != 0) {
    return (r < 0);
  }
  // Break ties by file number
  return (a->fd.GetNumber() < b->fd.GetNumber());
}
}  // namespace

#if ROCKS_VERSION_BUILDER_DEBUG
struct VersionBuilderDebugger {
  struct Item {
    size_t deletion, addition;
  };
  std::vector<std::pair<int, uint64_t>> deletion;
  std::vector<std::pair<int, FileMetaData>> addition;
  std::vector<Item> pos;

  VersionBuilderDebugger() { pos.emplace_back(Item{0, 0}); }

  void PushEdit(VersionEdit* edit) {
    auto& edit_deletion = edit->GetDeletedFiles();
    deletion.insert(deletion.end(), edit_deletion.begin(), edit_deletion.end());

    auto& edit_addition = edit->GetNewFiles();
    addition.insert(addition.end(), edit_addition.begin(), edit_addition.end());

    pos.emplace_back(Item{deletion.size(), addition.size()});
  }

  void PushVersion(VersionStorageInfo* vstorage) {
    for (int i = -1; i < vstorage->num_levels(); ++i) {
      for (auto f : vstorage->LevelFiles(i)) {
        addition.emplace_back(i, *f);
        auto& back = addition.back().second;
        back.table_reader_handle = nullptr;
        back.refs = 0;
      }
    }
    pos.emplace_back(Item{deletion.size(), addition.size()});
  }

  void Verify(VersionBuilder::Rep* rep, VersionStorageInfo* vstorage);
};
#else
struct VersionBuilderDebugger {
  void PushEdit(VersionEdit*) {}
  void PushVersion(VersionStorageInfo*) {}
  void Verify(VersionBuilder::Rep*, VersionStorageInfo*) {}
};
#endif

struct VersionBuilderContextImpl : VersionBuilder::Context {
  VersionBuilderContextImpl()
      : table_cache(nullptr),
        levels(nullptr),
        dependence_version(0),
        new_deleted_files(0),
        maintainer_job_limit(0) {}

  ~VersionBuilderContextImpl() {
    for (auto& pair : dependence_map) {
      if (pair.second.f != nullptr) {
        UnrefFile(pair.second.f);
      }
    }
    delete[] levels;
  }

  struct DependenceItem {
    uint64_t file_number;
    size_t dependence_version;
    size_t gc_forbidden_version;
    bool is_estimation;
    int level;
    FileMetaData* f;
    double entry_depended;
  };
  struct InheritanceItem {
    size_t depended : 1;
    size_t count : sizeof(size_t) * 8 - 1;
    size_t item_pos;
  };

  void UnrefFile(FileMetaData* f) {
    if (f->Unref()) {
      if (f->table_reader_handle) {
        assert(table_cache != nullptr);
        table_cache->ReleaseHandle(f->table_reader_handle);
        f->table_reader_handle = nullptr;
      }
      delete f;
    }
  }

  TableCache* table_cache;
  chash_map<uint64_t, FileMetaData*>* levels;
  size_t dependence_version;
  size_t new_deleted_files;
  uint64_t maintainer_job_limit;
  chash_map<uint64_t, DependenceItem> dependence_map;
  chash_map<uint64_t, InheritanceItem> inheritance_counter;
};

class VersionBuilder::Rep {
  friend VersionBuilderDebugger;
  using DependenceItem = VersionBuilderContextImpl::DependenceItem;
  using InheritanceItem = VersionBuilderContextImpl::InheritanceItem;

 private:
  // Helper to sort files_ in v
  // kLevel0 -- NewestFirstBySeqNo
  // kLevelNon0 -- BySmallestKey
  struct FileComparator {
    enum SortMethod {
      kLevel0 = 0,
      kLevelNon0 = 1,
    } sort_method;
    const InternalKeyComparator* internal_comparator;

    FileComparator() : internal_comparator(nullptr) {}

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      switch (sort_method) {
        case kLevel0:
          return NewestFirstBySeqNo(f1, f2);
        case kLevelNon0:
          return BySmallestKey(f1, f2, internal_comparator);
      }
      assert(false);
      return false;
    }
  };

  std::unique_ptr<VersionBuilderContextImpl> context_;
  std::unique_ptr<VersionBuilderDebugger> debugger_;

  const EnvOptions& env_options_;
  Logger* info_log_;
  TableCache* table_cache_;
  VersionStorageInfo* base_vstorage_;
  int num_levels_;
  // Store states of levels larger than num_levels_. We do this instead of
  // storing them in levels to avoid regression in case there are no files
  // on invalid levels. The version is not consistent if in the end the files
  // on invalid levels don't cancel out.
  chash_map<int, chash_set<uint64_t>> invalid_levels_;
  // Whether there are invalid new files or invalid deletion on levels larger
  // than num_levels_.
  bool has_invalid_levels_;
  FileComparator level_zero_cmp_;
  FileComparator level_nonzero_cmp_;
  Status status_;

 public:
  Rep(const EnvOptions& env_options, Logger* info_log, TableCache* table_cache,
      VersionStorageInfo* base_vstorage, bool enable_debugger = false)
      : env_options_(env_options),
        info_log_(info_log),
        table_cache_(table_cache),
        base_vstorage_(base_vstorage),
        num_levels_(base_vstorage->num_levels()),
        has_invalid_levels_(false) {
    if (enable_debugger) {
      debugger_.reset(new VersionBuilderDebugger);
    }
  }

  void PutInheritance(DependenceItem* item, size_t item_pos) {
    auto& inheritance_counter = context_->inheritance_counter;
    bool replace = inheritance_counter.count(item->f->fd.GetNumber()) == 0;
    auto emplace = [&](uint64_t file_number) {
      auto ib = inheritance_counter.emplace(file_number,
                                            InheritanceItem{0, 1, item_pos});
      if (!ib.second) {
        ++ib.first->second.count;
        if (replace) {
          ib.first->second.item_pos = item_pos;
        }
      }
    };
    for (auto file_number : item->f->prop.inheritance) {
      emplace(file_number);
    }
    emplace(item->f->fd.GetNumber());
  }

  void DelInheritance(FileMetaData* f) {
    auto& inheritance_counter = context_->inheritance_counter;
    auto erase = [&](uint64_t file_number) {
      auto find = inheritance_counter.find(file_number);
      assert(find != inheritance_counter.end());
      if (--find->second.count == 0) {
        inheritance_counter.erase(find);
      }
    };
    for (auto file_number : f->prop.inheritance) {
      erase(file_number);
    }
    erase(f->fd.GetNumber());
  }

  void PutSst(FileMetaData* f, int level) {
    auto& dependence_map_ = context_->dependence_map;
    uint64_t file_number = f->fd.GetNumber();
    auto ib = dependence_map_.emplace(
        file_number, DependenceItem{file_number, 0, 0, false, level, f, 0});
    f->Ref();
    if (ib.second) {
      PutInheritance(&ib.first->second, ib.first.pos());
    } else {
      auto& item = ib.first->second;
      item.level = level;
      context_->UnrefFile(item.f);
      item.f = f;
    }
    if (level >= 0) {
      context_->levels[level].emplace(file_number, f);
    }
  }

  void DelSst(uint64_t file_number, int level) {
    auto& dependence_map = context_->dependence_map;
    assert(dependence_map.count(file_number) > 0);
    assert(level >= 0);
    dependence_map[file_number].level = -1;
    context_->levels[level].erase(file_number);
  }

  DependenceItem* TransFileNumber(uint64_t file_number) {
    auto& dependence_map = context_->dependence_map;
    auto& inheritance_counter = context_->inheritance_counter;
    auto find = inheritance_counter.find(file_number);
    if (find != inheritance_counter.end()) {
      return &dependence_map.pos(find->second.item_pos)->second;
    }
    return nullptr;
  }

  void SetDependence(FileMetaData* f, bool is_map, bool is_estimation,
                     double ratio, bool finish) {
    auto& dependence_map = context_->dependence_map;
    auto& inheritance_counter = context_->inheritance_counter;
    auto dependence_version = context_->dependence_version;
    for (auto& dependence : f->prop.dependence) {
      auto find = inheritance_counter.find(dependence.file_number);
      if (find == inheritance_counter.end()) {
        if (finish) {
          status_ = Status::Aborted("Missing dependence files");
        }
        continue;
      }
      assert(dependence_map.pos(find->second.item_pos) != dependence_map.end());
      auto item = &dependence_map.pos(find->second.item_pos)->second;
      if (finish) {
        find->second.depended = true;
        item->is_estimation |= is_estimation;
        assert(is_map || dependence.entry_count > 0);
        item->entry_depended += dependence.entry_count * ratio;
      }
      item->dependence_version = dependence_version;
      if (is_map) {
        item->gc_forbidden_version = dependence_version;
        if (!item->f->prop.dependence.empty()) {
          SetDependence(item->f, item->f->prop.is_map_sst(),
                        is_estimation || item->f->prop.is_map_sst(),
                        ratio * dependence.entry_count /
                            std::max<uint64_t>(1, item->f->prop.num_entries),
                        finish);
        }
      }
    }
  }

  void CalculateDependence(bool finish, bool is_open_db,
                           double maintainer_job_ratio) {
    if (!finish && (!is_open_db || context_->new_deleted_files < 65536)) {
      return;
    }
    auto& dependence_map = context_->dependence_map;
    auto& inheritance_counter = context_->inheritance_counter;
    auto dependence_version = ++context_->dependence_version;
    for (int level = 0; level < num_levels_; ++level) {
      for (auto& pair : context_->levels[level]) {
        auto file_number = pair.first;
        assert(dependence_map.count(file_number) > 0);
        assert(inheritance_counter.count(file_number) > 0);
        auto& item = dependence_map[file_number];
        assert(item.level >= 0);
        item.dependence_version = dependence_version;
        item.gc_forbidden_version = dependence_version;
        if (!item.f->prop.dependence.empty()) {
          SetDependence(item.f, item.f->prop.is_map_sst(),
                        item.f->prop.is_map_sst(), 1, finish);
        }
      }
    }

    std::vector<uint64_t> old_file_queue;
    constexpr size_t max_queue_size = 8;
    auto push_old_file = [&](FileMetaData* f) {
      if (!f->being_compacted && !f->prop.is_map_sst() &&
          !f->prop.dependence.empty() &&
          !f->has_marked_for_compaction(FileMetaData::kMarkedFromUpdateBlob)) {
        for (auto& dependence : f->prop.dependence) {
          if (TransFileNumber(dependence.file_number)->file_number !=
              dependence.file_number) {
            // item maybe invalid pointer, don't access it
            old_file_queue.push_back(f->fd.GetNumber());
            std::push_heap(old_file_queue.begin(), old_file_queue.end());
            if (old_file_queue.size() > max_queue_size) {
              std::pop_heap(old_file_queue.begin(), old_file_queue.end());
              old_file_queue.pop_back();
            }
            break;
          }
        }
      }
    };

    for (auto it = dependence_map.begin(); it != dependence_map.end();) {
      auto& item = it->second;
      if (item.dependence_version == dependence_version) {
        assert(inheritance_counter.count(it->first) > 0 &&
               inheritance_counter.find(it->first)->second.item_pos ==
                   it.pos());
        if (finish) {
          uint64_t entry_depended = std::max<uint64_t>(1, item.entry_depended);
          entry_depended = std::min(item.f->prop.num_entries, entry_depended);
          uint64_t num_antiquation = item.f->prop.num_entries - entry_depended;
          switch (item.f->gc_status) {
            case FileMetaData::kGarbageCollectionForbidden:
              if (item.gc_forbidden_version == dependence_version) {
                push_old_file(item.f);
              } else {
                if (item.f->refs > 1) {
                  // if item.f in other versions, that assigning
                  // item.f->gc_status to permitted might let this item
                  // participate in GC before current version was installed. It
                  // will cause database corruption.
                  auto f = new FileMetaData(*item.f);
                  f->table_reader_handle = nullptr;
                  f->refs = 1;
                  f->being_compacted = false;
                  context_->UnrefFile(item.f);
                  item.f = f;
                }
                item.f->gc_status = FileMetaData::kGarbageCollectionPermitted;
                item.f->marked_for_compaction &= FileMetaData::kMarkedFromUser;
              }
              break;
            case FileMetaData::kGarbageCollectionCandidate:
              assert(item.gc_forbidden_version != dependence_version);
              if (!item.is_estimation ||
                  num_antiquation != item.f->num_antiquation) {
                item.f->gc_status = FileMetaData::kGarbageCollectionPermitted;
              }
              break;
            case FileMetaData::kGarbageCollectionPermitted:
              assert(item.gc_forbidden_version != dependence_version);
              break;
          }
          item.f->num_antiquation = num_antiquation;
        }
        ++it;
      } else {
        if (!item.f->has_marked_for_compaction(
                FileMetaData::kMarkedFromUpdateBlob)) {
          context_->maintainer_job_limit +=
              uint64_t(item.f->fd.GetFileSize() * maintainer_job_ratio);
        }
        DelInheritance(item.f);
        context_->UnrefFile(item.f);
        it = dependence_map.erase(it);
      }
    }

    if (finish) {
      std::sort(old_file_queue.begin(), old_file_queue.end());
      for (uint64_t fn : old_file_queue) {
        FileMetaData* f = dependence_map[fn].f;
        if (f->fd.GetFileSize() > context_->maintainer_job_limit) {
          break;
        }
        context_->maintainer_job_limit -= f->fd.GetFileSize();
        f->marked_for_compaction |= FileMetaData::kMarkedFromUpdateBlob;
      }
    }

    context_->new_deleted_files = 0;
  }

  void CheckDependence(VersionStorageInfo* vstorage, FileMetaData* f,
                       bool is_map) {
    for (auto& dependence : f->prop.dependence) {
      auto item = TransFileNumber(dependence.file_number);
      if (item == nullptr) {
        fprintf(stderr, "Missing dependence files");
        abort();
      }
      if (is_map && !item->f->prop.dependence.empty()) {
        CheckDependence(vstorage, item->f, item->f->prop.is_map_sst());
      }
    }
  }

  void CheckConsistency(VersionStorageInfo* vstorage, bool check_dependence) {
    Init();
#ifdef NDEBUG
    if (!vstorage->force_consistency_checks()) {
      // Dont run consistency checks in release mode except if
      // explicitly asked to
      return;
    }
#endif
    // make sure the files are sorted correctly
    for (int level = 0; level < num_levels_; level++) {
      auto& level_files = vstorage->LevelFiles(level);
      if (check_dependence) {
        for (auto f : level_files) {
          if (!f->prop.dependence.empty()) {
            CheckDependence(vstorage, f, f->prop.is_map_sst());
          }
        }
      }
      for (size_t i = 1; i < level_files.size(); i++) {
        auto f1 = level_files[i - 1];
        auto f2 = level_files[i];
        if (level == 0) {
          if (!level_zero_cmp_(f1, f2)) {
            fprintf(stderr, "L0 files are not sorted properly");
            abort();
          }

          if (f2->fd.smallest_seqno == f2->fd.largest_seqno) {
            // This is an external file that we ingested
            SequenceNumber external_file_seqno = f2->fd.smallest_seqno;
            if (!(external_file_seqno < f1->fd.largest_seqno ||
                  external_file_seqno == 0)) {
              fprintf(stderr,
                      "L0 file with seqno %" PRIu64 " %" PRIu64
                      " vs. file with global_seqno %" PRIu64 "\n",
                      f1->fd.smallest_seqno, f1->fd.largest_seqno,
                      external_file_seqno);
              abort();
            }
          } else if (f1->fd.smallest_seqno <= f2->fd.smallest_seqno) {
            fprintf(stderr,
                    "L0 files seqno %" PRIu64 " %" PRIu64 " vs. %" PRIu64
                    " %" PRIu64 "\n",
                    f1->fd.smallest_seqno, f1->fd.largest_seqno,
                    f2->fd.smallest_seqno, f2->fd.largest_seqno);
            abort();
          }
        } else {
          if (!level_nonzero_cmp_(f1, f2)) {
            fprintf(stderr, "L%d files are not sorted properly", level);
            abort();
          }

          // Make sure there is no overlap in levels > 0
          if (vstorage->InternalComparator()->Compare(f1->largest,
                                                      f2->smallest) >= 0) {
            fprintf(stderr, "L%d have overlapping ranges %s vs. %s\n", level,
                    (f1->largest).DebugString(true).c_str(),
                    (f2->smallest).DebugString(true).c_str());
            abort();
          }
        }
      }
    }
  }

  void CheckConsistencyForDeletes(VersionEdit* /*edit*/, uint64_t number,
                                  int level) {
    Init();
#ifdef NDEBUG
    if (!base_vstorage_->force_consistency_checks()) {
      // Dont run consistency checks in release mode except if
      // explicitly asked to
      return;
    }
#endif
    // a file to be deleted better exist in the previous version
    bool found = false;
    for (int l = -1; !found && l < num_levels_; l++) {
      const std::vector<FileMetaData*>& base_files =
          base_vstorage_->LevelFiles(l);
      for (size_t i = 0; i < base_files.size(); i++) {
        FileMetaData* f = base_files[i];
        if (f->fd.GetNumber() == number) {
          found = true;
          break;
        }
      }
    }
    // if the file did not exist in the previous version, then it
    // is possibly moved from lower level to higher level in current
    // version
    for (int l = level + 1; !found && l < num_levels_; l++) {
      auto& level_added = context_->levels[l];
      auto got = level_added.find(number);
      if (got != level_added.end()) {
        found = true;
        break;
      }
    }

    // maybe this file was added in a previous edit that was Applied
    if (!found) {
      auto& level_added = context_->levels[level];
      auto got = level_added.find(number);
      if (got != level_added.end()) {
        found = true;
      }
    }
    if (!found) {
      fprintf(stderr, "not found %" PRIu64 "\n", number);
      abort();
    }
  }

  bool CheckConsistencyForNumLevels() {
    Init();
    // Make sure there are no files on or beyond num_levels().
    if (has_invalid_levels_) {
      return false;
    }
    for (auto& level : invalid_levels_) {
      if (level.second.size() > 0) {
        return false;
      }
    }
    return true;
  }

  void Init() {
    if (context_) {
      return;
    }

    // level -1 used for dependence files
    level_zero_cmp_.sort_method = FileComparator::kLevel0;
    level_nonzero_cmp_.sort_method = FileComparator::kLevelNon0;
    level_nonzero_cmp_.internal_comparator =
        base_vstorage_->InternalComparator();

    auto base_context = base_vstorage_->ReleaseVersionBuilderContext();
    if (base_context == nullptr) {
      context_.reset(new VersionBuilderContextImpl());
      context_->levels = new chash_map<uint64_t, FileMetaData*>[num_levels_];
      context_->table_cache = table_cache_;

      for (int level = -1; level < num_levels_; level++) {
        for (auto f : base_vstorage_->LevelFiles(level)) {
          PutSst(f, level);
        }
      }
      CheckConsistency(base_vstorage_, true);
    } else {
      context_.reset(static_cast<VersionBuilderContextImpl*>(base_context));
      assert(context_->table_cache == table_cache_);

      for (auto& pair : context_->dependence_map) {
        pair.second.is_estimation = false;
        pair.second.entry_depended = 0;
      }
      for (auto& pair : context_->inheritance_counter) {
        pair.second.depended = 0;
      }
    }

    if (debugger_) {
      debugger_->PushVersion(base_vstorage_);
    }
  }

  // Apply all of the edits in *edit to the current state.
  void Apply(VersionEdit* edit) {
    Init();
    CheckConsistency(base_vstorage_, false);
    if (debugger_) {
      debugger_->PushEdit(edit);
    }

    // Delete files
    const VersionEdit::DeletedFileSet& del = edit->GetDeletedFiles();
    context_->new_deleted_files += del.size();
    for (auto& pair : del) {
      int level = pair.first;
      auto file_number = pair.second;
      if (level < num_levels_) {
        CheckConsistencyForDeletes(edit, file_number, level);
        assert(context_->levels[level].count(file_number) > 0);
        DelSst(file_number, level);
      } else {
        auto exising = invalid_levels_[level].find(file_number);
        if (exising != invalid_levels_[level].end()) {
          invalid_levels_[level].erase(exising);
        } else {
          // Deleting an non-existing file on invalid level.
          has_invalid_levels_ = true;
        }
      }
    }

    // Add new files
    for (auto& pair : edit->GetNewFiles()) {
      int level = pair.first;
      if (level < num_levels_) {
        FileMetaData* f = new FileMetaData(pair.second);
        assert(f->table_reader_handle == nullptr);
        assert(level < 0 ||
               context_->levels[level].count(f->fd.GetNumber()) == 0);
        PutSst(f, level);
      } else {
        uint64_t number = pair.second.fd.GetNumber();
        if (invalid_levels_[level].count(number) == 0) {
          invalid_levels_[level].insert(number);
        } else {
          // Creating an already existing file on invalid level.
          has_invalid_levels_ = true;
        }
      }
    }

    // shrink files
    CalculateDependence(false, edit->is_open_db(), 0);
  }

  // Save the current state in *v.
  // WARNING: this func will call out of mutex
  void SaveTo(VersionStorageInfo* vstorage, double maintainer_job_ratio) {
    Init();
    CheckConsistency(vstorage, true);
    CalculateDependence(true, false, maintainer_job_ratio);
    auto exists = [&](uint64_t file_number) {
      auto find = context_->inheritance_counter.find(file_number);
      assert(find != context_->inheritance_counter.end());
      return bool(find->second.depended);
    };

    std::vector<double> read_amp(num_levels_);

    for (int level = 0; level < num_levels_; level++) {
      auto& cmp = (level == 0) ? level_zero_cmp_ : level_nonzero_cmp_;

      auto& unordered_added_files = context_->levels[level];
      vstorage->Reserve(level, unordered_added_files.size());

      // Sort files for the level.
      std::vector<FileMetaData*> ordered_added_files;
      ordered_added_files.reserve(unordered_added_files.size());
      for (const auto& pair : unordered_added_files) {
        ordered_added_files.push_back(pair.second);
      }
      std::sort(ordered_added_files.begin(), ordered_added_files.end(), cmp);

      for (auto f : ordered_added_files) {
        vstorage->AddFile(level, f, c_style_callback(exists), &exists,
                          info_log_);
        if (level == 0) {
          read_amp[level] += f->prop.read_amp;
        } else {
          read_amp[level] = std::max<double>(read_amp[level], f->prop.read_amp);
        }
      }
    }
    for (auto& pair : context_->dependence_map) {
      auto& item = pair.second;
      if (item.level == -1) {
        vstorage->AddFile(-1, item.f, c_style_callback(exists), &exists,
                          info_log_);
      }
      vstorage->UpdateAccumulatedStats(item.f);
    }
    vstorage->set_read_amplification(read_amp);
    vstorage->oldest_snapshot_seqnum(base_vstorage_->oldest_snapshot_seqnum());

    CheckConsistency(vstorage, true);
    if (debugger_) {
      debugger_->Verify(this, vstorage);
    }
    vstorage->ResetVersionBuilderContext(context_.release());
    vstorage->ComputeBlobOverlapScore();
    vstorage->CalculateEdge();
    vstorage->CalculateBlobInfo();
  }

  void LoadTableHandlers(InternalStats* internal_stats,
                         bool prefetch_index_and_filter_in_cache,
                         const SliceTransform* prefix_extractor,
                         bool load_essence_sst, int max_threads) {
    assert(table_cache_ != nullptr);
    Init();
    // <file metadata, level>
    std::vector<std::pair<FileMetaData*, int>> files_meta;
    auto dependence_version = context_->dependence_version;
    for (int level = 0; level < num_levels_; level++) {
      for (auto& file_meta_pair : context_->levels[level]) {
        auto* file_meta = file_meta_pair.second;
        if ((load_essence_sst || file_meta->prop.is_map_sst()) &&
            file_meta->table_reader_handle == nullptr) {
          files_meta.emplace_back(file_meta, level);
        }
      }
    }
    for (auto& pair : context_->dependence_map) {
      auto& item = pair.second;
      if (item.dependence_version == dependence_version && item.level == -1 &&
          (load_essence_sst || item.f->prop.is_map_sst()) &&
          item.f->table_reader_handle == nullptr) {
        files_meta.emplace_back(item.f, -1);
      }
    }
    if (files_meta.empty()) {
      return;
    }

    std::atomic<size_t> next_file_meta_idx(0);
    std::function<void()> load_handlers_func([&]() {
      while (true) {
        size_t file_idx = next_file_meta_idx.fetch_add(1);
        if (file_idx >= files_meta.size()) {
          break;
        }

        auto* file_meta = files_meta[file_idx].first;
        int level = files_meta[file_idx].second;
        auto file_read_hist =
            level >= 0 ? internal_stats->GetFileReadHist(level) : nullptr;
        table_cache_->FindTable(
            env_options_, file_meta->fd, &file_meta->table_reader_handle,
            prefix_extractor, false /*no_io */, true /* record_read_stats */,
            file_read_hist, false, level, prefetch_index_and_filter_in_cache,
            file_meta->prop.is_map_sst());
        if (file_meta->table_reader_handle != nullptr) {
          // Load table_reader
          file_meta->fd.table_reader = table_cache_->GetTableReaderFromHandle(
              file_meta->table_reader_handle);
        }
      }
    });

    std::vector<port::Thread> threads;
    for (int i = 1; i < max_threads; i++) {
      threads.emplace_back(load_handlers_func);
    }
    load_handlers_func();
    for (auto& t : threads) {
      t.join();
    }
  }

  void UpgradeFileMetaData(const SliceTransform* prefix_extractor,
                           int max_threads) {
    Init();
    assert(table_cache_ != nullptr);
    std::vector<FileMetaData*> files_meta;
    auto dependence_version = context_->dependence_version;
    for (auto& pair : context_->dependence_map) {
      auto& item = pair.second;
      if (item.dependence_version == dependence_version &&
          item.f->need_upgrade) {
        files_meta.emplace_back(item.f);
      }
    }
    if (files_meta.empty()) {
      return;
    }

    std::atomic<size_t> next_file_meta_idx(0);
    std::function<void()> upgrade_func([&]() {
      while (true) {
        size_t file_idx = next_file_meta_idx.fetch_add(1);
        if (file_idx >= files_meta.size()) {
          break;
        }

        auto* file_meta = files_meta[file_idx];
        std::shared_ptr<const TableProperties> properties;

        auto s = table_cache_->GetTableProperties(env_options_, *file_meta,
                                                  &properties, prefix_extractor,
                                                  false /*no_io */);

        if (s.ok() && properties) {
          file_meta->prop.num_entries = properties->num_entries;
          file_meta->prop.num_deletions = properties->num_deletions;
          file_meta->prop.raw_key_size = properties->raw_key_size;
          file_meta->prop.raw_value_size = properties->raw_value_size;
        }
      }
    });

    std::vector<port::Thread> threads;
    for (int i = 1; i < max_threads; i++) {
      threads.emplace_back(upgrade_func);
    }
    upgrade_func();
    for (auto& t : threads) {
      t.join();
    }
  }
};

VersionBuilder::VersionBuilder(const EnvOptions& env_options,
                               TableCache* table_cache,
                               VersionStorageInfo* base_vstorage,
                               Logger* info_log)
    : rep_(new Rep(env_options, info_log, table_cache, base_vstorage,
                   true /* enable_debugger */)) {}

VersionBuilder::~VersionBuilder() { delete rep_; }

void VersionBuilder::CheckConsistency(VersionStorageInfo* vstorage) {
  rep_->CheckConsistency(vstorage, true);
}

void VersionBuilder::CheckConsistencyForDeletes(VersionEdit* edit,
                                                uint64_t number, int level) {
  rep_->CheckConsistencyForDeletes(edit, number, level);
}

bool VersionBuilder::CheckConsistencyForNumLevels() {
  return rep_->CheckConsistencyForNumLevels();
}

void VersionBuilder::Apply(VersionEdit* edit) { rep_->Apply(edit); }

void VersionBuilder::SaveTo(VersionStorageInfo* vstorage,
                            double maintainer_job_ratio) {
  rep_->SaveTo(vstorage, maintainer_job_ratio);
}

void VersionBuilder::LoadTableHandlers(InternalStats* internal_stats,
                                       bool prefetch_index_and_filter_in_cache,
                                       const SliceTransform* prefix_extractor,
                                       bool load_essence_sst, int max_threads) {
  rep_->LoadTableHandlers(internal_stats, prefetch_index_and_filter_in_cache,
                          prefix_extractor, load_essence_sst, max_threads);
}

void VersionBuilder::UpgradeFileMetaData(const SliceTransform* prefix_extractor,
                                         int max_threads) {
  rep_->UpgradeFileMetaData(prefix_extractor, max_threads);
}

#if ROCKS_VERSION_BUILDER_DEBUG
void VersionBuilderDebugger::Verify(VersionBuilder::Rep* rep,
                                    VersionStorageInfo* vstorage) {
  auto get_edit = [this](size_t i, VersionEdit* edit) {
    auto begin = pos[i], end = pos[i + 1];
    for (size_t j = begin.deletion; j < end.deletion; ++j) {
      auto& pair = deletion[j];
      edit->DeleteFile(pair.first, pair.second);
    }
    for (size_t j = begin.addition; j < end.addition; ++j) {
      auto& pair = addition[j];
      edit->AddFile(pair.first, pair.second);
    }
  };
  auto verify = [rep](VersionStorageInfo* l,
                      VersionStorageInfo* r) -> std::string {
    auto eq = TERARK_EQUAL_P(fd.GetNumber(), num_antiquation, gc_status);
    auto lt = TERARK_CMP_P(fd.GetNumber(), <, num_antiquation, <);
    /*
    auto eq = [](FileMetaData* fl, FileMetaData* fr) {
      return fl->fd.GetNumber() == fr->fd.GetNumber() &&
             fl->num_antiquation == fr->num_antiquation &&
             fl->gc_status == fr->gc_status;
    };
    auto lt = [](FileMetaData* fl, FileMetaData* fr) {
      return fl->fd.GetNumber() != fr->fd.GetNumber() ?
             fl->fd.GetNumber() < fr->fd.GetNumber() :
             fl->num_antiquation < fr->num_antiquation;
    };
    */
    using cmp = std::function<bool(FileMetaData*, FileMetaData*)>;
    auto debug_show = [&](const std::vector<FileMetaData*>& l_sst,
                          const std::vector<FileMetaData*>& r_sst,
                          const cmp& c) {
      std::vector<FileMetaData*> l_diff, r_diff;
      std::set_difference(l_sst.begin(), l_sst.end(), r_sst.begin(),
                          r_sst.end(), std::back_inserter(l_diff), c);
      std::set_difference(r_sst.begin(), r_sst.end(), l_sst.begin(),
                          l_sst.end(), std::back_inserter(r_diff), c);
      fprintf(stderr, "Diff %zd, %zd", l_diff.size(), r_diff.size());
    };
    for (int i = 0; i < l->num_levels(); ++i) {
      if (l->LevelFiles(i).size() != r->LevelFiles(i).size() ||
          std::mismatch(l->LevelFiles(i).begin(), l->LevelFiles(i).end(),
                        r->LevelFiles(i).begin(), eq)
                  .first != l->LevelFiles(i).end()) {
        debug_show(l->LevelFiles(i), r->LevelFiles(i),
                   i == 0 ? rep->level_zero_cmp_ : rep->level_nonzero_cmp_);
        char buffer[32];
        snprintf(buffer, sizeof buffer, "Level %d", i);
        return buffer;
      }
    }
    auto l_sst = l->LevelFiles(-1), r_sst = r->LevelFiles(-1);
    std::sort(l_sst.begin(), l_sst.end(), lt);
    std::sort(r_sst.begin(), r_sst.end(), lt);
    if (l_sst.size() != r_sst.size() ||
        std::mismatch(l_sst.begin(), l_sst.end(), r_sst.begin(), eq).first !=
            l_sst.end()) {
      debug_show(l_sst, r_sst, lt);
      return "Level -1";
    }
    std::vector<std::pair<uint64_t, uint64_t>> l_dep, r_dep;
    for (auto& pair : l->dependence_map()) {
      l_dep.emplace_back(pair.first, pair.second->fd.GetNumber());
    }
    for (auto& pair : r->dependence_map()) {
      r_dep.emplace_back(pair.first, pair.second->fd.GetNumber());
    }
    std::sort(l_dep.begin(), l_dep.end());
    std::sort(r_dep.begin(), r_dep.end());
    if (l_dep != r_dep) {
      return "Dependence";
    }
    return std::string();
  };

  bool has_err = false;
  for (size_t i = 1; i < pos.size() - 1; ++i) {
    VersionStorageInfo vstorage_0(
        vstorage->InternalComparator(),
        vstorage->InternalComparator()->user_comparator(),
        vstorage->num_levels(), kCompactionStyleNone, true);
    VersionBuilder::Rep rep_0(rep->env_options_, rep->info_log_,
                              rep->table_cache, &vstorage_0);
    for (size_t j = 0; j < i; ++j) {
      VersionEdit edit;
      get_edit(j, &edit);
      rep_0.Apply(&edit);
    }
    VersionStorageInfo vstorage_1(
        vstorage->InternalComparator(),
        vstorage->InternalComparator()->user_comparator(),
        vstorage->num_levels(), kCompactionStyleNone, true);
    rep_0.SaveTo(&vstorage_1, 0);
    VersionBuilder::Rep rep_1(rep->env_options_, rep->info_log_,
                              rep->table_cache, &vstorage_1);
    for (size_t j = i; j < pos.size() - 1; ++j) {
      VersionEdit edit;
      get_edit(j, &edit);
      rep_1.Apply(&edit);
    }
    rep_1.SaveTo(&vstorage_0, 0);
    auto err = verify(vstorage, &vstorage_0);
    if (!err.empty()) {
      has_err = true;
      fprintf(stderr,
              "VersionBuilder debug verify fail : edit count = %zd, "
              "break = %zd, error = %s\n",
              pos.size() - 2, i, err.c_str());
    }
    for (int j = -1; j < vstorage->num_levels(); ++j) {
      for (auto f : vstorage_0.LevelFiles(j)) {
        f->Unref();
      }
      for (auto f : vstorage_1.LevelFiles(j)) {
        f->Unref();
      }
    }
  }
  if (has_err) {
    abort();
  }
}
#endif

}  // namespace TERARKDB_NAMESPACE
