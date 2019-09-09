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
#include <map>
#include <set>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "db/internal_stats.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "port/port.h"
#include "table/table_reader.h"
#include "util/c_style_callback.h"

namespace rocksdb {

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

class VersionBuilder::Rep {
 private:
  // Helper to sort files_ in v
  // kLevel0 -- NewestFirstBySeqNo
  // kLevelNon0 -- BySmallestKey
  struct FileComparator {
    enum SortMethod { kLevel0 = 0, kLevelNon0 = 1, } sort_method;
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

  struct LevelState {
    std::unordered_set<uint64_t> deleted_files;
    // Map from file number to file meta data.
    std::unordered_map<uint64_t, FileMetaData*> added_files;
  };
  struct DependenceItem {
    size_t dependence_version;
    size_t skip_gc_version;
    int level;
    FileMetaData* f;
  };
  struct InheritanceItem {
    size_t count;
    uint64_t file_number;
  };

  const EnvOptions& env_options_;
  Logger* info_log_;
  TableCache* table_cache_;
  VersionStorageInfo* base_vstorage_;
  int num_levels_;
  LevelState* levels_;
  std::unordered_set<uint64_t> active_sst_;
  size_t dependence_version_;
  std::unordered_map<uint64_t, DependenceItem> dependence_map_;
  std::unordered_map<uint64_t, InheritanceItem> inheritance_counter_;
  std::unordered_map<uint64_t, uint64_t> delta_antiquation_;
  // Store states of levels larger than num_levels_. We do this instead of
  // storing them in levels_ to avoid regression in case there are no files
  // on invalid levels. The version is not consistent if in the end the files
  // on invalid levels don't cancel out.
  std::map<int, std::unordered_set<uint64_t>> invalid_levels_;
  // Whether there are invalid new files or invalid deletion on levels larger
  // than num_levels_.
  bool has_invalid_levels_;
  FileComparator level_zero_cmp_;
  FileComparator level_nonzero_cmp_;
  Status status_;

 public:
  Rep(const EnvOptions& env_options, Logger* info_log, TableCache* table_cache,
      VersionStorageInfo* base_vstorage)
      : env_options_(env_options),
        info_log_(info_log),
        table_cache_(table_cache),
        base_vstorage_(base_vstorage),
        num_levels_(base_vstorage->num_levels()),
        dependence_version_(0),
        has_invalid_levels_(false) {
    levels_ = new LevelState[num_levels_ + 1] + 1;
    level_zero_cmp_.sort_method = FileComparator::kLevel0;
    level_nonzero_cmp_.sort_method = FileComparator::kLevelNon0;
    level_nonzero_cmp_.internal_comparator =
        base_vstorage_->InternalComparator();
  }

  ~Rep() {
    for (int level = -1; level < num_levels_; level++) {
      const auto& added = levels_[level].added_files;
      for (auto& pair : added) {
        UnrefFile(pair.second);
      }
    }
    for (auto& pair : dependence_map_) {
      UnrefFile(pair.second.f);
    }
    delete[] (levels_ - 1);
  }

  void UnrefFile(FileMetaData* f) {
    f->refs--;
    if (f->refs <= 0) {
      if (f->table_reader_handle) {
        assert(table_cache_ != nullptr);
        table_cache_->ReleaseHandle(f->table_reader_handle);
        f->table_reader_handle = nullptr;
      }
      delete f;
    }
  }

  void PutInheritance(FileMetaData* f) {
    bool replace = inheritance_counter_.count(f->fd.GetNumber()) == 0;
    for (auto file_number : f->prop.inheritance_chain) {
      auto ib =
          inheritance_counter_.emplace(file_number,
                                       InheritanceItem{1, f->fd.GetNumber()});
      if (!ib.second) {
        auto& item = ib.first->second;
        ++item.count;
        if (replace) {
          item.file_number = f->fd.GetNumber();
        }
      }
    }
  }

  void DelInheritance(FileMetaData* f) {
    for (auto file_number : f->prop.inheritance_chain) {
      auto find = inheritance_counter_.find(file_number);
      assert(find != inheritance_counter_.end());
      if (--find->second.count == 0) {
        if (dependence_map_.count(file_number == 0))
        inheritance_counter_.erase(find);
      }
    }
  }

  void ActiveSst(FileMetaData* f, int level, bool add_ref) {
    auto ib = dependence_map_.emplace(f->fd.GetNumber(),
                                      DependenceItem{0, 0, level, f});
    if (add_ref) {
      ++f->refs;
    }
    if (ib.second) {
      PutInheritance(f);
    } else {
      auto& item = ib.first->second;
      item.level = level;
      UnrefFile(item.f);
      item.f = f;
    }
    if (level >= 0) {
      active_sst_.emplace(f->fd.GetNumber());
    } else {
      active_sst_.erase(f->fd.GetNumber());
    }
  }

  void ReclaimSst(FileMetaData* f, bool add_ref) {
    auto ib = dependence_map_.emplace(f->fd.GetNumber(),
                                      DependenceItem{0, 0, -1, f});
    if (ib.second) {
      if (add_ref) {
        ++f->refs;
      }
      PutInheritance(f);
    } else {
      assert(!ib.first->second.f->being_compacted);
    }
  }

  uint64_t TransFileNumber(uint64_t file_number) {
    auto find = inheritance_counter_.find(file_number);
    if (find != inheritance_counter_.end()) {
      return find->second.file_number;
    }
    return file_number;
  }

  void SetDependence(FileMetaData* f, bool recursive, bool finish) {
    for (auto file_number : f->prop.dependence) {
      auto find = dependence_map_.find(TransFileNumber(file_number));
      if (find == dependence_map_.end()) {
        if (finish) {
          status_ = Status::Aborted("Missing dependence files");
        }
        continue;
      }
      auto& item = find->second;
      item.dependence_version = dependence_version_;
      if (recursive) {
        item.skip_gc_version = dependence_version_;
        SetDependence(item.f, item.f->prop.purpose == kMapSst, finish);
      }
    }
  }

  void CaclDependence(bool finish) {
    ++dependence_version_;
    for (auto file_number : active_sst_) {
      assert(inheritance_counter_.count(file_number) == 0);
      assert(dependence_map_.count(file_number) > 0);
      auto& item = dependence_map_[file_number];
      assert(item.level >= 0);
      item.skip_gc_version = dependence_version_;
      SetDependence(item.f, item.f->prop.purpose == kMapSst, finish);
    }
    for (auto it = dependence_map_.begin(); it != dependence_map_.end(); ) {
      auto& item = it->second;
      if (item.dependence_version == dependence_version_ || item.level >= 0 ||
          (!item.f->prop.inheritance_chain.empty() &&
           inheritance_counter_.count(item.f->fd.GetNumber()) == 0)) {
        assert(inheritance_counter_.count(item.f->fd.GetNumber()) == 0);
        ++it;
      } else {
        DelInheritance(item.f);
        UnrefFile(item.f);
        it = dependence_map_.erase(it);
      }
    }
  }

  void CheckDependence(VersionStorageInfo* vstorage, FileMetaData* f,
                       bool recursive) {
    auto& dependence_map = vstorage->dependence_map();
    for (auto file_number : f->prop.dependence) {
      auto find = dependence_map.find(file_number);
      if (find == dependence_map.end()) {
        fprintf(stderr, "Missing dependence files");
        abort();
      }
      if (recursive) {
        CheckDependence(vstorage, find->second,
                        find->second->prop.purpose == kMapSst);
      }
    }
  }

  void CheckConsistency(VersionStorageInfo* vstorage) {
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
      for (auto f : level_files) {
        CheckDependence(vstorage, f, f->prop.purpose == kMapSst);
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
      auto& level_added = levels_[l].added_files;
      auto got = level_added.find(number);
      if (got != level_added.end()) {
        found = true;
        break;
      }
    }

    // maybe this file was added in a previous edit that was Applied
    if (!found) {
      auto& level_added = levels_[level].added_files;
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

  // Apply all of the edits in *edit to the current state.
  void Apply(VersionEdit* edit) {
    CheckConsistency(base_vstorage_);

    // Delete files
    const VersionEdit::DeletedFileSet& del = edit->GetDeletedFiles();
    for (const auto& del_file : del) {
      const auto level = del_file.first;
      const auto number = del_file.second;
      if (level < num_levels_) {
        levels_[level].deleted_files.insert(number);
        CheckConsistencyForDeletes(edit, number, level);

        auto exising = levels_[level].added_files.find(number);
        if (exising != levels_[level].added_files.end()) {
          levels_[level].added_files.erase(exising);
          ActiveSst(exising->second, -1, false);
        }
      } else {
        auto exising = invalid_levels_[level].find(number);
        if (exising != invalid_levels_[level].end()) {
          invalid_levels_[level].erase(exising);
        } else {
          // Deleting an non-existing file on invalid level.
          has_invalid_levels_ = true;
        }
      }
    }

    // Add new files
    for (const auto& new_file : edit->GetNewFiles()) {
      const int level = new_file.first;
      if (level < num_levels_) {
        FileMetaData* f = new FileMetaData(new_file.second);
        f->refs = 2;

        assert(levels_[level].added_files.find(f->fd.GetNumber()) ==
               levels_[level].added_files.end());
        levels_[level].deleted_files.erase(f->fd.GetNumber());
        levels_[level].added_files[f->fd.GetNumber()] = f;
        ActiveSst(f, level, false);
      } else {
        uint64_t number = new_file.second.fd.GetNumber();
        if (invalid_levels_[level].count(number) == 0) {
          invalid_levels_[level].insert(number);
        } else {
          // Creating an already existing file on invalid level.
          has_invalid_levels_ = true;
        }
      }
    }

    // Remove files
    CaclDependence(false);

    for (auto& pair : edit->GetAntiquation()) {
      delta_antiquation_[pair.first] += pair.second;
    }
  }

  // Save the current state in *v.
  void SaveTo(VersionStorageInfo* vstorage) {
    CheckConsistency(base_vstorage_);
    CheckConsistency(vstorage);

    std::vector<double> read_amp(num_levels_);

    for (auto f : base_vstorage_->LevelFiles(-1)) {
      ReclaimSst(f, true);
    }
    for (int level = 0; level < num_levels_; level++) {
      const auto& cmp = (level == 0) ? level_zero_cmp_ : level_nonzero_cmp_;
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const auto& base_files = base_vstorage_->LevelFiles(level);
      const auto& unordered_added_files = levels_[level].added_files;
      vstorage->Reserve(level,
                        base_files.size() + unordered_added_files.size());

      // Sort added files for the level.
      std::vector<FileMetaData*> added_files;
      added_files.reserve(unordered_added_files.size());
      for (const auto& pair : unordered_added_files) {
        added_files.push_back(pair.second);
      }
      std::sort(added_files.begin(), added_files.end(), cmp);

#ifndef NDEBUG
      FileMetaData* prev_added_file = nullptr;
      for (const auto& added : added_files) {
        if (level > 0 && prev_added_file != nullptr) {
          assert(base_vstorage_->InternalComparator()->Compare(
                     prev_added_file->smallest, added->smallest) <= 0);
        }
        prev_added_file = added;
      }
#endif

      auto maybe_add_file = [&](FileMetaData* f, bool is_base_file) {
        auto find = delta_antiquation_.find(f->fd.GetNumber());
        if (find != delta_antiquation_.end()) {
          f->num_antiquation += find->second;
        }
        if (levels_[level].deleted_files.count(f->fd.GetNumber()) > 0) {
          assert(is_base_file);
          ReclaimSst(f, true);
        } else {
          if (is_base_file) {
            ActiveSst(f, level, true);
          } else {
            assert(active_sst_.count(f->fd.GetNumber()) > 0);
          }
          vstorage->AddFile(level, f, info_log_);
          if (level == 0) {
            read_amp[level] += f->prop.read_amp;
          } else {
            read_amp[level] = std::max<int>(read_amp[level], f->prop.read_amp);
          }
        }
      };

      auto base_iter = base_files.begin(), base_end = base_files.end();
      auto added_iter = added_files.begin(), added_end = added_files.end();
      while (added_iter != added_end || base_iter != base_end) {
        if (base_iter == base_end ||
            (added_iter != added_end && cmp(*added_iter, *base_iter))) {
          maybe_add_file(*added_iter++, false);
        } else {
          maybe_add_file(*base_iter++, true);
        }
      }
    }
    CaclDependence(true);
    // Handle actual deleted files
    for (auto f : base_vstorage_->LevelFiles(-1)) {
      if (dependence_map_.count(f->fd.GetNumber()) == 0) {
        // f is to-be-deleted table file
        vstorage->RemoveCurrentStats(f);
      }
    }
    for (auto& pair : dependence_map_) {
      auto& item = pair.second;
      if (item.dependence_version == dependence_version_ && item.level == -1) {
        auto find = delta_antiquation_.find(pair.first);
        if (find != delta_antiquation_.end()) {
          item.f->num_antiquation += find->second;
        }
        vstorage->AddFile(-1, item.f, info_log_);
      }
    }
    auto exists = [&](FileMetaData* file_metadata) {
      auto find = dependence_map_.find(file_metadata->fd.GetNumber());
      if (find == dependence_map_.end()) {
        return false;
      }
      auto& item = find->second;
      file_metadata->is_skip_gc |= item.skip_gc_version == dependence_version_;
      return item.dependence_version == dependence_version_;
    };
    vstorage->ShrinkDependenceMap(&exists, c_style_callback(exists));
    vstorage->set_read_amplification(read_amp);

    CheckConsistency(vstorage);
  }

  void LoadTableHandlers(InternalStats* internal_stats, int max_threads,
                         bool prefetch_index_and_filter_in_cache,
                         const SliceTransform* prefix_extractor) {
    assert(table_cache_ != nullptr);
    // <file metadata, level>
    std::vector<std::pair<FileMetaData*, int>> files_meta;
    for (int level = 0; level < num_levels_; level++) {
      for (auto& file_meta_pair : levels_[level].added_files) {
        auto* file_meta = file_meta_pair.second;
        assert(!file_meta->table_reader_handle);
        files_meta.emplace_back(file_meta, level);
      }
    }
    for (auto& pair : dependence_map_) {
      auto& item = pair.second;
      if (item.dependence_version == dependence_version_ && item.level == -1) {
        files_meta.emplace_back(item.f, -1);
      }
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
            level > 0 ? internal_stats->GetFileReadHist(level) : nullptr;
        table_cache_->FindTable(
            env_options_, *(base_vstorage_->InternalComparator()),
            file_meta->fd, &file_meta->table_reader_handle, prefix_extractor,
            false /*no_io */, true /* record_read_stats */,
            file_read_hist, false, level, prefetch_index_and_filter_in_cache);
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
};

VersionBuilder::VersionBuilder(const EnvOptions& env_options,
                               TableCache* table_cache,
                               VersionStorageInfo* base_vstorage,
                               Logger* info_log)
    : rep_(new Rep(env_options, info_log, table_cache, base_vstorage)) {}

VersionBuilder::~VersionBuilder() { delete rep_; }

void VersionBuilder::CheckConsistency(VersionStorageInfo* vstorage) {
  rep_->CheckConsistency(vstorage);
}

void VersionBuilder::CheckConsistencyForDeletes(VersionEdit* edit,
                                                uint64_t number, int level) {
  rep_->CheckConsistencyForDeletes(edit, number, level);
}

bool VersionBuilder::CheckConsistencyForNumLevels() {
  return rep_->CheckConsistencyForNumLevels();
}

void VersionBuilder::Apply(VersionEdit* edit) { rep_->Apply(edit); }

void VersionBuilder::SaveTo(VersionStorageInfo* vstorage) {
  rep_->SaveTo(vstorage);
}

void VersionBuilder::LoadTableHandlers(InternalStats* internal_stats,
                                       int max_threads,
                                       bool prefetch_index_and_filter_in_cache,
                                       const SliceTransform* prefix_extractor) {
  rep_->LoadTableHandlers(internal_stats, max_threads,
                          prefetch_index_and_filter_in_cache, prefix_extractor);
}

}  // namespace rocksdb
