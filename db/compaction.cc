//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include <queue>
#include <vector>

#include "db/column_family.h"
#include "db/version_set.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/terark_namespace.h"
#include "util/c_style_callback.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "utilities/util/factory.h"

namespace TERARKDB_NAMESPACE {

const uint64_t kRangeTombstoneSentinel =
    PackSequenceAndType(kMaxSequenceNumber, kTypeRangeDeletion);

int sstableKeyCompare(const Comparator* user_cmp, const InternalKey& a,
                      const InternalKey& b) {
  auto c = user_cmp->Compare(a.user_key(), b.user_key());
  if (c != 0) {
    return c;
  }
  auto a_footer = ExtractInternalKeyFooter(a.Encode());
  auto b_footer = ExtractInternalKeyFooter(b.Encode());
  if (a_footer == kRangeTombstoneSentinel) {
    if (b_footer != kRangeTombstoneSentinel) {
      return -1;
    }
  } else if (b_footer == kRangeTombstoneSentinel) {
    return 1;
  }
  return 0;
}

int sstableKeyCompare(const Comparator* user_cmp, const InternalKey* a,
                      const InternalKey& b) {
  if (a == nullptr) {
    return -1;
  }
  return sstableKeyCompare(user_cmp, *a, b);
}

int sstableKeyCompare(const Comparator* user_cmp, const InternalKey& a,
                      const InternalKey* b) {
  if (b == nullptr) {
    return -1;
  }
  return sstableKeyCompare(user_cmp, a, *b);
}

uint64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  uint64_t sum = 0;
  for (size_t i = 0; i < files.size() && files[i]; i++) {
    sum += files[i]->fd.GetFileSize();
  }
  return sum;
}

uint64_t TotalCompensatedSize(const std::vector<FileMetaData*>& files) {
  uint64_t sum = 0;
  for (size_t i = 0; i < files.size() && files[i]; i++) {
    sum += files[i]->compensated_file_size;
  }
  return sum;
}

const char* CompactionTypeName(CompactionType type) {
  switch (type) {
    case kKeyValueCompaction:
      return "Compaction";
    case kMapCompaction:
      return "Map Compaction";
    case kGarbageCollection:
      return "Garbage Collection";
    default:
      return "Unknow Compaction";
  }
}

Status BuildInheritanceTree(const std::vector<CompactionInputFiles>& inputs,
                            const DependenceMap& dependence_map,
                            const Version* version,
                            std::vector<uint64_t>* inheritance_tree,
                            size_t* pruge_count_ptr) {
  // Purging the leaf nodes recursively with inheritance tree encoded by DFS
  // sequence.
  //
  //  GC job:
  //    input sst = [10,9,11]
  //      sst 10 = [3,1,1,2,2,3,7,4,4,5,5,6,6,7]
  //      sst 9  = [8,8]
  //      sst 11 = []
  //    output sst = 12
  //    dependence_map = {5,6,8,9,10,11}
  //
  //        1  2   4 5 6
  //        \ /     \ /
  //         3      7          8
  //          \    /           |
  //          sst 10    +    sst 9   +   sst 11
  //
  //  After purge & merge, we got output inheritance tree
  //
  //                5   6
  //                 \ /
  //                  7   8
  //                  |   |
  //                 10   9  11
  //                  \  |  /
  //                  sst 12
  //
  //  sst 12 = [10,7,5,5,6,6,7,10,9,8,8,9,11,11]

  Status s;
  size_t pruge_count = 0;

  inheritance_tree->clear();

  for (auto& level : inputs) {
    for (auto f : level.files) {
      std::shared_ptr<const TableProperties> tp;
      s = version->GetTableProperties(&tp, f);
      if (!s.ok()) {
        return s;
      }
      inheritance_tree->emplace_back(f->fd.GetNumber());
      // verify input inheritance tree
      size_t size = inheritance_tree->size();
      for (auto fn : tp->inheritance_tree) {
        if (inheritance_tree->back() == fn) {
          inheritance_tree->pop_back();
          if (inheritance_tree->size() < size) {
            break;
          }
        } else {
          inheritance_tree->emplace_back(fn);
        }
      }
      if (inheritance_tree->size() != size) {
        // input inheritance tree invalid !
        return Status::Corruption(
            "BuildInheritanceTree: bad inheritance_tree, file_number ",
            ToString(f->fd.GetNumber()));
      }
      // purge unnecessary node & merge input inheritance tree
      for (auto fn : tp->inheritance_tree) {
        if (inheritance_tree->back() == fn && dependence_map.count(fn) == 0) {
          ++pruge_count;
          inheritance_tree->pop_back();
        } else {
          inheritance_tree->emplace_back(fn);
        }
      }
      inheritance_tree->emplace_back(f->fd.GetNumber());
    }
  }
  if (pruge_count_ptr != nullptr) {
    *pruge_count_ptr = pruge_count;
  }

  inheritance_tree->shrink_to_fit();
  return s;
}

std::vector<uint64_t> InheritanceTreeToSet(const std::vector<uint64_t>& tree) {
  std::vector<uint64_t> set = tree;
  std::sort(set.begin(), set.end());
  auto it = std::unique(set.begin(), set.end());
  assert(set.end() - it == it - set.begin());
  set.resize(it - set.begin());
  set.shrink_to_fit();
  return set;
}

void ProcessFileMetaData(const char* job_info, FileMetaData* meta,
                         const TableProperties* tp,
                         const ImmutableCFOptions* iopt,
                         const MutableCFOptions* mopt) {
  meta->prop.num_deletions = tp->num_deletions;
  meta->prop.raw_key_size = tp->raw_key_size;
  meta->prop.raw_value_size = tp->raw_value_size;
  meta->prop.flags |=
      tp->num_range_deletions > 0 ? 0 : TablePropertyCache::kNoRangeDeletions;
  meta->prop.flags |=
      tp->snapshots.empty() ? 0 : TablePropertyCache::kHasSnapshots;

  if (tp->num_range_deletions > 0 && mopt->optimize_range_deletion &&
      !iopt->enable_lazy_compaction) {
    meta->marked_for_compaction |= FileMetaData::kMarkedFromRangeDeletion;
  }
  if (iopt->ttl_extractor_factory != nullptr) {
    GetCompactionTimePoint(tp->user_collected_properties,
                           &meta->prop.earliest_time_begin_compact,
                           &meta->prop.latest_time_end_compact);
    ROCKS_LOG_INFO(iopt->info_log,
                   "%s earliest_time_begin_compact = %" PRIu64
                   ", latest_time_end_compact = %" PRIu64,
                   job_info, meta->prop.earliest_time_begin_compact,
                   meta->prop.latest_time_end_compact);
  }
}

void Compaction::SetInputVersion(Version* _input_version) {
  input_version_ = _input_version;
  cfd_ = input_version_->cfd();

  cfd_->Ref();
  input_version_->Ref();
  edit_.SetColumnFamily(cfd_->GetID());
}

void Compaction::GetBoundaryKeys(
    VersionStorageInfo* vstorage,
    const std::vector<CompactionInputFiles>& inputs, Slice* smallest_user_key,
    Slice* largest_user_key) {
  bool initialized = false;
  const Comparator* ucmp = vstorage->InternalComparator()->user_comparator();
  for (size_t i = 0; i < inputs.size(); ++i) {
    if (inputs[i].files.empty()) {
      continue;
    }
    if (inputs[i].level == 0) {
      // we need to consider all files on level 0
      for (const auto* f : inputs[i].files) {
        const Slice& start_user_key = f->smallest.user_key();
        if (!initialized ||
            ucmp->Compare(start_user_key, *smallest_user_key) < 0) {
          *smallest_user_key = start_user_key;
        }
        const Slice& end_user_key = f->largest.user_key();
        if (!initialized ||
            ucmp->Compare(end_user_key, *largest_user_key) > 0) {
          *largest_user_key = end_user_key;
        }
        initialized = true;
      }
    } else {
      // we only need to consider the first and last file
      const Slice& start_user_key = inputs[i].files[0]->smallest.user_key();
      if (!initialized ||
          ucmp->Compare(start_user_key, *smallest_user_key) < 0) {
        *smallest_user_key = start_user_key;
      }
      const Slice& end_user_key = inputs[i].files.back()->largest.user_key();
      if (!initialized || ucmp->Compare(end_user_key, *largest_user_key) > 0) {
        *largest_user_key = end_user_key;
      }
      initialized = true;
    }
  }
}

// helper function to determine if compaction is creating files at the
// bottommost level
bool Compaction::IsBottommostLevel(
    int output_level, VersionStorageInfo* vstorage,
    const std::vector<CompactionInputFiles>& inputs) {
  int output_l0_idx;
  if (output_level == 0) {
    output_l0_idx = 0;
    for (const auto* file : vstorage->LevelFiles(0)) {
      if (inputs[0].files.back() == file) {
        break;
      }
      ++output_l0_idx;
    }
    assert(static_cast<size_t>(output_l0_idx) < vstorage->LevelFiles(0).size());
  } else {
    output_l0_idx = -1;
  }
  Slice smallest_key, largest_key;
  GetBoundaryKeys(vstorage, inputs, &smallest_key, &largest_key);
  return !vstorage->RangeMightExistAfterSortedRun(smallest_key, largest_key,
                                                  output_level, output_l0_idx);
}

// test function to validate the functionality of IsBottommostLevel()
// function -- determines if compaction with inputs and storage is bottommost
bool Compaction::TEST_IsBottommostLevel(
    int output_level, VersionStorageInfo* vstorage,
    const std::vector<CompactionInputFiles>& inputs) {
  return IsBottommostLevel(output_level, vstorage, inputs);
}

bool Compaction::IsFullCompaction(
    VersionStorageInfo* vstorage,
    const std::vector<CompactionInputFiles>& inputs) {
  size_t num_files_in_compaction = 0;
  size_t total_num_files = 0;
  for (int l = 0; l < vstorage->num_levels(); l++) {
    total_num_files += vstorage->NumLevelFiles(l);
  }
  for (size_t i = 0; i < inputs.size(); i++) {
    num_files_in_compaction += inputs[i].size();
  }
  return num_files_in_compaction == total_num_files;
}

Compaction::Compaction(CompactionParams&& params)
    : input_vstorage_(params.input_version),
      start_level_(params.inputs[0].level),
      output_level_(params.output_level),
      num_antiquation_(params.num_antiquation),
      max_output_file_size_(params.target_file_size),
      max_compaction_bytes_(params.max_compaction_bytes),
      max_subcompactions_(params.max_subcompactions),
      immutable_cf_options_(params.immutable_cf_options),
      mutable_cf_options_(params.mutable_cf_options),
      input_version_(nullptr),
      number_levels_(params.input_version->num_levels()),
      cfd_(nullptr),
      output_path_id_(params.output_path_id),
      output_compression_(params.compression),
      output_compression_opts_(params.compression_opts),
      partial_compaction_(params.partial_compaction),
      compaction_type_(params.compaction_type),
      separation_type_(params.separation_type),
      input_range_(std::move(params.input_range)),
      inputs_(std::move(params.inputs)),
      grandparents_(std::move(params.grandparents)),
      score_(params.score),
      compaction_load_(0),
      bottommost_level_(
          IsBottommostLevel(output_level_, params.input_version, inputs_)),
      is_full_compaction_(IsFullCompaction(params.input_version, inputs_)),
      is_manual_compaction_(params.manual_compaction),
      is_trivial_move_(false),
      compaction_reason_(params.compaction_reason) {
  MarkFilesBeingCompacted(true);
  if (is_manual_compaction_) {
    compaction_reason_ = CompactionReason::kManualCompaction;
  }
  TEST_SYNC_POINT_CALLBACK("Compaction::Compaction::Start", this);
  if (max_subcompactions_ == 0) {
    max_subcompactions_ = mutable_cf_options_.max_subcompactions;
  }

#ifndef NDEBUG
  for (size_t i = 1; i < inputs_.size(); ++i) {
    assert(inputs_[i].level > inputs_[i - 1].level);
  }
#endif

  // setup input_levels_
  {
    input_levels_.resize(num_input_levels());
    for (size_t which = 0; which < num_input_levels(); which++) {
      DoGenerateLevelFilesBrief(&input_levels_[which], inputs_[which].files,
                                &arena_);
    }
  }

  GetBoundaryKeys(params.input_version, inputs_, &smallest_user_key_,
                  &largest_user_key_);
}

Compaction::~Compaction() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
  }
  if (cfd_ != nullptr) {
    if (cfd_->Unref()) {
      delete cfd_;
    }
  }
}

bool Compaction::InputCompressionMatchesOutput() const {
  int base_level = input_vstorage_->base_level();
  bool matches = (GetCompressionType(immutable_cf_options_, input_vstorage_,
                                     mutable_cf_options_, start_level_,
                                     base_level) == output_compression_);
  if (matches) {
    TEST_SYNC_POINT("Compaction::InputCompressionMatchesOutput:Matches");
    return true;
  }
  TEST_SYNC_POINT("Compaction::InputCompressionMatchesOutput:DidntMatch");
  return matches;
}

bool Compaction::IsTrivialMove() const {
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  // If start_level_== output_level_, the purpose is to force compaction
  // filter to be applied to that level, and thus cannot be a trivial move.

  // TrivialMoveLevel
  if (compaction_reason_ == CompactionReason::kTrivialMoveLevel) {
    assert(is_trivial_move_);
    return true;
  }

  // Check if start level have files with overlapping ranges
  if (start_level_ == 0 && input_vstorage_->level0_non_overlapping() == false) {
    // We cannot move files from L0 to L1 if the files are overlapping
    return false;
  }

  if (is_manual_compaction_ &&
      (immutable_cf_options_.compaction_filter != nullptr ||
       immutable_cf_options_.compaction_filter_factory != nullptr)) {
    // This is a manual compaction and we have a compaction filter that should
    // be executed, we cannot do a trivial move
    return false;
  }

  // Used in universal compaction, where trivial move can be done if the
  // input files are non overlapping
  if ((mutable_cf_options_.compaction_options_universal.allow_trivial_move) &&
      (output_level_ != 0)) {
    return is_trivial_move_;
  }

  if (!(start_level_ != output_level_ && num_input_levels() == 1 &&
        input(0, 0)->fd.GetPathId() == output_path_id() &&
        InputCompressionMatchesOutput())) {
    return false;
  }

  // assert inputs_.size() == 1

  for (const auto& file : inputs_.front().files) {
    std::vector<FileMetaData*> file_grand_parents;
    if (output_level_ + 1 >= number_levels_) {
      continue;
    }
    input_vstorage_->GetOverlappingInputs(output_level_ + 1, &file->smallest,
                                          &file->largest, &file_grand_parents);
    const auto compaction_size =
        file->fd.GetFileSize() + TotalFileSize(file_grand_parents);
    if (compaction_size > max_compaction_bytes_) {
      return false;
    }
  }

  return true;
}

void Compaction::AddInputDeletions(VersionEdit* out_edit) {
  for (size_t which = 0; which < num_input_levels(); which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      out_edit->DeleteFile(level(which), inputs_[which][i]->fd.GetNumber());
    }
  }
}

bool Compaction::KeyNotExistsBeyondOutputLevel(
    const Slice& user_key, std::vector<size_t>* level_ptrs) const {
  assert(input_version_ != nullptr);
  assert(level_ptrs != nullptr);
  assert(level_ptrs->size() == static_cast<size_t>(number_levels_));
  if (bottommost_level_) {
    return true;
  } else if (output_level_ != 0 &&
             cfd_->ioptions()->compaction_style == kCompactionStyleLevel) {
    // Maybe use binary search to find right entry instead of linear search?
    const Comparator* user_cmp = cfd_->user_comparator();
    for (int lvl = output_level_ + 1; lvl < number_levels_; lvl++) {
      const std::vector<FileMetaData*>& files =
          input_vstorage_->LevelFiles(lvl);
      for (; level_ptrs->at(lvl) < files.size(); level_ptrs->at(lvl)++) {
        auto* f = files[level_ptrs->at(lvl)];
        if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
          // We've advanced far enough
          if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
            // Key falls in this file's range, so definitely
            // exists beyond output level
            return false;
          }
          break;
        }
      }
    }
    return true;
  }
  return false;
}

// Mark (or clear) each file that is being compacted
void Compaction::MarkFilesBeingCompacted(bool mark_as_compacted) {
  for (size_t i = 0; i < num_input_levels(); i++) {
    for (size_t j = 0; j < inputs_[i].size(); j++) {
      assert(mark_as_compacted ? !inputs_[i][j]->being_compacted
                               : inputs_[i][j]->being_compacted);
      inputs_[i][j]->being_compacted = mark_as_compacted;
    }
  }
}

// Sample output:
// If compacting 3 L0 files, 2 L3 files and 1 L4 file, and outputting to L5,
// print: "3@0 + 2@3 + 1@4 files to L5"
const char* Compaction::InputLevelSummary(
    InputLevelSummaryBuffer* scratch) const {
  int len = 0;
  bool is_first = true;
  for (auto& input_level : inputs_) {
    if (input_level.empty()) {
      continue;
    }
    if (!is_first) {
      len +=
          snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len, " + ");
      len = std::min(len, static_cast<int>(sizeof(scratch->buffer)));
    } else {
      is_first = false;
    }
    len += snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len,
                    "%" ROCKSDB_PRIszt "@%d", input_level.size(),
                    input_level.level);
    len = std::min(len, static_cast<int>(sizeof(scratch->buffer)));
  }
  snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len,
           " files to L%d", output_level());

  return scratch->buffer;
}

uint64_t Compaction::CalculateTotalInputSize() const {
  uint64_t size = 0;
  for (auto& input_level : inputs_) {
    for (auto f : input_level.files) {
      size += f->fd.GetFileSize();
    }
  }
  return size;
}

void Compaction::ReleaseCompactionFiles(Status status) {
  MarkFilesBeingCompacted(false);
  cfd_->compaction_picker()->ReleaseCompactionFiles(this, status);
}

void Compaction::ResetNextCompactionIndex() {
  assert(input_version_ != nullptr);
  if (start_level_ >= 0) {
    input_vstorage_->ResetNextCompactionIndex(start_level_);
  }
}

namespace {
int InputSummary(const std::vector<FileMetaData*>& files, char* output,
                 int len) {
  *output = '\0';
  int write = 0;
  for (size_t i = 0; i < files.size(); i++) {
    int sz = len - write;
    int ret;
    char sztxt[16];
    AppendHumanBytes(files.at(i)->fd.GetFileSize(), sztxt, 16);
    ret = snprintf(output + write, sz, "%" PRIu64 "(%s) ",
                   files.at(i)->fd.GetNumber(), sztxt);
    if (ret < 0 || ret >= sz) break;
    write += ret;
  }
  // if files.size() is non-zero, overwrite the last space
  return write - !!files.size();
}
}  // namespace

void Compaction::Summary(char* output, int len) {
  int write =
      snprintf(output, len, "Base version %" PRIu64 " Base level %d, inputs: [",
               input_version_->GetVersionNumber(), start_level_);
  if (write < 0 || write >= len) {
    return;
  }

  for (size_t level_iter = 0; level_iter < num_input_levels(); ++level_iter) {
    if (level_iter > 0) {
      write += snprintf(output + write, len - write, "], [");
      if (write < 0 || write >= len) {
        return;
      }
    }
    write +=
        InputSummary(inputs_[level_iter].files, output + write, len - write);
    if (write < 0 || write >= len) {
      return;
    }
  }

  snprintf(output + write, len - write, "]");
}

uint64_t Compaction::OutputFilePreallocationSize() const {
  uint64_t preallocation_size = 0;

  for (const auto& level_files : inputs_) {
    for (const auto& file : level_files.files) {
      preallocation_size += file->fd.GetFileSize();
    }
  }

  if (max_output_file_size_ != port::kMaxUint64 &&
      (immutable_cf_options_.compaction_style == kCompactionStyleLevel ||
       output_level() > 0)) {
    preallocation_size = std::min(max_output_file_size_, preallocation_size);
  }

  // Over-estimate slightly so we don't end up just barely crossing
  // the threshold
  // No point to prellocate more than 1GB.
  return std::min(uint64_t{1073741824},
                  preallocation_size + (preallocation_size / 10));
}

std::unique_ptr<CompactionFilter> Compaction::CreateCompactionFilter() const {
  if (!cfd_->ioptions()->compaction_filter_factory) {
    return nullptr;
  }

  CompactionFilter::Context context;
  context.is_full_compaction = is_full_compaction_;
  context.is_manual_compaction = is_manual_compaction_;
  context.is_bottommost_level = bottommost_level_;
  context.column_family_id = cfd_->GetID();
  context.smallest_user_key = smallest_user_key_;
  context.largest_user_key = largest_user_key_;
  return cfd_->ioptions()->compaction_filter_factory->CreateCompactionFilter(
      context);
}

bool Compaction::IsOutputLevelEmpty() const {
  return inputs_.back().level != output_level_ || inputs_.back().empty();
}

bool Compaction::ShouldFormSubcompactions() const {
  if (compaction_type_ == kMapCompaction || max_subcompactions_ <= 1 ||
      cfd_ == nullptr) {
    return false;
  }
  if (cfd_->ioptions()->compaction_style == kCompactionStyleLevel) {
    return (start_level_ == 0 || is_manual_compaction_) && output_level_ > 0 &&
           !IsOutputLevelEmpty();
  } else if (cfd_->ioptions()->compaction_style == kCompactionStyleUniversal) {
    return number_levels_ > 1 && output_level_ > 0;
  } else {
    return false;
  }
}

uint64_t Compaction::MaxInputFileCreationTime() const {
  uint64_t max_creation_time = 0;
  for (const auto& file : inputs_[0].files) {
    if (file->fd.table_reader != nullptr &&
        file->fd.table_reader->GetTableProperties() != nullptr) {
      uint64_t creation_time =
          file->fd.table_reader->GetTableProperties()->creation_time;
      max_creation_time = std::max(max_creation_time, creation_time);
    }
  }
  return max_creation_time;
}
std::unordered_map<uint64_t, uint64_t>&
Compaction::current_blob_overlap_scores() const {
  return input_vstorage_->blob_overlap_scores();
}

int Compaction::GetInputBaseLevel() const {
  return input_vstorage_->base_level();
}

}  // namespace TERARKDB_NAMESPACE

using namespace TERARKDB_NAMESPACE;
TERARK_FACTORY_INSTANTIATE_GNS(CompactionFilter*, Slice,
                               CompactionFilterContext);
TERARK_FACTORY_INSTANTIATE_GNS(CompactionFilterFactory*, Slice);
