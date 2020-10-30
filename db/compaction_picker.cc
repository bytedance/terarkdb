//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction_picker.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include <limits>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "db/column_family.h"
#include "db/map_builder.h"
#include "monitoring/statistics.h"
#include "util/c_style_callback.h"
#include "util/filename.h"
#include "util/log_buffer.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "utilities/util/function.hpp"

namespace rocksdb {

namespace {

inline double MixOverlapRatioAndDeletionRatio(double overlap_ratio,
                                              double deletion_ratio) {
  return overlap_ratio + deletion_ratio * 64;
}

struct LevelMapRangeSrc {
  LevelMapRangeSrc(const MapSstElement& e, double _estimate_entry_num,
                   double _estimate_del_num, Arena* arena)
      : start(ArenaPinSlice(e.smallest_key, arena)),
        limit(ArenaPinSlice(e.largest_key, arena)),
        estimate_size(e.EstimateSize()),
        estimate_entry_num(_estimate_entry_num),
        estimate_del_num(_estimate_del_num) {}

  Slice start, limit;
  uint64_t estimate_size;
  double estimate_entry_num;
  double estimate_del_num;
  size_t start_index, limit_index;
};
struct LevelMapRangeDst {
  LevelMapRangeDst(const MapSstElement& e, Arena* arena)
      : start(ArenaPinSlice(e.smallest_key, arena)),
        limit(ArenaPinSlice(e.largest_key, arena)),
        estimate_size(e.EstimateSize()),
        accumulate_estimate_size(0) {}

  Slice start, limit;
  uint64_t estimate_size;
  uint64_t accumulate_estimate_size;
};
struct LevelMapSection {
  ptrdiff_t start_index, limit_index;
  double weight;
};
struct GarbageFileInfo {
  FileMetaData* f;
  double score;
  uint64_t estimate_size;
};
struct FileUseInfo {
  uint64_t size;
  uint64_t used;
};
struct PickerCompositeHeapItem {
  Slice k;
  double s;
  operator double() const noexcept { return s; }
};

uint64_t TotalCompensatedFileSize(const std::vector<FileMetaData*>& files) {
  uint64_t sum = 0;
  for (size_t i = 0; i < files.size() && files[i]; i++) {
    sum += files[i]->compensated_file_size;
  }
  return sum;
}
/*
 * Find the optimal path to place a file
 * Given a level, finds the path where levels up to it will fit in levels
 * up to and including this path
 */
uint32_t GetPathId(const ImmutableCFOptions& ioptions,
                   const MutableCFOptions& mutable_cf_options, int level) {
  uint32_t p = 0;
  assert(!ioptions.cf_paths.empty());

  // size remaining in the most recent path
  uint64_t current_path_size = ioptions.cf_paths[0].target_size;

  uint64_t level_size;
  int cur_level = 0;

  // max_bytes_for_level_base denotes L1 size.
  // We estimate L0 size to be the same as L1.
  level_size = mutable_cf_options.max_bytes_for_level_base;

  // Last path is the fallback
  while (p < ioptions.cf_paths.size() - 1) {
    if (level_size <= current_path_size) {
      if (cur_level == level) {
        // Does desired level fit in this path?
        return p;
      } else {
        current_path_size -= level_size;
        if (cur_level > 0) {
          if (ioptions.level_compaction_dynamic_level_bytes) {
            // Currently, level_compaction_dynamic_level_bytes is ignored when
            // multiple db paths are specified. https://github.com/facebook/
            // rocksdb/blob/master/db/column_family.cc.
            // Still, adding this check to avoid accidentally using
            // max_bytes_for_level_multiplier_additional
            level_size = static_cast<uint64_t>(
                level_size * mutable_cf_options.max_bytes_for_level_multiplier);
          } else {
            level_size = static_cast<uint64_t>(
                level_size * mutable_cf_options.max_bytes_for_level_multiplier *
                mutable_cf_options.MaxBytesMultiplerAdditional(cur_level));
          }
        }
        cur_level++;
        continue;
      }
    }
    p++;
    current_path_size = ioptions.cf_paths[p].target_size;
  }
  return p;
}

void AssignUserKey(std::string& key, const Slice& ikey) {
  Slice ukey = ExtractUserKey(ikey);
  key.assign(ukey.data(), ukey.size());
};
}  // anonymous namespace

bool FindIntraL0Compaction(const std::vector<FileMetaData*>& level_files,
                           size_t min_files_to_compact,
                           uint64_t max_compact_bytes_per_del_file,
                           CompactionInputFiles* comp_inputs) {
  size_t compact_bytes = static_cast<size_t>(level_files[0]->fd.file_size);
  size_t compact_bytes_per_del_file = port::kMaxSizet;
  // compaction range will be [0, span_len).
  size_t span_len;
  // pull in files until the amount of compaction work per deleted file begins
  // increasing.
  size_t new_compact_bytes_per_del_file = 0;
  for (span_len = 1; span_len < level_files.size(); ++span_len) {
    compact_bytes += static_cast<size_t>(level_files[span_len]->fd.file_size);
    new_compact_bytes_per_del_file = compact_bytes / span_len;
    if (level_files[span_len]->being_compacted ||
        new_compact_bytes_per_del_file > compact_bytes_per_del_file) {
      break;
    }
    compact_bytes_per_del_file = new_compact_bytes_per_del_file;
  }

  if (span_len >= min_files_to_compact &&
      compact_bytes_per_del_file < max_compact_bytes_per_del_file) {
    assert(comp_inputs != nullptr);
    comp_inputs->level = 0;
    for (size_t i = 0; i < span_len; ++i) {
      comp_inputs->files.push_back(level_files[i]);
    }
    return true;
  }
  return false;
}

// Determine compression type, based on user options, level of the output
// file and whether compression is disabled.
// If enable_compression is false, then compression is always disabled no
// matter what the values of the other two parameters are.
// Otherwise, the compression type is determined based on options and level.
CompressionType GetCompressionType(const ImmutableCFOptions& ioptions,
                                   const VersionStorageInfo* vstorage,
                                   const MutableCFOptions& mutable_cf_options,
                                   int level, int base_level,
                                   const bool enable_compression) {
  if (!enable_compression) {
    // disable compression
    return kNoCompression;
  }

  // If bottommost_compression is set and we are compacting to the
  // bottommost level then we should use it.
  if (ioptions.bottommost_compression != kDisableCompressionOption &&
      level >= (vstorage->num_non_empty_levels() - 1)) {
    return ioptions.bottommost_compression;
  }
  // If the user has specified a different compression level for each level,
  // then pick the compression for that level.
  if (!ioptions.compression_per_level.empty()) {
    assert(level == 0 || level >= base_level);
    int idx = (level == 0) ? 0 : level - base_level + 1;

    const int n = static_cast<int>(ioptions.compression_per_level.size()) - 1;
    // It is possible for level_ to be -1; in that case, we use level
    // 0's compression.  This occurs mostly in backwards compatibility
    // situations when the builder doesn't know what level the file
    // belongs to.  Likewise, if level is beyond the end of the
    // specified compression levels, use the last value.
    return ioptions.compression_per_level[std::max(0, std::min(idx, n))];
  } else {
    return mutable_cf_options.compression;
  }
}

CompressionOptions GetCompressionOptions(const ImmutableCFOptions& ioptions,
                                         const VersionStorageInfo* vstorage,
                                         int level,
                                         const bool enable_compression) {
  if (!enable_compression) {
    return ioptions.compression_opts;
  }
  // If bottommost_compression is set and we are compacting to the
  // bottommost level then we should use the specified compression options
  // for the bottmomost_compression.
  if (ioptions.bottommost_compression != kDisableCompressionOption &&
      level >= (vstorage->num_non_empty_levels() - 1) &&
      ioptions.bottommost_compression_opts.enabled) {
    return ioptions.bottommost_compression_opts;
  }
  return ioptions.compression_opts;
}

CompactionPicker::CompactionPicker(TableCache* table_cache,
                                   const EnvOptions& env_options,
                                   const ImmutableCFOptions& ioptions,
                                   const InternalKeyComparator* icmp)
    : table_cache_(table_cache),
      env_options_(env_options),
      ioptions_(ioptions),
      icmp_(icmp) {}

CompactionPicker::~CompactionPicker() {}

double CompactionPicker::GetQ(std::vector<double>::const_iterator b,
                              std::vector<double>::const_iterator e, size_t g) {
  double S = std::accumulate(b, e, 0.0);
  // sum of [q, q^2, q^3, ... , q^n]
  auto F = [](double q, size_t n) {
    return (std::pow(q, n + 1) - q) / (q - 1);
  };
  // let S = ∑q^i, i in <1..n>, seek q
  double q = std::pow(S, 1.0 / g);
  if (S <= g + 1) {
    q = 1;
  } else {
    // Newton-Raphson method
    for (size_t c = 0; c < 8; ++c) {
      double Fp = q, q_k = q;
      for (size_t k = 2; k <= g; ++k) {
        Fp += k * (q_k *= q);
      }
      q -= (F(q, g) - S) / Fp;
    }
  }
  return q;
}

bool CompactionPicker::ReadMapElement(MapSstElement& map_element,
                                      InternalIterator* iter,
                                      LogBuffer* log_buffer,
                                      const std::string& cf_name) {
  LazyBuffer value = iter->value();
  auto s = value.fetch();
  if (!s.ok()) {
    ROCKS_LOG_BUFFER(log_buffer,
                     "[%s] CompactionPicker LazyBuffer decode fail: %s\n",
                     cf_name.c_str(), s.ToString().c_str());
    return false;
  }
  if (!map_element.Decode(iter->key(), value.slice())) {
    ROCKS_LOG_BUFFER(log_buffer,
                     "[%s] CompactionPicker MapSstElement Decode fail\n",
                     cf_name.c_str());
    return false;
  }
  return true;
}

bool CompactionPicker::FixInputRange(std::vector<SelectedRange>& input_range,
                                     const InternalKeyComparator& icmp,
                                     bool sort, bool merge) {
  auto uc = icmp.user_comparator();
  auto range_cmp = [uc](const SelectedRange& a, const SelectedRange& b) {
    int r = uc->Compare(a.limit, b.limit);
    if (r == 0) {
      r = int(a.include_limit) - int(b.include_limit);
    }
    if (r == 0) {
      r = uc->Compare(a.start, b.start);
    }
    if (r == 0) {
      r = int(b.include_start) - int(a.include_start);
    }
    return r < 0;
  };
  if (sort) {
    std::sort(input_range.begin(), input_range.end(), range_cmp);
  } else {
    assert(std::is_sorted(input_range.begin(), input_range.end(), range_cmp));
  }
  if (merge && input_range.size() > 1) {
    size_t c = 0, n = input_range.size();
    auto it = input_range.begin();
    for (size_t i = 1; i < n; ++i) {
      if (uc->Compare(it[c].limit, it[i].start) >= 0) {
        if (uc->Compare(it[c].limit, it[i].limit) < 0) {
          it[c].limit = std::move(it[i].limit);
          it[c].include_limit = it[i].include_limit;
        }
      } else if (++c != i) {
        it[c] = std::move(it[i]);
      }
    }
    input_range.resize(c + 1);
  }
  // remove empty ranges
  if (input_range.size() > 1) {
    for (auto it = input_range.begin() + 1; it != input_range.end();) {
      if (uc->Compare(it->start, it[-1].start) == 0 ||
          uc->Compare(it->limit, it[-1].limit) == 0) {
        it[-1].limit = std::move(it->limit);
        it[-1].include_limit = it->include_limit;
        it = input_range.erase(it);
      } else {
        ++it;
      }
    }
  }
  if (!input_range.empty()) {
    for (auto it = input_range.begin(); it != input_range.end();) {
      if (uc->Compare(it->start, it->limit) == 0 &&
          (!it->include_start || !it->include_limit)) {
        it = input_range.erase(it);
      } else {
        ++it;
      }
    }
  }
  assert(std::is_sorted(input_range.begin(), input_range.end(),
                        TERARK_FIELD(start) < *uc));
  assert(std::is_sorted(input_range.begin(), input_range.end(),
                        TERARK_FIELD(limit) < *uc));
  assert(std::find_if(input_range.begin(), input_range.end(),
                      [uc](const RangeStorage& r) {
           return uc->Compare(r.start, r.limit) > 0;
         }) == input_range.end());
  return !input_range.empty();
}

// Delete this compaction from the list of running compactions.
void CompactionPicker::ReleaseCompactionFiles(Compaction* c, Status status) {
  UnregisterCompaction(c);
  if (!status.ok()) {
    c->ResetNextCompactionIndex();
  }
}

void CompactionPicker::GetRange(const CompactionInputFiles& inputs,
                                InternalKey* smallest,
                                InternalKey* largest) const {
  const int level = inputs.level;
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();

  if (level == 0) {
    for (size_t i = 0; i < inputs.size(); i++) {
      FileMetaData* f = inputs[i];
      if (i == 0) {
        *smallest = f->smallest;
        *largest = f->largest;
      } else {
        if (icmp_->Compare(f->smallest, *smallest) < 0) {
          *smallest = f->smallest;
        }
        if (icmp_->Compare(f->largest, *largest) > 0) {
          *largest = f->largest;
        }
      }
    }
  } else {
    *smallest = inputs[0]->smallest;
    *largest = inputs[inputs.size() - 1]->largest;
  }
}

void CompactionPicker::GetRange(const CompactionInputFiles& inputs1,
                                const CompactionInputFiles& inputs2,
                                InternalKey* smallest,
                                InternalKey* largest) const {
  assert(!inputs1.empty() || !inputs2.empty());
  if (inputs1.empty()) {
    GetRange(inputs2, smallest, largest);
  } else if (inputs2.empty()) {
    GetRange(inputs1, smallest, largest);
  } else {
    InternalKey smallest1, smallest2, largest1, largest2;
    GetRange(inputs1, &smallest1, &largest1);
    GetRange(inputs2, &smallest2, &largest2);
    *smallest =
        icmp_->Compare(smallest1, smallest2) < 0 ? smallest1 : smallest2;
    *largest = icmp_->Compare(largest1, largest2) < 0 ? largest2 : largest1;
  }
}

void CompactionPicker::GetRange(const std::vector<CompactionInputFiles>& inputs,
                                InternalKey* smallest,
                                InternalKey* largest) const {
  InternalKey current_smallest;
  InternalKey current_largest;
  bool initialized = false;
  for (const auto& in : inputs) {
    if (in.empty()) {
      continue;
    }
    GetRange(in, &current_smallest, &current_largest);
    if (!initialized) {
      *smallest = current_smallest;
      *largest = current_largest;
      initialized = true;
    } else {
      if (icmp_->Compare(current_smallest, *smallest) < 0) {
        *smallest = current_smallest;
      }
      if (icmp_->Compare(current_largest, *largest) > 0) {
        *largest = current_largest;
      }
    }
  }
  assert(initialized);
}

bool CompactionPicker::ExpandInputsToCleanCut(const std::string& /*cf_name*/,
                                              VersionStorageInfo* vstorage,
                                              CompactionInputFiles* inputs,
                                              InternalKey** next_smallest) {
  // This isn't good compaction
  assert(!inputs->empty());

  const int level = inputs->level;
  // GetOverlappingInputs will always do the right thing for level-0.
  // So we don't need to do any expansion if level == 0.
  if (level == 0) {
    return true;
  }

  InternalKey smallest, largest;

  // Keep expanding inputs until we are sure that there is a "clean cut"
  // boundary between the files in input and the surrounding files.
  // This will ensure that no parts of a key are lost during compaction.
  int hint_index = -1;
  size_t old_size;
  do {
    old_size = inputs->size();
    GetRange(*inputs, &smallest, &largest);
    inputs->clear();
    vstorage->GetOverlappingInputs(level, &smallest, &largest, &inputs->files,
                                   hint_index, &hint_index, true,
                                   next_smallest);
  } while (inputs->size() > old_size);

  // we started off with inputs non-empty and the previous loop only grew
  // inputs. thus, inputs should be non-empty here
  assert(!inputs->empty());

  // If, after the expansion, there are files that are already under
  // compaction, then we must drop/cancel this compaction.
  if (AreFilesInCompaction(inputs->files)) {
    return false;
  }
  return true;
}

bool CompactionPicker::RangeOverlapWithCompaction(
    const Slice& smallest_user_key, const Slice& largest_user_key,
    int level) const {
  const Comparator* ucmp = icmp_->user_comparator();
  for (Compaction* c : compactions_in_progress_) {
    if (c->output_level() == level &&
        ucmp->Compare(smallest_user_key, c->GetLargestUserKey()) <= 0 &&
        ucmp->Compare(largest_user_key, c->GetSmallestUserKey()) >= 0) {
      // Overlap
      return true;
    }
  }
  // Did not overlap with any running compaction in level `level`
  return false;
}

bool CompactionPicker::FilesRangeOverlapWithCompaction(
    const std::vector<CompactionInputFiles>& inputs, int level) const {
  bool is_empty = true;
  for (auto& in : inputs) {
    if (!in.empty()) {
      is_empty = false;
      break;
    }
  }
  if (is_empty) {
    // No files in inputs
    return false;
  }

  InternalKey smallest, largest;
  GetRange(inputs, &smallest, &largest);
  return RangeOverlapWithCompaction(smallest.user_key(), largest.user_key(),
                                    level);
}

// Returns true if any one of specified files are being compacted
bool CompactionPicker::AreFilesInCompaction(
    const std::vector<FileMetaData*>& files) {
  for (size_t i = 0; i < files.size(); i++) {
    if (files[i]->being_compacted) {
      return true;
    }
  }
  return false;
}

Compaction* CompactionPicker::CompactFiles(
    const CompactionOptions& compact_options,
    const std::vector<CompactionInputFiles>& input_files, int output_level,
    VersionStorageInfo* vstorage, const MutableCFOptions& mutable_cf_options,
    uint32_t output_path_id) {
  assert(input_files.size());
  // This compaction output should not overlap with a running compaction as
  // `SanitizeCompactionInputFiles` should've checked earlier and db mutex
  // shouldn't have been released since.
  assert(!FilesRangeOverlapWithCompaction(input_files, output_level));

  CompressionType compression_type;
  if (compact_options.compression == kDisableCompressionOption) {
    int base_level;
    if (ioptions_.compaction_style == kCompactionStyleLevel) {
      base_level = vstorage->base_level();
    } else {
      base_level = 1;
    }
    compression_type = GetCompressionType(
        ioptions_, vstorage, mutable_cf_options, output_level, base_level);
  } else {
    // TODO(ajkr): `CompactionOptions` offers configurable `CompressionType`
    // without configurable `CompressionOptions`, which is inconsistent.
    compression_type = compact_options.compression;
  }
  CompactionParams params(vstorage, ioptions_, mutable_cf_options);
  params.inputs = std::move(input_files);
  params.output_level = output_level;
  params.target_file_size =
      output_level == 0 ? 0 : compact_options.output_file_size_limit;
  params.max_compaction_bytes = mutable_cf_options.max_compaction_bytes;
  params.output_path_id = output_path_id;
  params.compression = compression_type;
  params.compression_opts =
      GetCompressionOptions(ioptions_, vstorage, output_level);

  params.max_subcompactions = compact_options.max_subcompactions;
  params.manual_compaction = true;

  return RegisterCompaction(new Compaction(std::move(params)));
}

Status CompactionPicker::GetCompactionInputsFromFileNumbers(
    std::vector<CompactionInputFiles>* input_files,
    std::unordered_set<uint64_t>* input_set, const VersionStorageInfo* vstorage,
    const CompactionOptions& /*compact_options*/) const {
  if (input_set->size() == 0U) {
    return Status::InvalidArgument(
        "Compaction must include at least one file.");
  }
  assert(input_files);

  std::vector<CompactionInputFiles> matched_input_files;
  matched_input_files.resize(vstorage->num_levels());
  int first_non_empty_level = -1;
  int last_non_empty_level = -1;
  // TODO(yhchiang): use a lazy-initialized mapping from
  //                 file_number to FileMetaData in Version.
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    for (auto file : vstorage->LevelFiles(level)) {
      auto iter = input_set->find(file->fd.GetNumber());
      if (iter != input_set->end()) {
        matched_input_files[level].files.push_back(file);
        input_set->erase(iter);
        last_non_empty_level = level;
        if (first_non_empty_level == -1) {
          first_non_empty_level = level;
        }
      }
    }
  }

  if (!input_set->empty()) {
    std::string message(
        "Cannot find matched SST files for the following file numbers:");
    for (auto fn : *input_set) {
      message += " ";
      message += ToString(fn);
    }
    return Status::InvalidArgument(message);
  }

  for (int level = first_non_empty_level; level <= last_non_empty_level;
       ++level) {
    matched_input_files[level].level = level;
    input_files->emplace_back(std::move(matched_input_files[level]));
  }

  return Status::OK();
}

// Returns true if any one of the parent files are being compacted
bool CompactionPicker::IsRangeInCompaction(VersionStorageInfo* vstorage,
                                           const InternalKey* smallest,
                                           const InternalKey* largest,
                                           int level, int* level_index) {
  std::vector<FileMetaData*> inputs;
  assert(level < NumberLevels());

  vstorage->GetOverlappingInputs(level, smallest, largest, &inputs,
                                 level_index ? *level_index : 0, level_index);
  return AreFilesInCompaction(inputs);
}

// Populates the set of inputs of all other levels that overlap with the
// start level.
// Now we assume all levels except start level and output level are empty.
// Will also attempt to expand "start level" if that doesn't expand
// "output level" or cause "level" to include a file for compaction that has an
// overlapping user-key with another file.
// REQUIRES: input_level and output_level are different
// REQUIRES: inputs->empty() == false
// Returns false if files on parent level are currently in compaction, which
// means that we can't compact them
bool CompactionPicker::SetupOtherInputs(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, CompactionInputFiles* inputs,
    CompactionInputFiles* output_level_inputs, int* parent_index,
    int base_index) {
  assert(!inputs->empty());
  assert(output_level_inputs->empty());
  const int input_level = inputs->level;
  const int output_level = output_level_inputs->level;
  if (input_level == output_level) {
    // no possibility of conflict
    return true;
  }

  // For now, we only support merging two levels, start level and output level.
  // We need to assert other levels are empty.
  for (int l = input_level + 1; l < output_level; l++) {
    assert(vstorage->NumLevelFiles(l) == 0);
  }

  InternalKey smallest, largest;

  // Get the range one last time.
  GetRange(*inputs, &smallest, &largest);

  // Populate the set of next-level files (inputs_GetOutputLevelInputs()) to
  // include in compaction
  vstorage->GetOverlappingInputs(output_level, &smallest, &largest,
                                 &output_level_inputs->files, *parent_index,
                                 parent_index);
  if (AreFilesInCompaction(output_level_inputs->files)) {
    return false;
  }
  if (!output_level_inputs->empty()) {
    if (!ExpandInputsToCleanCut(cf_name, vstorage, output_level_inputs)) {
      return false;
    }
  }

  // See if we can further grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up. We also choose NOT
  // to expand if this would cause "level" to include some entries for some
  // user key, while excluding other entries for the same user key. This
  // can happen when one user key spans multiple files.
  if (!output_level_inputs->empty()) {
    const uint64_t limit = mutable_cf_options.max_compaction_bytes;
    const uint64_t output_level_inputs_size =
        TotalCompensatedFileSize(output_level_inputs->files);
    const uint64_t inputs_size = TotalCompensatedFileSize(inputs->files);
    bool expand_inputs = false;

    CompactionInputFiles expanded_inputs;
    expanded_inputs.level = input_level;
    // Get closed interval of output level
    InternalKey all_start, all_limit;
    GetRange(*inputs, *output_level_inputs, &all_start, &all_limit);
    bool try_overlapping_inputs = true;
    vstorage->GetOverlappingInputs(input_level, &all_start, &all_limit,
                                   &expanded_inputs.files, base_index, nullptr);
    uint64_t expanded_inputs_size =
        TotalCompensatedFileSize(expanded_inputs.files);
    if (!ExpandInputsToCleanCut(cf_name, vstorage, &expanded_inputs)) {
      try_overlapping_inputs = false;
    }
    if (try_overlapping_inputs && expanded_inputs.size() > inputs->size() &&
        output_level_inputs_size + expanded_inputs_size < limit &&
        !AreFilesInCompaction(expanded_inputs.files)) {
      InternalKey new_start, new_limit;
      GetRange(expanded_inputs, &new_start, &new_limit);
      CompactionInputFiles expanded_output_level_inputs;
      expanded_output_level_inputs.level = output_level;
      vstorage->GetOverlappingInputs(output_level, &new_start, &new_limit,
                                     &expanded_output_level_inputs.files,
                                     *parent_index, parent_index);
      assert(!expanded_output_level_inputs.empty());
      if (!AreFilesInCompaction(expanded_output_level_inputs.files) &&
          ExpandInputsToCleanCut(cf_name, vstorage,
                                 &expanded_output_level_inputs) &&
          expanded_output_level_inputs.size() == output_level_inputs->size()) {
        expand_inputs = true;
      }
    }
    if (!expand_inputs) {
      vstorage->GetCleanInputsWithinInterval(input_level, &all_start,
                                             &all_limit, &expanded_inputs.files,
                                             base_index, nullptr);
      expanded_inputs_size = TotalCompensatedFileSize(expanded_inputs.files);
      if (expanded_inputs.size() > inputs->size() &&
          output_level_inputs_size + expanded_inputs_size < limit &&
          !AreFilesInCompaction(expanded_inputs.files)) {
        expand_inputs = true;
      }
    }
    if (expand_inputs) {
      ROCKS_LOG_INFO(ioptions_.info_log,
                     "[%s] Expanding@%d %" ROCKSDB_PRIszt "+%" ROCKSDB_PRIszt
                     "(%" PRIu64 "+%" PRIu64 " bytes) to %" ROCKSDB_PRIszt
                     "+%" ROCKSDB_PRIszt " (%" PRIu64 "+%" PRIu64 " bytes)\n",
                     cf_name.c_str(), input_level, inputs->size(),
                     output_level_inputs->size(), inputs_size,
                     output_level_inputs_size, expanded_inputs.size(),
                     output_level_inputs->size(), expanded_inputs_size,
                     output_level_inputs_size);
      inputs->files = expanded_inputs.files;
    }
  }
  return true;
}

void CompactionPicker::GetGrandparents(
    VersionStorageInfo* vstorage, const CompactionInputFiles& inputs,
    const CompactionInputFiles& output_level_inputs,
    std::vector<FileMetaData*>* grandparents) {
  InternalKey start, limit;
  GetRange(inputs, output_level_inputs, &start, &limit);
  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  if (output_level_inputs.level + 1 < NumberLevels()) {
    vstorage->GetOverlappingInputs(output_level_inputs.level + 1, &start,
                                   &limit, grandparents);
  }
}

// Try to perform garbage collection from certain column family.
// Resulting as a pointer of compaction, nullptr as nothing to do.
Compaction* CompactionPicker::PickGarbageCollection(
    const std::string& /*cf_name*/, uint64_t min_log_number_to_keep,
    const MutableCFOptions& mutable_cf_options, VersionStorageInfo* vstorage,
    LogBuffer* /*log_buffer*/) {
  std::vector<GarbageFileInfo> gc_files;

  // Setting fragment_size as one eighth max_file_size prevents selecting
  // massive files to single compaction which would pin down the maximum
  // deletable file number for a long time resulting possible storage leakage.
  uint64_t max_file_size =
      MaxFileSizeForLevel(mutable_cf_options, 1, ioptions_.compaction_style);
  uint64_t fragment_size = max_file_size / 8;
  uint64_t max_pick_size = max_file_size * 8;

  // Traverse level -1 to filter out all blob sstables needs GC.
  // 1. score more than garbage collection baseline.
  // 2. fragile files that can be reorganized
  // 3. marked for compaction for other reasons
  for (auto f : vstorage->LevelFiles(-1)) {
    if (!f->is_gc_permitted() || f->being_compacted) {
      continue;
    }
    if (f->prop.is_blob_wal() &&
        (f->fd.GetNumber() >= min_log_number_to_keep || f->is_gc_defered())) {
      continue;
    }
    GarbageFileInfo info = {f};
    info.score = std::min(
        1.0, f->num_antiquation / std::max<double>(1, f->prop.num_entries));
    info.estimate_size =
        static_cast<uint64_t>(f->fd.file_size * (1 - info.score));
    if (info.score >= mutable_cf_options.blob_gc_ratio ||
        info.estimate_size <= fragment_size) {
      gc_files.push_back(info);
    } else if (f->marked_for_compaction) {
      info.score = mutable_cf_options.blob_gc_ratio;
      gc_files.push_back(info);
    }
  }

  // Sorting by ratio decreasing.
  std::sort(gc_files.begin(), gc_files.end(), TERARK_CMP(score, >));

  // Return nullptr if
  //   1. Got empty section.
  //   2. Score lower than setting ratio.
  //   3. Only one small file were selected.
  if (gc_files.empty() ||
      gc_files.front().score < mutable_cf_options.blob_gc_ratio ||
      (gc_files.size() == 1 && gc_files[0].f->fd.file_size <= fragment_size)) {
    return nullptr;
  }

  // Set up inputs for garbage collection.
  std::vector<CompactionInputFiles> inputs(1);
  auto& input = inputs.front();
  input.level = -1;
  input.files.push_back(gc_files.front().f);
  gc_files.front().f->set_gc_candidate();

  uint64_t total_estimate_size = gc_files.front().estimate_size;
  uint64_t total_pick_size = gc_files.front().f->fd.file_size;
  uint64_t num_antiquation = gc_files.front().f->num_antiquation;
  for (auto it = std::next(gc_files.begin()); it != gc_files.end(); ++it) {
    auto& info = *it;
    if ((total_estimate_size + info.estimate_size > max_file_size ||
         total_pick_size + info.f->num_antiquation > max_pick_size)
#ifndef NDEBUG
        // triger wal gc aggressively in debug
        && !info.f->prop.is_blob_wal()
#endif  // !NDEBUG
    ) {
      continue;
    }
    total_estimate_size += info.estimate_size;
    total_pick_size = gc_files.front().f->fd.file_size;
    num_antiquation += info.f->num_antiquation;
    input.files.push_back(info.f);
    info.f->set_gc_candidate();
  }

  int bottommost_level = vstorage->num_levels() - 1;

  // Set compaction params.
  CompactionParams params(vstorage, ioptions_, mutable_cf_options);
  params.inputs = std::move(inputs);
  params.output_level = -1;
  params.num_antiquation = num_antiquation;
  params.max_compaction_bytes = LLONG_MAX;
  params.output_path_id = GetPathId(ioptions_, mutable_cf_options, 1);
  params.compression = GetCompressionType(
      ioptions_, vstorage, mutable_cf_options, bottommost_level, 1, true);
  params.compression_opts =
      GetCompressionOptions(ioptions_, vstorage, bottommost_level, true);
  params.max_subcompactions = 1;
  params.score = 0;
  params.compaction_type = kGarbageCollection;
  params.compaction_reason = CompactionReason::kGarbageCollection;

  return RegisterCompaction(new Compaction(std::move(params)));
}

void CompactionPicker::InitFilesBeingCompact(
    const MutableCFOptions& mutable_cf_options, VersionStorageInfo* vstorage,
    const InternalKey* begin, const InternalKey* end,
    std::unordered_set<uint64_t>* files_being_compact) {
  if (!ioptions_.enable_lazy_compaction) {
    return;
  }
  ReadOptions options;
  MapSstElement element;
  auto create_iter = [&](const FileMetaData* file_metadata,
                         const DependenceMap& depend_map, Arena* arena,
                         TableReader** table_reader_ptr) {
    return table_cache_->NewIterator(
        options, env_options_, *file_metadata, depend_map, nullptr,
        mutable_cf_options.prefix_extractor.get(), table_reader_ptr, nullptr,
        false, arena, true, -1);
  };
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    auto& level_files = vstorage->LevelFiles(level);
    if (level_files.empty()) {
      continue;
    }
    Arena arena;
    ScopedArenaIterator iter(NewMapElementIterator(
        level_files.data(), level_files.size(), icmp_, &create_iter,
        c_style_callback(create_iter), &arena));
    for (level == 0 || begin == nullptr ? iter->SeekToFirst()
                                        : iter->Seek(begin->Encode());
         iter->Valid(); iter->Next()) {
      LazyBuffer value = iter->value();
      if (!value.fetch().ok() || !element.Decode(iter->key(), value.slice())) {
        // TODO: log error ?
        break;
      }
      if (begin != nullptr &&
          icmp_->Compare(element.largest_key, begin->Encode()) < 0) {
        if (level == 0) {
          continue;
        } else {
          break;
        }
      }
      if (end != nullptr &&
          icmp_->Compare(element.smallest_key, end->Encode()) > 0) {
        if (level == 0) {
          continue;
        } else {
          break;
        }
      }
      auto& dependence_map = vstorage->dependence_map();
      for (auto& link : element.link) {
        files_being_compact->emplace(link.file_number);
        auto find = dependence_map.find(link.file_number);
        if (find == dependence_map.end()) {
          files_being_compact->emplace(link.file_number);
        } else {
          for (auto& dependence : find->second->prop.dependence) {
            files_being_compact->emplace(dependence.file_number);
          };
        }
      }
    }
  }
}

Compaction* CompactionPicker::CompactRange(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, int input_level, int output_level,
    uint32_t output_path_id, uint32_t max_subcompactions,
    const InternalKey* begin, const InternalKey* end,
    InternalKey** compaction_end, bool* manual_conflict,
    const std::unordered_set<uint64_t>* files_being_compact) {
  // CompactionPickerFIFO has its own implementation of compact range
  assert(ioptions_.compaction_style != kCompactionStyleFIFO);
  CompactionInputFiles inputs;
  inputs.level = input_level;
  bool covering_the_whole_range = true;
  if (output_level == ColumnFamilyData::kCompactToBaseLevel) {
    assert(input_level == 0);
    output_level = vstorage->base_level();
    assert(output_level > 0);
  }

  // All files are 'overlapping' in universal style compaction.
  // We have to compact the entire range in one shot.
  if (ioptions_.compaction_style == kCompactionStyleUniversal) {
    begin = nullptr;
    end = nullptr;
  } else if (ioptions_.enable_lazy_compaction) {
    LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL, ioptions_.info_log);
    if (vstorage->LevelFiles(input_level).empty()) {
      return nullptr;
    }
    if (input_level == output_level) {
      return PickRangeCompaction(cf_name, mutable_cf_options, vstorage,
                                 input_level, begin, end, max_subcompactions,
                                 files_being_compact, manual_conflict,
                                 &log_buffer);
    } else if ((*compaction_end)->size() > 0) {
      return nullptr;
    }
    if (AreFilesInCompaction(vstorage->LevelFiles(input_level)) ||
        AreFilesInCompaction(vstorage->LevelFiles(output_level))) {
      *manual_conflict = true;
      return nullptr;
    }
    std::vector<CompactionInputFiles> input_vec(2);
    input_vec[0].level = input_level;
    input_vec[0].files = vstorage->LevelFiles(input_level);
    input_vec[1].level = output_level;
    input_vec[1].files = vstorage->LevelFiles(output_level);

    SelectedRange range;
    if (begin != nullptr && end != nullptr) {
      AssignUserKey(range.start, begin->Encode());
      range.include_start = true;
      AssignUserKey(range.limit, end->Encode());
      range.include_limit = false;
    } else {
      Slice smallest_user_key, largest_user_key;
      Compaction::GetBoundaryKeys(vstorage, input_vec, &smallest_user_key,
                                  &largest_user_key);
      if (begin != nullptr) {
        AssignUserKey(range.start, begin->Encode());
      } else {
        range.start.assign(smallest_user_key.data(), smallest_user_key.size());
      }
      range.include_start = true;
      if (end != nullptr) {
        AssignUserKey(range.limit, end->Encode());
        range.include_limit = false;
      } else {
        range.limit.assign(largest_user_key.data(), largest_user_key.size());
        range.include_limit = true;
      }
    }
    (*compaction_end)->Set(Slice(), kMaxSequenceNumber, kTypeDeletion);

    CompactionParams params(vstorage, ioptions_, mutable_cf_options);
    params.inputs = std::move(input_vec);
    params.output_level = output_level;
    params.target_file_size = MaxFileSizeForLevel(
        mutable_cf_options, output_level, ioptions_.compaction_style);
    params.max_compaction_bytes = LLONG_MAX;
    params.output_path_id = output_path_id;
    params.compression = GetCompressionType(
        ioptions_, vstorage, mutable_cf_options, output_level, 1);
    params.compression_opts =
        GetCompressionOptions(ioptions_, vstorage, output_level);
    params.manual_compaction = true;
    params.max_subcompactions = 1;
    params.input_range = {std::move(range)};
    params.compaction_type = kMapCompaction;
    return RegisterCompaction(new Compaction(std::move(params)));
  }

  vstorage->GetOverlappingInputs(input_level, begin, end, &inputs.files);
  if (inputs.empty()) {
    return nullptr;
  }

  if ((input_level == 0) && (!level0_compactions_in_progress_.empty())) {
    // Only one level 0 compaction allowed
    TEST_SYNC_POINT("CompactionPicker::CompactRange:Conflict");
    *manual_conflict = true;
    return nullptr;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  if (input_level > 0) {
    const uint64_t limit = mutable_cf_options.max_compaction_bytes;
    uint64_t total = 0;
    for (size_t i = 0; i + 1 < inputs.size(); ++i) {
      uint64_t s = inputs[i]->compensated_file_size;
      total += s;
      if (total >= limit) {
        covering_the_whole_range = false;
        inputs.files.resize(i + 1);
        break;
      }
    }
  }
  assert(output_path_id < static_cast<uint32_t>(ioptions_.cf_paths.size()));

  InternalKey key_storage;
  InternalKey* next_smallest = &key_storage;
  if (ExpandInputsToCleanCut(cf_name, vstorage, &inputs, &next_smallest) ==
      false) {
    // manual compaction is now multi-threaded, so it can
    // happen that ExpandWhileOverlapping fails
    // we handle it higher in RunManualCompaction
    *manual_conflict = true;
    return nullptr;
  }

  if (covering_the_whole_range || !next_smallest) {
    *compaction_end = nullptr;
  } else {
    **compaction_end = *next_smallest;
  }

  CompactionInputFiles output_level_inputs;
  output_level_inputs.level = output_level;
  if (input_level != output_level) {
    int parent_index = -1;
    if (!SetupOtherInputs(cf_name, mutable_cf_options, vstorage, &inputs,
                          &output_level_inputs, &parent_index, -1)) {
      // manual compaction is now multi-threaded, so it can
      // happen that SetupOtherInputs fails
      // we handle it higher in RunManualCompaction
      *manual_conflict = true;
      return nullptr;
    }
  }

  std::vector<CompactionInputFiles> compaction_inputs({inputs});
  if (!output_level_inputs.empty()) {
    compaction_inputs.push_back(output_level_inputs);
  }
  for (size_t i = 0; i < compaction_inputs.size(); i++) {
    if (AreFilesInCompaction(compaction_inputs[i].files)) {
      *manual_conflict = true;
      return nullptr;
    }
  }

  // 2 non-exclusive manual compactions could run at the same time producing
  // overlaping outputs in the same level.
  if (FilesRangeOverlapWithCompaction(compaction_inputs, output_level)) {
    // This compaction output could potentially conflict with the output
    // of a currently running compaction, we cannot run it.
    *manual_conflict = true;
    return nullptr;
  }

  std::vector<FileMetaData*> grandparents;
  GetGrandparents(vstorage, inputs, output_level_inputs, &grandparents);

  CompactionParams params(vstorage, ioptions_, mutable_cf_options);
  params.inputs = std::move(compaction_inputs);
  params.output_level = output_level;
  params.target_file_size = MaxFileSizeForLevel(
      mutable_cf_options, output_level, ioptions_.compaction_style,
      vstorage->base_level(), ioptions_.level_compaction_dynamic_level_bytes);
  params.max_compaction_bytes = mutable_cf_options.max_compaction_bytes;
  params.output_path_id = output_path_id;
  params.compression =
      GetCompressionType(ioptions_, vstorage, mutable_cf_options, output_level,
                         vstorage->base_level());
  params.compression_opts =
      GetCompressionOptions(ioptions_, vstorage, output_level);
  params.max_subcompactions = max_subcompactions;
  params.grandparents = std::move(grandparents);
  params.manual_compaction = true;

  Compaction* compaction = new Compaction(std::move(params));

  TEST_SYNC_POINT_CALLBACK("CompactionPicker::CompactRange:Return", compaction);
  RegisterCompaction(compaction);

  // Creating a compaction influences the compaction score because the score
  // takes running compactions into account (by skipping files that are already
  // being compacted). Since we just changed compaction score, we recalculate it
  // here
  vstorage->ComputeCompactionScore(ioptions_, mutable_cf_options);

  return compaction;
}

Compaction* CompactionPicker::PickRangeCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, int level, const InternalKey* begin,
    const InternalKey* end, uint32_t max_subcompactions,
    const std::unordered_set<uint64_t>* files_being_compact,
    bool* manual_conflict, LogBuffer* log_buffer) {
  assert(ioptions_.enable_lazy_compaction);
  auto& level_files = vstorage->LevelFiles(level);

  if (files_being_compact == nullptr || files_being_compact->empty() ||
      level_files.empty()) {
    return nullptr;
  }
  if (AreFilesInCompaction(level_files)) {
    *manual_conflict = true;
    return nullptr;
  }
  std::vector<CompactionInputFiles> inputs;
  inputs.emplace_back(CompactionInputFiles{level, level_files});

  if (level == 0 && level_files.size() > 1) {
    uint32_t path_id = GetPathId(ioptions_, mutable_cf_options, 1ULL << 20);
    CompactionParams params(vstorage, ioptions_, mutable_cf_options);

    params.inputs = std::move(inputs);
    params.output_level = level;
    params.target_file_size = MaxFileSizeForLevel(mutable_cf_options, level,
                                                  kCompactionStyleUniversal);
    params.output_path_id = path_id;
    params.compression_opts = ioptions_.compression_opts;
    params.manual_compaction = true;
    params.max_subcompactions = 1;
    params.score = 0;
    params.compaction_type = kMapCompaction;
    return new Compaction(std::move(params));
  }

  std::vector<SelectedRange> input_range;
  Arena arena;
  DependenceMap empty_dependence_map;
  ReadOptions options;
  auto create_iter = [&](const FileMetaData* file_metadata,
                         const DependenceMap& depend_map, Arena* arena,
                         TableReader** table_reader_ptr) {
    return table_cache_->NewIterator(
        options, env_options_, *file_metadata, depend_map, nullptr,
        mutable_cf_options.prefix_extractor.get(), table_reader_ptr, nullptr,
        false, arena, true, -1);
  };
  ScopedArenaIterator iter(NewMapElementIterator(
      level_files.data(), level_files.size(), icmp_, &create_iter,
      c_style_callback(create_iter), &arena));
  if (!iter->status().ok()) {
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal: Read level files error %s.",
                     cf_name.c_str(), iter->status().getState());
    return nullptr;
  }

  MapSstElement map_element;
  SelectedRange range;
  auto& ic = ioptions_.internal_comparator;
  auto uc = ic.user_comparator();
  auto need_compact = [&](const MapSstElement& e) {
    if (begin != nullptr) {
      int c = uc->Compare(ExtractUserKey(e.largest_key), begin->user_key());
      if (GetInternalKeySeqno(e.largest_key) == kMaxSequenceNumber) {
        if (c <= 0) return false;
      } else {
        if (c < 0) return false;
      }
    }
    if (end != nullptr &&
        uc->Compare(ExtractUserKey(e.smallest_key), end->user_key()) >= 0) {
      return false;
    }
    auto& dependence_map = vstorage->dependence_map();
    for (auto& link : e.link) {
      if (files_being_compact->count(link.file_number) > 0) {
        return true;
      }
      auto find = dependence_map.find(link.file_number);
      if (find == dependence_map.end()) {
        // TODO: log error
        continue;
      }
      auto f = find->second;
      if (!f->prop.is_map_sst()) {
        continue;
      }
      for (auto& dependence : f->prop.dependence) {
        if (files_being_compact->count(dependence.file_number) > 0) {
          return true;
        }
      }
    }
    return false;
  };
  bool has_start = false;
  size_t max_compaction_bytes = mutable_cf_options.max_compaction_bytes;
  size_t subcompact_size = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (!ReadMapElement(map_element, iter.get(), log_buffer, cf_name)) {
      return nullptr;
    }
    if (has_start) {
      if (need_compact(map_element)) {
        if (subcompact_size < max_compaction_bytes) {
          subcompact_size += map_element.EstimateSize();
          AssignUserKey(range.limit, map_element.largest_key);
        } else {
          AssignUserKey(range.limit, map_element.smallest_key);
          range.include_start = true;
          range.include_limit = false;
          input_range.emplace_back(std::move(range));
          if (input_range.size() >= max_subcompactions) {
            has_start = false;
            break;
          }
          subcompact_size = map_element.EstimateSize();
          AssignUserKey(range.start, map_element.smallest_key);
          AssignUserKey(range.limit, map_element.largest_key);
        }
      } else {
        has_start = false;
        AssignUserKey(range.limit, map_element.smallest_key);
        range.include_start = true;
        range.include_limit = false;
        input_range.emplace_back(std::move(range));
        if (input_range.size() >= max_subcompactions) {
          break;
        }
        subcompact_size = 0;
      }
    } else if (need_compact(map_element)) {
      subcompact_size += map_element.EstimateSize();
      has_start = true;
      AssignUserKey(range.start, map_element.smallest_key);
      AssignUserKey(range.limit, map_element.largest_key);
    }
  }
  if (has_start) {
    range.include_start = true;
    range.include_limit = true;
    Slice end_key;
    if (level == 0) {
      for (auto f : level_files) {
        if (end_key.empty() || ic.Compare(f->largest.Encode(), end_key) > 0) {
          end_key = f->largest.Encode();
        }
      }
    } else {
      end_key = level_files.back()->largest.Encode();
    }
    end_key = ExtractUserKey(end_key);
    assert(ic.user_comparator()->Compare(range.limit, end_key) <= 0);
    range.limit.assign(end_key.data(), end_key.size());
    input_range.emplace_back(std::move(range));
  }
  if (input_range.empty()) {
    return nullptr;
  }
  if (begin != nullptr &&
      uc->Compare(input_range.front().start, begin->user_key()) < 0) {
    AssignUserKey(input_range.front().start, begin->Encode());
  }
  if (end != nullptr &&
      uc->Compare(input_range.back().limit, end->user_key()) > 0) {
    AssignUserKey(input_range.back().limit, end->Encode());
    input_range.back().include_limit = false;
  }
  if (!FixInputRange(input_range, ic, false /* sort */, false /* merge */)) {
    return nullptr;
  }
  uint32_t path_id = GetPathId(ioptions_, mutable_cf_options, level);
  CompactionParams params(vstorage, ioptions_, mutable_cf_options);

  params.inputs = std::move(inputs);
  params.output_level = level;
  params.target_file_size = MaxFileSizeForLevel(
      mutable_cf_options, std::max(1, level), ioptions_.compaction_style);
  params.max_compaction_bytes = LLONG_MAX;
  params.output_path_id = path_id;
  params.compression =
      GetCompressionType(ioptions_, vstorage, mutable_cf_options, level, 1);
  params.compression_opts = GetCompressionOptions(ioptions_, vstorage, level);
  params.manual_compaction = true;
  params.score = 0;
  params.partial_compaction = true;
  params.max_subcompactions = max_subcompactions;
  params.compaction_type = kKeyValueCompaction;
  params.input_range = std::move(input_range);

  return RegisterCompaction(new Compaction(std::move(params)));
}

#ifndef ROCKSDB_LITE
namespace {
// Test whether two files have overlapping key-ranges.
bool HaveOverlappingKeyRanges(const Comparator* c, const SstFileMetaData& a,
                              const SstFileMetaData& b) {
  if (c->Compare(a.smallestkey, b.smallestkey) >= 0) {
    if (c->Compare(a.smallestkey, b.largestkey) <= 0) {
      // b.smallestkey <= a.smallestkey <= b.largestkey
      return true;
    }
  } else if (c->Compare(a.largestkey, b.smallestkey) >= 0) {
    // a.smallestkey < b.smallestkey <= a.largestkey
    return true;
  }
  if (c->Compare(a.largestkey, b.largestkey) <= 0) {
    if (c->Compare(a.largestkey, b.smallestkey) >= 0) {
      // b.smallestkey <= a.largestkey <= b.largestkey
      return true;
    }
  } else if (c->Compare(a.smallestkey, b.largestkey) <= 0) {
    // a.smallestkey <= b.largestkey < a.largestkey
    return true;
  }
  return false;
}
}  // namespace

Status CompactionPicker::SanitizeCompactionInputFilesForAllLevels(
    std::unordered_set<uint64_t>* input_files,
    const ColumnFamilyMetaData& cf_meta, const int output_level) const {
  auto& levels = cf_meta.levels;
  auto comparator = icmp_->user_comparator();

  // TODO(yhchiang): add is_adjustable to CompactionOptions

  // the smallest and largest key of the current compaction input
  std::string smallestkey;
  std::string largestkey;
  // a flag for initializing smallest and largest key
  bool is_first = false;
  const int kNotFound = -1;

  // For each level, it does the following things:
  // 1. Find the first and the last compaction input files
  //    in the current level.
  // 2. Include all files between the first and the last
  //    compaction input files.
  // 3. Update the compaction key-range.
  // 4. For all remaining levels, include files that have
  //    overlapping key-range with the compaction key-range.
  for (int l = 0; l <= output_level; ++l) {
    auto& current_files = levels[l].files;
    int first_included = static_cast<int>(current_files.size());
    int last_included = kNotFound;

    // identify the first and the last compaction input files
    // in the current level.
    for (size_t f = 0; f < current_files.size(); ++f) {
      if (input_files->find(TableFileNameToNumber(current_files[f].name)) !=
          input_files->end()) {
        first_included = std::min(first_included, static_cast<int>(f));
        last_included = std::max(last_included, static_cast<int>(f));
        if (is_first == false) {
          smallestkey = current_files[f].smallestkey;
          largestkey = current_files[f].largestkey;
          is_first = true;
        }
      }
    }
    if (last_included == kNotFound) {
      continue;
    }

    if (l != 0) {
      // expend the compaction input of the current level if it
      // has overlapping key-range with other non-compaction input
      // files in the same level.
      while (first_included > 0) {
        if (comparator->Compare(current_files[first_included - 1].largestkey,
                                current_files[first_included].smallestkey) <
            0) {
          break;
        }
        first_included--;
      }

      while (last_included < static_cast<int>(current_files.size()) - 1) {
        if (comparator->Compare(current_files[last_included + 1].smallestkey,
                                current_files[last_included].largestkey) > 0) {
          break;
        }
        last_included++;
      }
    } else if (output_level > 0) {
      last_included = static_cast<int>(current_files.size() - 1);
    }

    // include all files between the first and the last compaction input files.
    for (int f = first_included; f <= last_included; ++f) {
      if (current_files[f].being_compacted) {
        return Status::Aborted("Necessary compaction input file " +
                               current_files[f].name +
                               " is currently being compacted.");
      }
      input_files->insert(TableFileNameToNumber(current_files[f].name));
    }

    // update smallest and largest key
    if (l == 0) {
      for (int f = first_included; f <= last_included; ++f) {
        if (comparator->Compare(smallestkey, current_files[f].smallestkey) >
            0) {
          smallestkey = current_files[f].smallestkey;
        }
        if (comparator->Compare(largestkey, current_files[f].largestkey) < 0) {
          largestkey = current_files[f].largestkey;
        }
      }
    } else {
      if (comparator->Compare(smallestkey,
                              current_files[first_included].smallestkey) > 0) {
        smallestkey = current_files[first_included].smallestkey;
      }
      if (comparator->Compare(largestkey,
                              current_files[last_included].largestkey) < 0) {
        largestkey = current_files[last_included].largestkey;
      }
    }

    SstFileMetaData aggregated_file_meta;
    aggregated_file_meta.smallestkey = smallestkey;
    aggregated_file_meta.largestkey = largestkey;

    // For all lower levels, include all overlapping files.
    // We need to add overlapping files from the current level too because even
    // if there no input_files in level l, we would still need to add files
    // which overlap with the range containing the input_files in levels 0 to l
    // Level 0 doesn't need to be handled this way because files are sorted by
    // time and not by key
    for (int m = std::max(l, 1); m <= output_level; ++m) {
      for (auto& next_lv_file : levels[m].files) {
        if (HaveOverlappingKeyRanges(comparator, aggregated_file_meta,
                                     next_lv_file)) {
          if (next_lv_file.being_compacted) {
            return Status::Aborted(
                "File " + next_lv_file.name +
                " that has overlapping key range with one of the compaction "
                " input file is currently being compacted.");
          }
          input_files->insert(TableFileNameToNumber(next_lv_file.name));
        }
      }
    }
  }
  if (RangeOverlapWithCompaction(smallestkey, largestkey, output_level)) {
    return Status::Aborted(
        "A running compaction is writing to the same output level in an "
        "overlapping key range");
  }
  return Status::OK();
}

Status CompactionPicker::SanitizeCompactionInputFiles(
    std::unordered_set<uint64_t>* input_files,
    const ColumnFamilyMetaData& cf_meta, const int output_level) const {
  assert(static_cast<int>(cf_meta.levels.size()) - 1 ==
         cf_meta.levels[cf_meta.levels.size() - 1].level);
  if (output_level >= static_cast<int>(cf_meta.levels.size())) {
    return Status::InvalidArgument(
        "Output level for column family " + cf_meta.name +
        " must between [0, " +
        ToString(cf_meta.levels[cf_meta.levels.size() - 1].level) + "].");
  }

  if (output_level > MaxOutputLevel()) {
    return Status::InvalidArgument(
        "Exceed the maximum output level defined by "
        "the current compaction algorithm --- " +
        ToString(MaxOutputLevel()));
  }

  if (output_level < 0) {
    return Status::InvalidArgument("Output level cannot be negative.");
  }

  if (input_files->size() == 0) {
    return Status::InvalidArgument(
        "A compaction must contain at least one file.");
  }

  Status s = SanitizeCompactionInputFilesForAllLevels(input_files, cf_meta,
                                                      output_level);

  if (!s.ok()) {
    return s;
  }

  // for all input files, check whether the file number matches
  // any currently-existing files.
  for (auto file_num : *input_files) {
    bool found = false;
    for (const auto& level_meta : cf_meta.levels) {
      for (const auto& file_meta : level_meta.files) {
        if (file_num == TableFileNameToNumber(file_meta.name)) {
          if (file_meta.being_compacted) {
            return Status::Aborted("Specified compaction input file " +
                                   MakeTableFileName("", file_num) +
                                   " is already being compacted.");
          }
          found = true;
          break;
        }
      }
      if (found) {
        break;
      }
    }
    if (!found) {
      return Status::InvalidArgument(
          "Specified compaction input file " + MakeTableFileName("", file_num) +
          " does not exist in column family " + cf_meta.name + ".");
    }
  }

  return Status::OK();
}
#endif  // !ROCKSDB_LITE

Compaction* CompactionPicker::RegisterCompaction(Compaction* c) {
  if (c == nullptr) {
    return nullptr;
  }
  assert(ioptions_.compaction_style != kCompactionStyleLevel ||
         ioptions_.enable_lazy_compaction || c->output_level() <= 0 ||
         !FilesRangeOverlapWithCompaction(*c->inputs(), c->output_level()));
  if (c->start_level() == 0 ||
      ioptions_.compaction_style == kCompactionStyleUniversal) {
    level0_compactions_in_progress_.insert(c);
  }
  compactions_in_progress_.insert(c);
  return c;
}

void CompactionPicker::UnregisterCompaction(Compaction* c) {
  if (c == nullptr) {
    return;
  }
  if (c->start_level() == 0 ||
      ioptions_.compaction_style == kCompactionStyleUniversal) {
    level0_compactions_in_progress_.erase(c);
  }
  compactions_in_progress_.erase(c);
}

/*
 *  PickCompositeCompaction() means to pick compaction from SSTables in the
 *  same level, also called tiny/inner compaction at somewhere. Aiming to
 *  reduce read and space amplifications at a lower cost.
 */
Compaction* CompactionPicker::PickCompositeCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, const std::vector<SequenceNumber>& snapshots,
    const std::vector<SortedRun>& sorted_runs, LogBuffer* log_buffer) {
  // Return nullptr if there is no space amplification.
  if (!vstorage->has_space_amplification()) {
    return nullptr;
  }
  std::vector<CompactionInputFiles> inputs(1);
  auto& input = inputs.front();
  input.level = -1;
  double max_read_amp_ratio = -std::numeric_limits<double>::infinity();
  double read_amp = 1;
  uint32_t max_subcompactions = mutable_cf_options.max_subcompactions;
  // Traverse all sorted_runs from the highest to bottomest finding selection.
  for (auto& sr : sorted_runs) {
    // Skip if this sorted run was occupied by other compaction.
    if (sr.skip_composite) {
      continue;
    }
    FileMetaData* f;
    if (sr.level > 0) {
      // Skip if this level has no map sst.
      if (!vstorage->has_map_sst(sr.level)) {
        continue;
      }
      auto& level_files = vstorage->LevelFiles(sr.level);
      // Skip if any file of this sorted run were being compacted.
      if (AreFilesInCompaction(level_files)) {
        continue;
      }
      // Make selection.
      if (level_files.size() > 1) {
        input.level = sr.level;
        input.files.clear();
        break;
      }
      f = level_files.front();
    } else {
      // Skip if this file were being compacted or it was not a msap sstable.
      if (sr.file->being_compacted || !sr.file->prop.is_map_sst()) {
        continue;
      }
      f = sr.file;
    }
    // Estimate overall read amplification of selects.
    double level_read_amp = f->prop.read_amp;
    double level_read_amp_ratio = 1. * level_read_amp / sr.size;
    if (level_read_amp <= 1) {
      level_read_amp_ratio = -level_read_amp_ratio;
    }
    if (level_read_amp_ratio >= max_read_amp_ratio) {
      max_read_amp_ratio = level_read_amp_ratio;
      read_amp = level_read_amp;
      input.level = sr.level;
      input.files = {f};
    }
  }
  // Return nullptr if traverse gets nothing.
  if (input.level == -1) {
    return nullptr;
  }
  if (max_read_amp_ratio < 0 && vstorage->num_non_empty_levels() > 1 &&
      !vstorage->has_map_sst(vstorage->num_non_empty_levels() - 1)) {
    auto c = PickBottommostLevelCompaction(cf_name, mutable_cf_options,
                                           vstorage, snapshots, log_buffer);
    if (c != nullptr) {
      return c;
    }
  }
  CompactionType compaction_type = kKeyValueCompaction;
  std::vector<SelectedRange> input_range;

  auto new_compaction = [&] {
    int level = input.level;
    CompactionParams params(vstorage, ioptions_, mutable_cf_options);
    params.inputs = std::move(inputs);
    params.output_level = level;
    params.target_file_size = MaxFileSizeForLevel(
        mutable_cf_options, std::max(1, level), ioptions_.compaction_style);
    params.max_compaction_bytes = LLONG_MAX;
    params.output_path_id = GetPathId(ioptions_, mutable_cf_options, level);
    params.compression = GetCompressionType(ioptions_, vstorage,
                                            mutable_cf_options, level, 1, true);
    params.compression_opts =
        GetCompressionOptions(ioptions_, vstorage, level, true);
    params.max_subcompactions = max_subcompactions;
    params.score = read_amp;
    params.partial_compaction = true;
    params.compaction_type = compaction_type;
    params.input_range = std::move(input_range);
    params.compaction_reason = CompactionReason::kCompositeAmplification;

    return new Compaction(std::move(params));
  };

  if (input.files.empty()) {
    input.files = vstorage->LevelFiles(input.level);
    assert(input.files.size() > 1);
    compaction_type = kMapCompaction;
    max_subcompactions = 1;
    return new_compaction();
  }

  Arena arena;
  DependenceMap empty_dependence_map;
  ReadOptions options;
  ScopedArenaIterator iter(table_cache_->NewIterator(
      options, env_options_, *input.files.front(), empty_dependence_map,
      nullptr, mutable_cf_options.prefix_extractor.get(), nullptr, nullptr,
      false, &arena, true, input.level));
  if (!iter->status().ok()) {
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Universal: Read map sst error %s.",
                     cf_name.c_str(), iter->status().getState());
    return nullptr;
  }

  auto is_perfect = [this, vstorage](const MapSstElement& e) {
    if (e.link.size() != 1) {
      return false;
    }
    auto& dependence_map = vstorage->dependence_map();
    auto find = dependence_map.find(e.link.front().file_number);
    if (find == dependence_map.end()) {
      // TODO log error
      return false;
    }
    auto f = find->second;
    if (f->prop.is_map_sst()) {
      return false;
    }
    Range r(e.smallest_key, e.largest_key, e.include_smallest,
            e.include_largest);
    return IsPerfectRange(r, f, *icmp_);
  };

  std::unordered_map<uint64_t, FileUseInfo> file_used;
  MapSstElement map_element;
  SelectedRange range;
  auto uc = ioptions_.internal_comparator.user_comparator();
  (void)uc;

  auto set_include_limit = [&] {
    range.include_limit = true;
    auto uend = input.files.front()->largest.user_key();
    assert(uc->Compare(range.limit, uend) <= 0);
    range.limit.assign(uend.data(), uend.size());
  };

  for (auto& dependence : input.files.front()->prop.dependence) {
    auto& dependence_map = vstorage->dependence_map();
    FileUseInfo info;
    auto find = dependence_map.find(dependence.file_number);
    if (find == dependence_map.end()) {
      // TODO log error
      continue;
    }
    auto f = find->second;
    info.size = f->fd.GetFileSize();
    info.used = info.size - info.size * f->num_antiquation /
                                std::max<uint64_t>(1, f->prop.num_entries);
    file_used.emplace(dependence.file_number, info);
  }
  std::vector<PickerCompositeHeapItem> priority_heap;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (!ReadMapElement(map_element, iter.get(), log_buffer, cf_name)) {
      return nullptr;
    }
    if (is_perfect(map_element)) {
      continue;
    }
    double p = map_element.link.size();
    size_t size = 0, used = 0;
    for (auto& l : map_element.link) {
      auto find = file_used.find(l.file_number);
      if (find == file_used.end()) {
        // TODO log error
        continue;
      }
      size += find->second.size;
      used += find->second.used;
    }
    p *= (1 + double(size - std::min(used, size)) / size);
    if (p <= 2.0) {
      continue;
    }
    PickerCompositeHeapItem item = {
        ArenaPinSlice(map_element.largest_key, &arena), p};
    priority_heap.push_back(item);
  }
  std::make_heap(priority_heap.begin(), priority_heap.end(),
                 std::less<double>());
  struct Comp {
    const InternalKeyComparator* c;
    bool operator()(const Slice& a, const Slice& b) const {
      return c->Compare(a, b) < 0;
    }
  } c = {icmp_};
  std::set<Slice, Comp> unique_check(c);
  auto push_unique = [&](const Slice& key) {
    unique_check.emplace(ArenaPinSlice(key, &arena));
  };
  uint64_t pick_size =
      size_t(MaxFileSizeForLevel(mutable_cf_options, std::max(1, input.level),
                                 ioptions_.compaction_style) *
             2);
  auto estimate_size = [](const MapSstElement& element) {
    uint64_t sum = 0;
    for (auto& l : element.link) {
      sum += l.size;
    }
    return sum;
  };
  while (!priority_heap.empty()) {
    auto key = priority_heap.front().k;
    auto weight = priority_heap.front().s;
    std::pop_heap(priority_heap.begin(), priority_heap.end(),
                  std::less<double>());
    priority_heap.pop_back();
    iter->Seek(key);
    assert(iter->Valid());
    if (unique_check.count(iter->key()) > 0) {
      continue;
    }
    if (!ReadMapElement(map_element, iter.get(), log_buffer, cf_name)) {
      return nullptr;
    }
    AssignUserKey(range.start, map_element.smallest_key);
    AssignUserKey(range.limit, map_element.largest_key);
    range.include_start = true;
    range.include_limit = false;
    uint64_t sum = estimate_size(map_element);
    push_unique(iter->key());
    while (sum < pick_size) {
      iter->Next();
      if (!iter->Valid()) {
        set_include_limit();
        break;
      }
      if (!ReadMapElement(map_element, iter.get(), log_buffer, cf_name)) {
        return nullptr;
      }
      if (unique_check.count(iter->key()) > 0 || is_perfect(map_element)) {
        AssignUserKey(range.limit, map_element.smallest_key);
        break;
      } else {
        AssignUserKey(range.limit, map_element.largest_key);
        sum += estimate_size(map_element);
        push_unique(iter->key());
      }
    }
    // always try pick prev range
    iter->SeekForPrev(key);
    assert(iter->Valid());
    do {
      iter->Prev();
      if (!iter->Valid() || unique_check.count(iter->key()) > 0) {
        break;
      }
      if (!ReadMapElement(map_element, iter.get(), log_buffer, cf_name)) {
        return nullptr;
      }
      if (is_perfect(map_element)) {
        break;
      }
      AssignUserKey(range.start, map_element.smallest_key);
      sum += estimate_size(map_element);
      push_unique(iter->key());
    } while (sum < pick_size);
    input_range.emplace_back(SelectedRange(std::move(range), weight));
    if (input_range.size() >= max_subcompactions) {
      break;
    }
  }
  if (!input_range.empty()) {
    if (CompactionPicker::FixInputRange(input_range,
                                        ioptions_.internal_comparator,
                                        true /* sort */, false /* merge */)) {
      compaction_type = kKeyValueCompaction;
      return new_compaction();
    }
  }
  bool has_start = false;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (!ReadMapElement(map_element, iter.get(), log_buffer, cf_name)) {
      return nullptr;
    }

    if (has_start) {
      if (is_perfect(map_element)) {
        has_start = false;
        AssignUserKey(range.limit, map_element.smallest_key);
        range.include_start = true;
        range.include_limit = false;
        input_range.emplace_back(std::move(range), 0);
        if (input_range.size() >= max_subcompactions) {
          break;
        }
      } else {
        AssignUserKey(range.limit, map_element.largest_key);
      }
    } else if (!is_perfect(map_element)) {
      has_start = true;
      AssignUserKey(range.start, map_element.smallest_key);
      AssignUserKey(range.limit, map_element.largest_key);
    }
  }
  if (has_start) {
    range.include_start = true;
    set_include_limit();
    input_range.emplace_back(std::move(range));
  }
  if (CompactionPicker::FixInputRange(input_range,
                                      ioptions_.internal_comparator,
                                      false /* sort */, false /* merge */)) {
    compaction_type = kKeyValueCompaction;
    return new_compaction();
  }
  // for unmap level 0
  if (input.level != 0) {
    max_subcompactions = 1;
    compaction_type = kMapCompaction;
    return new_compaction();
  }
  return nullptr;
}

Compaction* CompactionPicker::PickBottommostLevelCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, const std::vector<SequenceNumber>& snapshots,
    LogBuffer* log_buffer) {
  assert(ioptions_.enable_lazy_compaction);
  assert(std::is_sorted(snapshots.begin(), snapshots.end()));
  if (vstorage->num_non_empty_levels() <= 1) {
    return nullptr;
  }
  auto need_compact = [&](FileMetaData* f) {
    if (f->being_compacted || f->prop.is_map_sst()) {
      return false;
    }
    if (f->marked_for_compaction) {
      return true;
    }
    if (!f->prop.has_snapshots() && f->prop.num_deletions == 0) {
      return false;
    }
    SequenceNumber oldest_snapshot_seqnum =
        snapshots.empty() ? kMaxSequenceNumber : snapshots.front();
    if (!ioptions_.pin_table_properties_in_reader) {
      return f->fd.largest_seqno < oldest_snapshot_seqnum;
    }
    std::shared_ptr<const TableProperties> tp;
    auto s = table_cache_->GetTableProperties(
        env_options_, *f, &tp, mutable_cf_options.prefix_extractor.get(), true);
    if (s.IsIncomplete()) {
      return f->fd.largest_seqno < oldest_snapshot_seqnum;
    }
    if (!s.ok()) {
      ROCKS_LOG_BUFFER(log_buffer,
                       "[%s] CompactionPicker::PickBottommostLevelCompaction "
                       "GetTableProperties fail\n",
                       cf_name.c_str(), s.ToString().c_str());
      return false;
    }
    if (!tp || tp->snapshots.empty()) {
      return false;
    }
    std::vector<SequenceNumber> diff;
    std::set_difference(tp->snapshots.begin(), tp->snapshots.end(),
                        snapshots.begin(), snapshots.end(),
                        std::back_inserter(diff));
    return !diff.empty();
  };
  int level = vstorage->num_non_empty_levels() - 1;
  auto& level_files = vstorage->LevelFiles(level);
  uint32_t max_subcompactions = mutable_cf_options.max_subcompactions;
  std::vector<CompactionInputFiles> inputs;
  inputs.emplace_back(CompactionInputFiles{level, level_files});

  std::vector<SelectedRange> input_range;
  SelectedRange range;
  bool has_start = false;
  size_t max_compaction_bytes = mutable_cf_options.max_compaction_bytes;
  size_t subcompact_size = 0;
  for (auto f : level_files) {
    if (has_start) {
      if (need_compact(f)) {
        if (subcompact_size < max_compaction_bytes) {
          subcompact_size += f->fd.file_size;
          AssignUserKey(range.limit, f->largest.Encode());
        } else {
          AssignUserKey(range.limit, f->smallest.Encode());
          range.include_start = true;
          range.include_limit = false;
          input_range.emplace_back(std::move(range));
          if (input_range.size() >= max_subcompactions) {
            has_start = false;
            break;
          }
          subcompact_size = f->fd.file_size;
          AssignUserKey(range.start, f->smallest.Encode());
          AssignUserKey(range.limit, f->largest.Encode());
        }
      } else {
        has_start = false;
        AssignUserKey(range.limit, f->smallest.Encode());
        range.include_start = true;
        range.include_limit = false;
        input_range.emplace_back(std::move(range));
        if (input_range.size() >= max_subcompactions) {
          break;
        }
        subcompact_size = 0;
      }
    } else if (need_compact(f)) {
      subcompact_size += f->fd.file_size;
      has_start = true;
      AssignUserKey(range.start, f->smallest.Encode());
      AssignUserKey(range.limit, f->largest.Encode());
    }
  }
  if (has_start) {
    range.include_start = true;
    range.include_limit = true;
    AssignUserKey(range.limit, level_files.back()->largest.Encode());
    input_range.emplace_back(std::move(range));
  }
  if (!CompactionPicker::FixInputRange(input_range,
                                       ioptions_.internal_comparator,
                                       false /* sort */, false /* merge */)) {
    return nullptr;
  }
  CompactionParams params(vstorage, ioptions_, mutable_cf_options);
  params.inputs = std::move(inputs);
  params.output_level = level;
  params.target_file_size = MaxFileSizeForLevel(
      mutable_cf_options, std::max(1, level), ioptions_.compaction_style);
  params.max_compaction_bytes = LLONG_MAX;
  params.output_path_id = GetPathId(ioptions_, mutable_cf_options, level);
  params.compression = GetCompressionType(ioptions_, vstorage,
                                          mutable_cf_options, level, 1, true);
  params.compression_opts =
      GetCompressionOptions(ioptions_, vstorage, level, true);
  params.max_subcompactions = max_subcompactions;
  params.score = 0;
  params.partial_compaction = true;
  params.compaction_type = kKeyValueCompaction;
  params.input_range = std::move(input_range);
  params.compaction_reason = CompactionReason::kBottommostFiles;

  return new Compaction(std::move(params));
}

void CompactionPicker::PickFilesMarkedForCompaction(
    const std::string& cf_name, VersionStorageInfo* vstorage, int* start_level,
    int* output_level, CompactionInputFiles* start_level_inputs) {
  if (vstorage->FilesMarkedForCompaction().empty()) {
    return;
  }

  auto continuation = [&, cf_name](std::pair<int, FileMetaData*> level_file) {
    // If it's being compacted it has nothing to do here.
    // If this assert() fails that means that some function marked some
    // files as being_compacted, but didn't call ComputeCompactionScore()
    assert(!level_file.second->being_compacted);
    *start_level = level_file.first;
    *output_level =
        (*start_level == 0) ? vstorage->base_level() : *start_level + 1;

    if (*start_level == 0 && !level0_compactions_in_progress()->empty()) {
      return false;
    }

    start_level_inputs->files = {level_file.second};
    start_level_inputs->level = *start_level;
    return ExpandInputsToCleanCut(cf_name, vstorage, start_level_inputs);
  };

  // take a chance on a random file first
  Random64 rnd(/* seed */ reinterpret_cast<uint64_t>(vstorage));
  size_t random_file_index = static_cast<size_t>(rnd.Uniform(
      static_cast<uint64_t>(vstorage->FilesMarkedForCompaction().size())));

  if (continuation(vstorage->FilesMarkedForCompaction()[random_file_index])) {
    // found the compaction!
    return;
  }

  for (auto& level_file : vstorage->FilesMarkedForCompaction()) {
    if (continuation(level_file)) {
      // found the compaction!
      return;
    }
  }
  start_level_inputs->files.clear();
}

bool CompactionPicker::GetOverlappingL0Files(
    VersionStorageInfo* vstorage, CompactionInputFiles* start_level_inputs,
    int output_level, int* parent_index) {
  // Two level 0 compaction won't run at the same time, so don't need to worry
  // about files on level 0 being compacted.
  assert(level0_compactions_in_progress()->empty());
  InternalKey smallest, largest;
  GetRange(*start_level_inputs, &smallest, &largest);
  // Note that the next call will discard the file we placed in
  // c->inputs_[0] earlier and replace it with an overlapping set
  // which will include the picked file.
  start_level_inputs->files.clear();
  vstorage->GetOverlappingInputs(0, &smallest, &largest,
                                 &(start_level_inputs->files));

  // If we include more L0 files in the same compaction run it can
  // cause the 'smallest' and 'largest' key to get extended to a
  // larger range. So, re-invoke GetRange to get the new key range
  GetRange(*start_level_inputs, &smallest, &largest);
  if (IsRangeInCompaction(vstorage, &smallest, &largest, output_level,
                          parent_index)) {
    return false;
  }
  assert(!start_level_inputs->files.empty());

  return true;
}

bool LevelCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  if (!vstorage->ExpiredTtlFiles().empty()) {
    return true;
  }
  if (!vstorage->BottommostFilesMarkedForCompaction().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForCompaction().empty()) {
    return true;
  }
  for (int i = 0; i <= vstorage->MaxInputLevel(); i++) {
    if (vstorage->CompactionScore(i) >= 1) {
      return true;
    }
  }
  return vstorage->has_space_amplification();
}

namespace {
// A class to build a leveled compaction step-by-step.
class LevelCompactionBuilder {
 public:
  LevelCompactionBuilder(const std::string& cf_name,
                         VersionStorageInfo* vstorage,
                         CompactionPicker* compaction_picker,
                         LogBuffer* log_buffer,
                         const MutableCFOptions& mutable_cf_options,
                         const ImmutableCFOptions& ioptions)
      : cf_name_(cf_name),
        vstorage_(vstorage),
        compaction_picker_(compaction_picker),
        log_buffer_(log_buffer),
        mutable_cf_options_(mutable_cf_options),
        ioptions_(ioptions) {}

  // Pick and return a compaction.
  Compaction* PickCompaction();

  // Pick lazy compaction
  Compaction* PickLazyCompaction(const std::vector<SequenceNumber>& snapshots);

  // Pick the initial files to compact to the next level. (or together
  // in Intra-L0 compactions)
  void SetupInitialFiles();

  // If the initial files are from L0 level, pick other L0
  // files if needed.
  bool SetupOtherL0FilesIfNeeded();

  // Based on initial files, setup other files need to be compacted
  // in this compaction, accordingly.
  bool SetupOtherInputsIfNeeded();

  Compaction* GetCompaction();

  // For the specfied level, pick a file that we want to compact.
  // Returns false if there is no file to compact.
  // If it returns true, inputs->files.size() will be exactly one.
  // If level is 0 and there is already a compaction on that level, this
  // function will return false.
  bool PickFileToCompact();

  // For L0->L0, picks the longest span of files that aren't currently
  // undergoing compaction for which work-per-deleted-file decreases. The span
  // always starts from the newest L0 file.
  //
  // Intra-L0 compaction is independent of all other files, so it can be
  // performed even when L0->base_level compactions are blocked.
  //
  // Returns true if `inputs` is populated with a span of files to be compacted;
  // otherwise, returns false.
  bool PickIntraL0Compaction();

  void PickExpiredTtlFiles();

  const std::string& cf_name_;
  VersionStorageInfo* vstorage_;
  CompactionPicker* compaction_picker_;
  LogBuffer* log_buffer_;
  int start_level_ = -1;
  int output_level_ = -1;
  int parent_index_ = -1;
  int base_index_ = -1;
  double start_level_score_ = 0;
  bool is_manual_ = false;
  CompactionInputFiles start_level_inputs_;
  std::vector<CompactionInputFiles> compaction_inputs_;
  CompactionInputFiles output_level_inputs_;
  std::vector<FileMetaData*> grandparents_;
  CompactionType compaction_type_ = CompactionType::kKeyValueCompaction;
  std::vector<SelectedRange> input_range_ = {};
  CompactionReason compaction_reason_ = CompactionReason::kUnknown;

  const MutableCFOptions& mutable_cf_options_;
  const ImmutableCFOptions& ioptions_;

  static const int kMinFilesForIntraL0Compaction = 4;
};

void LevelCompactionBuilder::PickExpiredTtlFiles() {
  if (vstorage_->ExpiredTtlFiles().empty()) {
    return;
  }

  auto continuation = [&](std::pair<int, FileMetaData*> level_file) {
    // If it's being compacted it has nothing to do here.
    // If this assert() fails that means that some function marked some
    // files as being_compacted, but didn't call ComputeCompactionScore()
    assert(!level_file.second->being_compacted);
    start_level_ = level_file.first;
    output_level_ =
        (start_level_ == 0) ? vstorage_->base_level() : start_level_ + 1;

    if ((start_level_ == vstorage_->num_non_empty_levels() - 1) ||
        (start_level_ == 0 &&
         !compaction_picker_->level0_compactions_in_progress()->empty())) {
      return false;
    }

    start_level_inputs_.files = {level_file.second};
    start_level_inputs_.level = start_level_;
    return compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                      &start_level_inputs_);
  };

  for (auto& level_file : vstorage_->ExpiredTtlFiles()) {
    if (continuation(level_file)) {
      // found the compaction!
      return;
    }
  }

  start_level_inputs_.files.clear();
}

void LevelCompactionBuilder::SetupInitialFiles() {
  // Find the compactions by size on all levels.
  bool skipped_l0_to_base = false;
  for (int i = 0; i < compaction_picker_->NumberLevels() - 1; i++) {
    start_level_score_ = vstorage_->CompactionScore(i);
    start_level_ = vstorage_->CompactionScoreLevel(i);
    assert(i == 0 || start_level_score_ <= vstorage_->CompactionScore(i - 1));
    if (start_level_score_ >= 1) {
      if (skipped_l0_to_base && start_level_ == vstorage_->base_level()) {
        // If L0->base_level compaction is pending, don't schedule further
        // compaction from base level. Otherwise L0->base_level compaction
        // may starve.
        continue;
      }
      output_level_ =
          (start_level_ == 0) ? vstorage_->base_level() : start_level_ + 1;
      if (PickFileToCompact()) {
        // found the compaction!
        if (start_level_ == 0) {
          // L0 score = `num L0 files` / `level0_file_num_compaction_trigger`
          compaction_reason_ = CompactionReason::kLevelL0FilesNum;
        } else {
          // L1+ score = `Level files size` / `MaxBytesForLevel`
          compaction_reason_ = CompactionReason::kLevelMaxLevelSize;
        }
        break;
      } else {
        // didn't find the compaction, clear the inputs
        start_level_inputs_.clear();
        if (start_level_ == 0) {
          skipped_l0_to_base = true;
          // L0->base_level may be blocked due to ongoing L0->base_level
          // compactions. It may also be blocked by an ongoing compaction from
          // base_level downwards.
          //
          // In these cases, to reduce L0 file count and thus reduce likelihood
          // of write stalls, we can attempt compacting a span of files within
          // L0.
          if (PickIntraL0Compaction()) {
            output_level_ = 0;
            compaction_reason_ = CompactionReason::kLevelL0FilesNum;
            break;
          }
        }
      }
    }
  }

  // if we didn't find a compaction, check if there are any files marked for
  // compaction
  if (start_level_inputs_.empty()) {
    parent_index_ = base_index_ = -1;

    // PickFilesMarkedForCompaction();
    compaction_picker_->PickFilesMarkedForCompaction(
        cf_name_, vstorage_, &start_level_, &output_level_,
        &start_level_inputs_);
    if (!start_level_inputs_.empty()) {
      is_manual_ = true;
      compaction_reason_ = CompactionReason::kFilesMarkedForCompaction;
      return;
    }

    size_t i;
    for (i = 0; i < vstorage_->BottommostFilesMarkedForCompaction().size();
         ++i) {
      auto& level_and_file = vstorage_->BottommostFilesMarkedForCompaction()[i];
      assert(!level_and_file.second->being_compacted);
      start_level_inputs_.level = output_level_ = start_level_ =
          level_and_file.first;
      start_level_inputs_.files = {level_and_file.second};
      if (compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                     &start_level_inputs_)) {
        break;
      }
    }
    if (i == vstorage_->BottommostFilesMarkedForCompaction().size()) {
      start_level_inputs_.clear();
    } else {
      assert(!start_level_inputs_.empty());
      compaction_reason_ = CompactionReason::kBottommostFiles;
      return;
    }

    assert(start_level_inputs_.empty());
    PickExpiredTtlFiles();
    if (!start_level_inputs_.empty()) {
      compaction_reason_ = CompactionReason::kTtl;
    }
  }
}

bool LevelCompactionBuilder::SetupOtherL0FilesIfNeeded() {
  if (start_level_ == 0 && output_level_ != 0) {
    return compaction_picker_->GetOverlappingL0Files(
        vstorage_, &start_level_inputs_, output_level_, &parent_index_);
  }
  return true;
}

bool LevelCompactionBuilder::SetupOtherInputsIfNeeded() {
  // Setup input files from output level. For output to L0, we only compact
  // spans of files that do not interact with any pending compactions, so don't
  // need to consider other levels.
  if (output_level_ != 0) {
    output_level_inputs_.level = output_level_;
    if (!compaction_picker_->SetupOtherInputs(
            cf_name_, mutable_cf_options_, vstorage_, &start_level_inputs_,
            &output_level_inputs_, &parent_index_, base_index_)) {
      return false;
    }

    compaction_inputs_.push_back(start_level_inputs_);
    if (!output_level_inputs_.empty()) {
      compaction_inputs_.push_back(output_level_inputs_);
    }

    // In some edge cases we could pick a compaction that will be compacting
    // a key range that overlap with another running compaction, and both
    // of them have the same output level. This could happen if
    // (1) we are running a non-exclusive manual compaction
    // (2) AddFile ingest a new file into the LSM tree
    // We need to disallow this from happening.
    if (compaction_picker_->FilesRangeOverlapWithCompaction(compaction_inputs_,
                                                            output_level_)) {
      // This compaction output could potentially conflict with the output
      // of a currently running compaction, we cannot run it.
      return false;
    }
    compaction_picker_->GetGrandparents(vstorage_, start_level_inputs_,
                                        output_level_inputs_, &grandparents_);
  } else {
    compaction_inputs_.push_back(start_level_inputs_);
  }
  return true;
}

Compaction* LevelCompactionBuilder::PickCompaction() {
  // Pick up the first file to start compaction. It may have been extended
  // to a clean cut.
  SetupInitialFiles();
  if (start_level_inputs_.empty()) {
    return nullptr;
  }
  assert(start_level_ >= 0 && output_level_ >= 0);

  // If it is a L0 -> base level compaction, we need to set up other L0
  // files if needed.
  if (!SetupOtherL0FilesIfNeeded()) {
    return nullptr;
  }

  // Pick files in the output level and expand more files in the start level
  // if needed.
  if (!SetupOtherInputsIfNeeded()) {
    return nullptr;
  }

  // Form a compaction object containing the files we picked.
  Compaction* c = GetCompaction();

  TEST_SYNC_POINT_CALLBACK("LevelCompactionPicker::PickCompaction:Return", c);

  return c;
}

Compaction* LevelCompactionBuilder::PickLazyCompaction(
    const std::vector<SequenceNumber>& snapshots) {
  using SortedRun = CompactionPicker::SortedRun;
  std::vector<SortedRun> sorted_runs(vstorage_->num_levels());
  int bottommost_level = vstorage_->num_non_empty_levels() - 1;
  if (bottommost_level < 0) {
    return nullptr;
  }
  assert(bottommost_level < vstorage_->num_levels());

  auto picker = compaction_picker_;
  // filter out being_compacted levels
  for (int i = 0; i <= bottommost_level; ++i) {
    sorted_runs[i].being_compacted =
        picker->AreFilesInCompaction(vstorage_->LevelFiles(i));
  }
  // if level 0 has any range deletion or map sstable, try to push them down.
  if ((vstorage_->has_range_deletion(0) &&
       (bottommost_level > 0 || vstorage_->LevelFiles(0).size() > 1)) ||
      vstorage_->has_map_sst(0) ||
      int(vstorage_->LevelFiles(0).size()) >=
          mutable_cf_options_.level0_file_num_compaction_trigger) {
    if (!sorted_runs[0].being_compacted && !sorted_runs[1].being_compacted) {
      compaction_inputs_.resize(2);
      compaction_inputs_[0].level = 0;
      compaction_inputs_[0].files = vstorage_->LevelFiles(0);
      compaction_inputs_[1].level = 1;
      compaction_inputs_[1].files = vstorage_->LevelFiles(1);
      start_level_ = 0;
      output_level_ = 1;
      compaction_type_ = CompactionType::kMapCompaction;
      compaction_reason_ = vstorage_->has_range_deletion(0)
                               ? CompactionReason::kRangeDeletion
                               : CompactionReason::kLevelL0FilesNum;
      return GetCompaction();
    }
    sorted_runs[1].skip_composite = true;
  }

  size_t target_file_size_base = mutable_cf_options_.target_file_size_base;
  size_t base_size = mutable_cf_options_.write_buffer_size *
                     ioptions_.min_write_buffer_number_to_merge *
                     mutable_cf_options_.level0_file_num_compaction_trigger;
  auto get_q = [&] {
    int n = vstorage_->num_levels();
    std::vector<double> level_ratio(n * 2);
    auto base_size_real = double(base_size);
    for (int i = 1; i < n; ++i) {
      auto& sr = sorted_runs[i];
      sr.level = i;
      for (auto f : vstorage_->LevelFiles(i)) {
        sr.size += vstorage_->FileSize(f);
        sr.compensated_file_size += f->compensated_file_size;
      }
      level_ratio[i] = sr.size / base_size_real;
      level_ratio[i + n] = sr.compensated_file_size / base_size_real;
    }
    double q1 = std::max<double>(
        std::atan(1) * 4,
        CompactionPicker::GetQ(level_ratio.begin() + 1, level_ratio.begin() + n,
                               n - 1));
    double q2 = std::max<double>(
        std::atan(1) * 4, CompactionPicker::GetQ(level_ratio.begin() + n + 1,
                                                 level_ratio.end(), n - 1));
    return std::pair<double, double>(q1, q2);
  };

  sorted_runs[bottommost_level].being_compacted =
      picker->AreFilesInCompaction(vstorage_->LevelFiles(bottommost_level));
  auto pick_map_compaction = [&](int level, uint64_t pick_size, double q) {
    auto& level_files = vstorage_->LevelFiles(level);
    auto& next_level_files = vstorage_->LevelFiles(level + 1);
    DependenceMap empty_dependence_map;
    ReadOptions options;
    auto create_iter = [&](const FileMetaData* file_metadata,
                           const DependenceMap& depend_map, Arena* arena,
                           TableReader** table_reader_ptr) {
      return picker->table_cache()->NewIterator(
          options, picker->env_options(), *file_metadata, depend_map, nullptr,
          mutable_cf_options_.prefix_extractor.get(), table_reader_ptr, nullptr,
          false, arena, true, -1);
    };
    std::vector<LevelMapRangeSrc> src;
    Arena arena;
    {
      auto calc_estimate_info = [&](const MapSstElement& elem) {
        auto& dependence_map = vstorage_->dependence_map();
        double total_entry_num = 0;
        double total_del_num = 0;
        for (auto& link : elem.link) {
          auto find = dependence_map.find(link.file_number);
          if (find == dependence_map.end()) {
            // TODO log error
            continue;
          }
          auto meta = find->second;
          double ratio = link.size / meta->fd.GetFileSize();
          total_entry_num += meta->prop.num_entries * ratio;
          total_del_num += meta->prop.num_deletions * ratio;
        }
        return std::make_pair(total_entry_num, total_del_num);
      };
      ScopedArenaIterator iter(
          NewMapElementIterator(level_files.data(), level_files.size(),
                                &ioptions_.internal_comparator, &create_iter,
                                c_style_callback(create_iter), &arena));
      if (!iter->status().ok()) {
        return iter->status();
      }
      MapSstElement map_element;
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        if (!CompactionPicker::ReadMapElement(map_element, iter.get(),
                                              log_buffer_, cf_name_)) {
          return Status::Corruption("CompactionPicker bad map element");
        }
        double estimate_entry_num;
        double estimate_del_num;
        std::tie(estimate_entry_num, estimate_del_num) =
            calc_estimate_info(map_element);
        src.emplace_back(map_element, estimate_entry_num, estimate_del_num,
                         &arena);
      }
    }
    if (src.empty()) {
      return Status::OK();
    }
    std::vector<LevelMapRangeDst> dst;
    {
      ScopedArenaIterator iter(NewMapElementIterator(
          next_level_files.data(), next_level_files.size(),
          &ioptions_.internal_comparator, &create_iter,
          c_style_callback(create_iter), &arena));
      if (!iter->status().ok()) {
        return iter->status();
      }
      MapSstElement map_element;
      uint64_t accumulate = 0;
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        if (!CompactionPicker::ReadMapElement(map_element, iter.get(),
                                              log_buffer_, cf_name_)) {
          return Status::Corruption("CompactionPicker bad map element");
        }
        dst.emplace_back(map_element, &arena);
        accumulate += dst.back().estimate_size;
        dst.back().accumulate_estimate_size = accumulate;
      }
    }
    if (dst.empty()) {
      uint64_t src_size = 0;
      for (auto it = src.begin(); it != src.end(); ++it) {
        src_size += it->estimate_size;
        if (src_size >= pick_size) {
          if (++it != src.end()) {
            input_range_.emplace_back(ExtractUserKey(src.front().start),
                                      ExtractUserKey(it->start), true, false);
          } else {
            input_range_.emplace_back(ExtractUserKey(src.front().start),
                                      ExtractUserKey(src.back().limit), true,
                                      true);
          }
          break;
        }
      }
    } else {
      std::vector<LevelMapSection> sections;
      auto m = dst.size();
      auto& kMin = dst.front().start;
      auto& kMax = dst.back().limit;
      auto ic = &ioptions_.internal_comparator;
      for (size_t i = 0, p = 0; i < src.size(); ++i) {
        if (ic->Compare(src[i].limit, kMin) < 0) {
          src[i].start_index = src[i].limit_index = 0;
          continue;
        }
        if (ic->Compare(kMax, src[i].start) < 0) {
          src[i].start_index = src[i].limit_index = m - 1;
          continue;
        }
        while (ic->Compare(dst[p].limit, src[i].start) < 0) {
          p++;
        }
        src[i].start_index = p < m ? p : m - 1;
        while (p < m && ic->Compare(dst[p].limit, src[i].limit) < 0) {
          p++;
        }
        src[i].limit_index = p < m ? p : m - 1;
      }
      auto queue_start = src.begin();
      auto queue_limit = queue_start;
      size_t src_size = queue_start->estimate_size;
      double entry_num = queue_start->estimate_entry_num;
      double del_num = queue_start->estimate_del_num;

      auto fn_new_section = [&] {
        auto overlap_ratio =
            double(src_size) /
            double(src_size +
                   dst[queue_limit->limit_index].accumulate_estimate_size -
                   dst[queue_start->start_index].accumulate_estimate_size +
                   dst[queue_start->start_index].estimate_size + 1);
        auto deletion_ratio = del_num / entry_num;
        sections.emplace_back(LevelMapSection{
            queue_start - src.begin(), queue_limit - src.begin(),
            MixOverlapRatioAndDeletionRatio(overlap_ratio, deletion_ratio)});
      };

      auto fn_step_right = [&] {
        ++queue_limit;
        src_size += queue_limit->estimate_size;
        entry_num += queue_limit->estimate_entry_num;
        del_num += queue_limit->estimate_del_num;
        if (src_size > base_size) {
          fn_new_section();
        }
      };

      auto fn_step_left = [&] {
        src_size -= queue_start->estimate_size;
        entry_num -= queue_start->estimate_entry_num;
        del_num -= queue_start->estimate_del_num;
        ++queue_start;
        if (src_size > target_file_size_base) {
          fn_new_section();
        }
      };

      assert(!src.empty());
      auto queue_end = src.end() - 1;
      do {
        while (src_size < pick_size && queue_limit != queue_end) {
          fn_step_right();
        }
        while (src_size >= target_file_size_base) {
          fn_step_left();
        }
      } while (queue_limit != queue_end);

      if (sections.empty()) {
        queue_start = src.begin();
        queue_limit = src.end() - 1;
        src_size = 0;
        entry_num = 0;
        del_num = 0;
        for (auto& item : src) {
          src_size += item.estimate_size;
          entry_num += item.estimate_entry_num;
          del_num += item.estimate_del_num;
        }
        fn_new_section();
      } else {
        std::sort(sections.begin(), sections.end(), TERARK_CMP(weight, >));
      }
      src_size = 0;
      for (size_t i = 0; i < sections.size(); ++i) {
        for (ptrdiff_t j = sections[i].start_index;
             j <= sections[i].limit_index; ++j) {
          src_size += src[j].estimate_size;
          src[j].estimate_size = 0;
        }
        if (sections[i].limit_index + 1 < ptrdiff_t(src.size())) {
          input_range_.emplace_back(
              ExtractUserKey(src[sections[i].start_index].start),
              ExtractUserKey(src[sections[i].limit_index + 1].start), true,
              false);
        } else {
          input_range_.emplace_back(
              ExtractUserKey(src[sections[i].start_index].start),
              ExtractUserKey(src[sections[i].limit_index].limit), true, true);
        }
        if (src_size >= pick_size) {
          break;
        }
      }
    }
    if (CompactionPicker::FixInputRange(input_range_,
                                        ioptions_.internal_comparator,
                                        true /* sort */, true /* merge */)) {
      compaction_inputs_.resize(2);
      compaction_inputs_[0].level = level;
      compaction_inputs_[0].files = level_files;
      compaction_inputs_[1].level = level + 1;
      compaction_inputs_[1].files = next_level_files;
      output_level_ = level + 1;
      start_level_score_ = q;
      compaction_type_ = kMapCompaction;
      compaction_reason_ = CompactionReason::kLevelMaxLevelSize;
    }
    return Status::OK();
  };
  auto pick_range_deletion = [&](int level) {
    auto& level_files = vstorage_->LevelFiles(level);
    Arena arena;
    DependenceMap empty_dependence_map;
    ReadOptions options;
    auto create_iter = [&](const FileMetaData* file_metadata,
                           const DependenceMap& depend_map, Arena* arena,
                           TableReader** table_reader_ptr) {
      return picker->table_cache()->NewIterator(
          options, picker->env_options(), *file_metadata, depend_map, nullptr,
          mutable_cf_options_.prefix_extractor.get(), table_reader_ptr, nullptr,
          false, arena, true, -1);
    };
    ScopedArenaIterator iter(NewMapElementIterator(
        level_files.data(), level_files.size(), &ioptions_.internal_comparator,
        &create_iter, c_style_callback(create_iter), &arena));
    if (!iter->status().ok()) {
      return iter->status();
    }
    MapSstElement map_element;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      if (!CompactionPicker::ReadMapElement(map_element, iter.get(),
                                            log_buffer_, cf_name_)) {
        return Status::Corruption("CompactionPicker bad map element");
      }
      if (!map_element.has_delete_range) {
        continue;
      }
      input_range_.emplace_back(ExtractUserKey(map_element.smallest_key),
                                ExtractUserKey(map_element.largest_key), true,
                                false);
    }
    if (CompactionPicker::FixInputRange(input_range_,
                                        ioptions_.internal_comparator,
                                        false /* sort */, true /* merge */)) {
      compaction_inputs_.resize(2);
      compaction_inputs_[0].level = level;
      compaction_inputs_[0].files = level_files;
      compaction_inputs_[1].level = level + 1;
      compaction_inputs_[1].files = vstorage_->LevelFiles(level + 1);
      output_level_ = level + 1;
      start_level_score_ = 0;
      compaction_type_ = kMapCompaction;
      compaction_reason_ = CompactionReason::kRangeDeletion;
    }
    return Status::OK();
  };

  for (int i = 1; i < bottommost_level; ++i) {
    if (!vstorage_->has_range_deletion(i)) {
      continue;
    }
    if (sorted_runs[i].being_compacted || sorted_runs[i + 1].being_compacted) {
      sorted_runs[i].skip_composite = true;
      sorted_runs[i + 1].skip_composite = true;
      continue;
    }
    auto s = pick_range_deletion(i);
    if (!s.ok()) {
      ROCKS_LOG_BUFFER(log_buffer_,
                       "[%s] PickCompaction range_deletion error %s.",
                       cf_name_.c_str(), s.getState());
      return nullptr;
    }
    if (!input_range_.empty()) {
      return GetCompaction();
    }
  }
  double level_size = double(base_size);
  double level_compensated_size = double(base_size);
  auto q_pair = get_q();
  for (int i = 1; i < vstorage_->num_levels() - 1; ++i) {
    level_size *= q_pair.first;
    level_compensated_size *= q_pair.second;
    if (vstorage_->LevelFiles(i).empty()) {
      continue;
    }
    double fixed_size = level_size;
    if (i < bottommost_level) {
      fixed_size = std::min(
          fixed_size, double(sorted_runs[i].size + sorted_runs[i + 1].size) /
                          (q_pair.first + 1));
    }
    if (sorted_runs[i].size > fixed_size) {
      if (sorted_runs[i].being_compacted ||
          sorted_runs[i + 1].being_compacted) {
        sorted_runs[i].skip_composite = true;
        sorted_runs[i + 1].skip_composite = true;
        continue;
      }
      uint64_t pick_size = target_file_size_base;
      double diff_size =
          double(sorted_runs[i].size) - fixed_size + target_file_size_base / 2;
      if (diff_size > double(pick_size)) {
        pick_size = uint64_t(diff_size);
      }
      auto s = pick_map_compaction(i, pick_size, q_pair.first);
      if (!s.ok()) {
        ROCKS_LOG_BUFFER(log_buffer_, "[%s] PickCompaction map error %s.",
                         cf_name_.c_str(), s.getState());
        return nullptr;
      }
      if (!input_range_.empty()) {
        return GetCompaction();
      }
    }
    fixed_size = level_compensated_size;
    if (i < bottommost_level) {
      fixed_size = std::min(fixed_size,
                            double(sorted_runs[i].compensated_file_size +
                                   sorted_runs[i + 1].compensated_file_size) /
                                (q_pair.second + 1));
    }
    if (sorted_runs[i].compensated_file_size > fixed_size) {
      if (sorted_runs[i].being_compacted ||
          sorted_runs[i + 1].being_compacted) {
        sorted_runs[i].skip_composite = true;
        sorted_runs[i + 1].skip_composite = true;
        continue;
      }
      auto s = pick_map_compaction(i, target_file_size_base, q_pair.second);
      if (!s.ok()) {
        ROCKS_LOG_BUFFER(log_buffer_, "[%s] PickCompaction map error %s.",
                         cf_name_.c_str(), s.getState());
        return nullptr;
      }
      if (!input_range_.empty()) {
        return GetCompaction();
      }
    }
  }
  sorted_runs.erase(sorted_runs.begin());
  return picker->PickCompositeCompaction(cf_name_, mutable_cf_options_,
                                         vstorage_, snapshots, sorted_runs,
                                         log_buffer_);
}

Compaction* LevelCompactionBuilder::GetCompaction() {
  CompactionParams params(vstorage_, ioptions_, mutable_cf_options_);
  params.inputs = std::move(compaction_inputs_);
  params.output_level = output_level_;
  params.target_file_size = MaxFileSizeForLevel(
      mutable_cf_options_, output_level_, ioptions_.compaction_style,
      vstorage_->base_level(), ioptions_.level_compaction_dynamic_level_bytes);
  params.max_compaction_bytes = mutable_cf_options_.max_compaction_bytes;
  params.output_path_id =
      GetPathId(ioptions_, mutable_cf_options_, output_level_);
  params.compression =
      GetCompressionType(ioptions_, vstorage_, mutable_cf_options_,
                         output_level_, vstorage_->base_level());
  params.compression_opts =
      GetCompressionOptions(ioptions_, vstorage_, output_level_);
  params.grandparents = std::move(grandparents_);
  params.manual_compaction = is_manual_;
  params.score = start_level_score_;
  params.compaction_type = compaction_type_;
  params.input_range = std::move(input_range_);
  params.compaction_reason = compaction_reason_;

  auto c = new Compaction(std::move(params));

  // If it's level 0 compaction, make sure we don't execute any other level 0
  // compactions in parallel
  compaction_picker_->RegisterCompaction(c);

  // Creating a compaction influences the compaction score because the score
  // takes running compactions into account (by skipping files that are
  // already being compacted). Since we just changed compaction score, we
  // recalculate it here
  vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);
  return c;
}

bool LevelCompactionBuilder::PickFileToCompact() {
  // level 0 files are overlapping. So we cannot pick more
  // than one concurrent compactions at this level. This
  // could be made better by looking at key-ranges that are
  // being compacted at level 0.
  if (start_level_ == 0 &&
      !compaction_picker_->level0_compactions_in_progress()->empty()) {
    TEST_SYNC_POINT("LevelCompactionPicker::PickCompactionBySize:0");
    return false;
  }

  start_level_inputs_.clear();

  assert(start_level_ >= 0);

  // Pick the largest file in this level that is not already
  // being compacted
  const std::vector<int>& file_size =
      vstorage_->FilesByCompactionPri(start_level_);
  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(start_level_);

  unsigned int cmp_idx;
  for (cmp_idx = vstorage_->NextCompactionIndex(start_level_);
       cmp_idx < file_size.size(); cmp_idx++) {
    int index = file_size[cmp_idx];
    auto* f = level_files[index];

    // do not pick a file to compact if it is being compacted
    // from n-1 level.
    if (f->being_compacted) {
      continue;
    }

    start_level_inputs_.files.push_back(f);
    start_level_inputs_.level = start_level_;
    if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &start_level_inputs_) ||
        compaction_picker_->FilesRangeOverlapWithCompaction(
            {start_level_inputs_}, output_level_)) {
      // A locked (pending compaction) input-level file was pulled in due to
      // user-key overlap.
      start_level_inputs_.clear();
      continue;
    }

    // Now that input level is fully expanded, we check whether any output
    // files are locked due to pending compaction.
    //
    // Note we rely on ExpandInputsToCleanCut() to tell us whether any output-
    // level files are locked, not just the extra ones pulled in for user-key
    // overlap.
    InternalKey smallest, largest;
    compaction_picker_->GetRange(start_level_inputs_, &smallest, &largest);
    CompactionInputFiles output_level_inputs;
    output_level_inputs.level = output_level_;
    vstorage_->GetOverlappingInputs(output_level_, &smallest, &largest,
                                    &output_level_inputs.files);
    if (!output_level_inputs.empty() &&
        !compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &output_level_inputs)) {
      start_level_inputs_.clear();
      continue;
    }
    base_index_ = index;
    break;
  }

  // store where to start the iteration in the next call to PickCompaction
  vstorage_->SetNextCompactionIndex(start_level_, cmp_idx);

  return start_level_inputs_.size() > 0;
}

bool LevelCompactionBuilder::PickIntraL0Compaction() {
  start_level_inputs_.clear();
  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(0 /* level */);
  if (level_files.size() <
          static_cast<size_t>(
              mutable_cf_options_.level0_file_num_compaction_trigger + 2) ||
      level_files[0]->being_compacted) {
    // If L0 isn't accumulating much files beyond the regular trigger, don't
    // resort to L0->L0 compaction yet.
    return false;
  }
  return FindIntraL0Compaction(level_files, kMinFilesForIntraL0Compaction,
                               port::kMaxUint64, &start_level_inputs_);
}
}  // namespace

Compaction* LevelCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage, const std::vector<SequenceNumber>& snapshots,
    LogBuffer* log_buffer) {
  LevelCompactionBuilder builder(cf_name, vstorage, this, log_buffer,
                                 mutable_cf_options, ioptions_);
  if (ioptions_.enable_lazy_compaction) {
    return builder.PickLazyCompaction(snapshots);
  } else {
    return builder.PickCompaction();
  }
}

}  // namespace rocksdb
