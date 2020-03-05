#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <numeric>
#include <vector>

namespace rocksdb {
template <size_t MAX_LATENCY_US_FAST = 10 * 1000>  // 10ms
class HistStats {
 public:
  HistStats() : last_report_time(std::chrono::high_resolution_clock::now()) {}

  void AppendRecord(size_t us) {
    if (us < buckets_.size()) {
      ++buckets_[us];
    } else {
      large_nums_.emplace_back(us);
    }
  }

  template <size_t N>
  auto GetResult(const double (&percentiles)[N]) {
    double total = std::accumulate(buckets_.cbegin(), buckets_.cend(), 0) +
                   large_nums_.size();

    std::array<size_t, N + 2> result{};
    size_t idx = 0;
    size_t accum = 0;
    size_t total_tm = 0;
    size_t& avg = result[N];
    size_t& max = result[N + 1];

    for (size_t d = 0; d < buckets_.size(); ++d) {
      size_t c = buckets_[d];
      if (c) {
        accum += c;
        total_tm += d * c;
        if (idx < N && accum / total >= percentiles[idx]) {
          result[idx++] = d;
        }
        max = d;
      }
    }

    if (!large_nums_.empty()) {
      std::sort(large_nums_.begin(), large_nums_.end());
      for (size_t i = 0; i < large_nums_.size(); ++i) {
        ++accum;
        total_tm += large_nums_[i];
        if (idx < N && accum / total >= percentiles[idx]) {
          result[idx++] = large_nums_[i];
        }
      }
      max = large_nums_.back();
    }
    avg = total_tm / accum;
    return result;
  }

  void Merge(const HistStats& another) {
    large_nums_.insert(large_nums_.end(), another.large_nums_.begin(),
                       another.large_nums_.end());
    for (size_t d = 0; d < another.buckets_.size(); ++d) {
      size_t c = another.buckets_[d];
      buckets_[d] += c;
    }
  }

  void Reset() {
    buckets_.fill(0);
    if (large_nums_.size() > MAX_LATENCY_US_FAST) {
      large_nums_.resize(0);
      large_nums_.shrink_to_fit();
    }
  }

  std::chrono::high_resolution_clock::time_point last_report_time;

 private:
  std::array<size_t, MAX_LATENCY_US_FAST> buckets_{};
  std::vector<size_t> large_nums_;
};
}  // namespace rocksdb