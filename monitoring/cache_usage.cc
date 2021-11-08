
#include "rocksdb/terark_namespace.h"

#ifdef WITH_DIAGNOSE_CACHE
#include "util/chash_map.h"
#include "util/chash_set.h"
#include <cassert>
#include <mutex>
#include <vector>

#endif



namespace TERARKDB_NAMESPACE {

#ifdef WITH_DIAGNOSE_CACHE

struct CacheUsageCollector {
  CacheUsageCollector(bool _is_tls = true);
  ~CacheUsageCollector();
  chash_map<uint64_t, ssize_t> usage;
  bool is_tls;
  std::mutex mutex;
};

static std::mutex g_cache_usage_mutex;
static chash_set<CacheUsageCollector*> g_cache_usage_collector_set;
static CacheUsageCollector g_cache_usage_collector(false);
static thread_local CacheUsageCollector cache_usage_collector;

CacheUsageCollector::CacheUsageCollector(bool _is_tls) : is_tls(_is_tls) {
  if (is_tls) {
    g_cache_usage_collector_set.emplace(this);
  }
}
CacheUsageCollector::~CacheUsageCollector() {
  if (is_tls) {
    std::unique_lock<std::mutex> l(g_cache_usage_mutex);
    for (auto pair : usage) {
      auto ib = g_cache_usage_collector.usage.emplace(pair.first, 0);
      ib.first->second += pair.second;
      if (ib.first->second == 0) {
        g_cache_usage_collector.usage.erase(ib.first);
      }
    }
  }
  g_cache_usage_collector_set.erase(this);
}

void CollectCacheUsage(uint64_t file_number, ssize_t charge) {
  auto& collector = cache_usage_collector;
  std::unique_lock<std::mutex> l(collector.mutex);
  auto ib = collector.usage.emplace(file_number, 0);
  ib.first->second += charge;
  if (ib.first->second == 0) {
    collector.usage.erase(ib.first);
  }
}

void GetCacheUsage(std::vector<std::pair<uint64_t, size_t>>* usage) {
  usage->clear();
  std::unique_lock<std::mutex> l(g_cache_usage_mutex);
  for (auto& collector : g_cache_usage_collector_set) {
    std::unique_lock<std::mutex> l(collector->mutex);
    for (auto pair : collector->usage) {
      auto ib = g_cache_usage_collector.usage.emplace(pair.first, 0);
      ib.first->second += pair.second;
      if (ib.first->second == 0) {
        g_cache_usage_collector.usage.erase(ib.first);
      }
    }
    collector->usage.clear();
  }
  usage->reserve(g_cache_usage_collector.usage.size());
  for (auto pair : g_cache_usage_collector.usage) {
    assert(pair.second > 0);
    usage->emplace_back(pair.first, size_t(pair.second));
  }
}
#else

void CollectCacheUsage(uint64_t /*file_number*/, ssize_t /*charge*/) {}
void GetCacheUsage(std::vector<std::pair<uint64_t, size_t>>* usage) {
  usage->clear();
}
#endif

}  // namespace TERARKDB_NAMESPACE