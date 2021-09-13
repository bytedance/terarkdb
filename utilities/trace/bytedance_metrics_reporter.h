#include <atomic>
#include <deque>

#include "rocksdb/env.h"
#include "rocksdb/metrics_reporter.h"
#include "rocksdb/terark_namespace.h"
#include "stats.h"
namespace TERARKDB_NAMESPACE {
class ByteDanceMetricsReporterFactory : public MetricsReporterFactory {
 public:
  ByteDanceMetricsReporterFactory();

  explicit ByteDanceMetricsReporterFactory(const std::string& ns);

  ~ByteDanceMetricsReporterFactory() override = default;

 public:
  HistReporterHandle* BuildHistReporter(const std::string& name,
                                        const std::string& tags, Logger* logger,
                                        Env* const env) override;

  CountReporterHandle* BuildCountReporter(const std::string& name,
                                          const std::string& tags,
                                          Logger* logger,
                                          Env* const env) override;

 private:
#ifdef WITH_BYTEDANCE_METRICS
  std::deque<ByteDanceHistReporterHandle> hist_reporters_;
  std::deque<ByteDanceCountReporterHandle> count_reporters_;
#endif

  void InitNamespace(const std::string& ns);
};
}  // namespace TERARKDB_NAMESPACE