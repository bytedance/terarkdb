#include "rocksdb/metrics_reporter.h"

#include <inttypes.h>

#include <cassert>
#include <cstddef>
#include <string>

#include "rocksdb/env.h"
#include "util/logging.h"

#ifdef WITH_BOOSTLIB
#define REPORT_DEBUG_STACKTRACE 1
#if REPORT_DEBUG_STACKTRACE
#include <boost/stacktrace.hpp>
#endif
#endif

namespace TERARKDB_NAMESPACE {

LatencyHistGuard::LatencyHistGuard(HistReporterHandle* handle)
    : handle_(handle), begin_time_ns_(handle_->GetEnv()->NowNanos()) {
  assert(handle_ != nullptr);
}

LatencyHistGuard::~LatencyHistGuard() {
  auto us = (handle_->GetEnv()->NowNanos() - begin_time_ns_) / 1000;
  handle_->AddRecord(us);
}

LatencyHistLoggedGuard::LatencyHistLoggedGuard(HistReporterHandle* handle,
                                               uint64_t threshold_us)
    : handle_(handle),
      begin_time_ns_(handle_->GetEnv()->NowNanos()),
      log_threshold_us_(threshold_us) {
  assert(handle_ != nullptr);
#if REPORT_DEBUG_STACKTRACE
  auto stacktrace = new boost::stacktrace::stacktrace();
  start_stacktrace_ = stacktrace;
#endif
}

LatencyHistLoggedGuard::~LatencyHistLoggedGuard() {
  auto us = (handle_->GetEnv()->NowNanos() - begin_time_ns_) / 1000;
  handle_->AddRecord(us);
  if (us >= log_threshold_us_ && handle_->GetLogger() != nullptr) {
#if REPORT_DEBUG_STACKTRACE
    auto stacktrace =
        static_cast<boost::stacktrace::stacktrace*>(start_stacktrace_);
    ROCKS_LOG_WARN(
        handle_->GetLogger(),
        "[name:%s] [tags:%s]: %" PRIu64 "us\n%s----------\n%s-----------\n",
        handle_->GetName(), handle_->GetTag(), us,
        boost::stacktrace::to_string(*stacktrace).c_str(),
        boost::stacktrace::to_string(boost::stacktrace::stacktrace()).c_str());
#else
    ROCKS_LOG_WARN(handle_->GetLogger(), "[name:%s] [tags:%s]: %" PRIu64 "us\n",
                   handle_->GetName(), handle_->GetTag(),
                   static_cast<uint64_t>(us));
#endif
  }

#if REPORT_DEBUG_STACKTRACE
  auto stacktrace =
      static_cast<boost::stacktrace::stacktrace*>(start_stacktrace_);
  start_stacktrace_ = nullptr;
  delete stacktrace;
#endif
}

CurriedMetricsReporterFactory::CurriedMetricsReporterFactory(
    std::shared_ptr<MetricsReporterFactory> factory, Logger* logger,
    Env* const env)
    : factory_(std::move(factory)), logger_(logger), env_(env) {
  assert(factory_ != nullptr);
  assert(logger_ != nullptr);
  assert(env_ != nullptr);
}

}  // namespace TERARKDB_NAMESPACE