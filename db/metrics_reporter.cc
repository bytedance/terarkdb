#include "rocksdb/metrics_reporter.h"

#include <inttypes.h>

#include <chrono>
#include <cstddef>
#include <string>

#include "util/logging.h"

#define REPORT_DEBUG_STACKTRACE 1
#if REPORT_DEBUG_STACKTRACE
#include <boost/stacktrace.hpp>
#endif

namespace TERARKDB_NAMESPACE {

LatencyHistLoggedGuard::LatencyHistLoggedGuard(HistReporterHandle* handle,
                                               unsigned int threshold_us,
                                               Env* env)
    : handle_(handle),
      env_(handle_ == nullptr ? nullptr
                              : env == nullptr ? Env::Default() : env),
      begin_time_(env_ == nullptr ? 0 : env_->NowMicros()),
      log_threshold_us_(threshold_us) {
#if REPORT_DEBUG_STACKTRACE
  auto stacktrace = new boost::stacktrace::stacktrace();
  start_stacktrace_ = stacktrace;
#endif
}

LatencyHistLoggedGuard::~LatencyHistLoggedGuard() {
  if (handle_ != nullptr) {
    auto us = (env_->NowMicros() - begin_time_);
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
          boost::stacktrace::to_string(boost::stacktrace::stacktrace())
              .c_str());
#else
      ROCKS_LOG_WARN(handle_->GetLogger(),
                     "[name:%s] [tags:%s]: %" PRIu64 "us\n", handle_->GetName(),
                     handle_->GetTag(), static_cast<uint64_t>(us));
#endif
    }
  }
#if REPORT_DEBUG_STACKTRACE
  auto stacktrace =
      static_cast<boost::stacktrace::stacktrace*>(start_stacktrace_);
  start_stacktrace_ = nullptr;
  delete stacktrace;
#endif
}

}  // namespace TERARKDB_NAMESPACE