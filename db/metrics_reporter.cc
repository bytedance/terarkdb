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
#if defined(__APPLE__)
#define _GNU_SOURCE
#endif
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

namespace {
// Do nothing
class LoggerForDummyReporter : public Logger {
 public:
  LoggerForDummyReporter() : Logger(InfoLogLevel::HEADER_LEVEL) {}

  void LogHeader(const char* /*format*/, va_list /*ap*/) override {}
  void Logv(const char* /*format*/, va_list /*ap*/) override {}
  void Logv(const InfoLogLevel /*log_level*/, const char* /*format*/,
            va_list /*ap*/) override {}
};
// Ignore NowMicros & NowNanos
class EnvForDummyReporter : public EnvWrapper {
 public:
  EnvForDummyReporter() : EnvWrapper(Env::Default()) {}

  uint64_t NowMicros() override { return 0; }
  uint64_t NowNanos() override { return 0; }
};
static EnvForDummyReporter dummy_env_;
static LoggerForDummyReporter dummy_logger_;

class DummyHistReporterHandleImpl : public HistReporterHandle {
  const char* GetName() override { return ""; }
  const char* GetTag() override { return ""; }
  Logger* GetLogger() override { return &dummy_logger_; }
  Env* GetEnv() override { return &dummy_env_; }
  void AddRecord(size_t /*val*/) override {}
};

class DummyCountReporterHandleImpl : public CountReporterHandle {
  void AddCount(size_t /*val*/) override {}
};

static DummyHistReporterHandleImpl dummy_hist_;
static DummyCountReporterHandleImpl dummy_count_;
}  // namespace

HistReporterHandle* DummyHistReporterHandle() { return &dummy_hist_; };
CountReporterHandle* DummyCountReporterHandle() { return &dummy_count_; };

}  // namespace TERARKDB_NAMESPACE
