#include "rocksdb/metrics_reporter.h"

#include <atomic>

#include "port/stack_trace.h"
#include "rocksdb/env.h"
#include "rocksdb/terark_namespace.h"
#include "util/logging.h"
#include "util/mock_time_env.h"
#include "util/testharness.h"

namespace TERARKDB_NAMESPACE {

class TestLogger : public Logger {
 public:
  TestLogger(int* count) : count_(count) {}

  using Logger::Logv;
  virtual void Logv(const char* /*format*/, va_list /*ap*/) override {
    ++*count_;
  };

 private:
  int* count_;
};
class TestHistReporterHandle : public HistReporterHandle {
 public:
  TestHistReporterHandle(Env* const env)
      : name_("name"), tags_("tags"), count(0), logger_(&count), env_(env) {}

  ~TestHistReporterHandle() {}

 public:
  void AddRecord(size_t val) override { stat_.push_back(val); }

  const char* GetName() override { return name_.c_str(); }
  const char* GetTag() override { return tags_.c_str(); }
  Logger* GetLogger() override { return &logger_; }
  Env* GetEnv() override { return env_; }

  int LoggerCount() { return count; }

 private:
  const std::string name_;
  const std::string tags_;
  int count;
  TestLogger logger_;
  Env* env_;
  std::vector<size_t> stat_;
};

class MockMetricsReporterTest : public testing::Test {
 public:
  MockMetricsReporterTest() : env_(Env::Default()), handler_(&env_) {}

 protected:
  MockTimeEnv env_;
  TestHistReporterHandle handler_;
};

TEST_F(MockMetricsReporterTest, Basic) {
  ASSERT_EQ(handler_.LoggerCount(), 0);
  {
    LatencyHistLoggedGuard g(&handler_, 100);
    env_.MockSleepForMicroseconds(100);
  }
  ASSERT_EQ(handler_.LoggerCount(), 1);
  {
    LatencyHistLoggedGuard g(&handler_, 100);
    env_.MockSleepForMicroseconds(99);
  }
  ASSERT_EQ(handler_.LoggerCount(), 1);
}
}  // namespace TERARKDB_NAMESPACE
int main(int argc, char** argv) {
  TERARKDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}