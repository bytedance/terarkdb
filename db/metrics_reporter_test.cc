#include "rocksdb/metrics_reporter.h"

#include <atomic>
#include <chrono>
#include <list>

#include "env/mock_env.h"
#include "port/stack_trace.h"
#include "rocksdb/env.h"
#include "rocksdb/terark_namespace.h"
#include "util/testharness.h"

namespace TERARKDB_NAMESPACE {

class TestLogger : public Logger {
 public:
  using Logger::Logv;
  virtual void Logv(const char* /*format*/, va_list /*ap*/) override {
    ++count_;
  };
  int Count() const { return count_; }

 private:
  int count_ = 0;
};
class TestHistReporterHandle : public HistReporterHandle {
 public:
  TestHistReporterHandle()
      : name_("name"), tags_("tags"), log_(new TestLogger) {}

  ~TestHistReporterHandle() { delete log_; }

 public:
  void AddRecord(size_t val) override { stat_.push_back(val); }

  Logger* GetLogger() override { return log_; }
  const char* GetTag() { return tags_.c_str(); }
  const char* GetName() { return name_.c_str(); }

 private:
  const std::string name_;
  const std::string tags_;
  Logger* log_;
  std::vector<size_t> stat_;
};

class MetricsReporterTest : public testing::Test {
 public:
  MetricsReporterTest() {
    log_ = reinterpret_cast<TestLogger*>(handler_.GetLogger());
  }

 protected:
  TestHistReporterHandle handler_ = TestHistReporterHandle();
  TestLogger* log_;
};

TEST_F(MetricsReporterTest, Basic) {
  MockEnv* env = new MockEnv(Env::Default());
  { LatencyHistLoggedGuard g(nullptr, 100, env); }
  ASSERT_EQ(log_->Count(), 0);
  {
    LatencyHistLoggedGuard g(&handler_, 100, env);
    env->FakeSleepForMicroseconds(100);
  }
  ASSERT_EQ(log_->Count(), 1);
  {
    LatencyHistLoggedGuard g(&handler_, 100, env);
    env->FakeSleepForMicroseconds(90);
  }
  ASSERT_EQ(log_->Count(), 1);
  {
    LatencyHistLoggedGuard g(&handler_, 100, nullptr);
    env->FakeSleepForMicroseconds(100);
  }
  ASSERT_EQ(log_->Count(), 1);
  {
    LatencyHistLoggedGuard g(&handler_, 100, nullptr);
    env->SleepForMicroseconds(100);
  }
  ASSERT_EQ(log_->Count(), 2);
}
}  // namespace TERARKDB_NAMESPACE
int main(int argc, char** argv) {
  TERARKDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}