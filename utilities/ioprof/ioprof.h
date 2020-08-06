
#include <list>
#include <memory>
#include <string>

// Author: Haiping Zhao (ByteDance)
// Date: July 30, 2020

///////////////////////////////////////////////////////////////////////////////////////////////////

using int64 = long long;

namespace IOProfiler {

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // what data to collect

  struct Data {          // Explained: https://www.linuxprogrammingblog.com/io-profiling
    int64 rchar;         // Counters of data read and written
    int64 wchar;
    int64 syscr;         // Counters for number of I/O operation
    int64 syscw;
    int64 read_bytes;    // Counts data that actually needed to be read from the medium to
                         // satisfy the process' read request. So if the read data were in
                         // the page cache it's not counted here.
    int64 write_bytes;   // Counts data that caused a page to be dirty. It's similar to
                         // read_bytes but due to the nature of caching writes it can't be
                         // described so simply.
    int64 cancelled_write_bytes;

    int64 walltime;      // clock_gettime(CLOCK_MONOTONIC), in ns

    bool error;          // some error occurred while collecting

    Data operator-(const Data &another) const;
  };

  class Collector {
  public:
    virtual ~Collector() {}
    virtual Data Collect() = 0;
  };

  class LocalDiskCollector : public Collector {
  public:
    virtual Data Collect() override;
  };

  class Reporter {
  public:
    virtual ~Reporter() {}
    virtual void Report(const std::string &fullname, const Data &data) = 0;
  };

  class ConsoleReporter : public Reporter {
  public:
    virtual void Report(const std::string &fullname, const Data &data) override;
  };

  /////////////////////////////////////////////////////////////////////////////////////////////////

  class Profiler {
  public:
    static Profiler &TheOne();

  public:
    void Initialize(const std::string &name,
		    const std::shared_ptr<Collector> &collector,
		    const std::shared_ptr<Reporter> &reporter);

    std::string GetName() const { return m_name; }
    std::shared_ptr<Collector> GetCollector() { return m_collector; }
    std::shared_ptr<Reporter> GetReporter() { return m_reporter; }

    std::string GetScope() const;
    void PushScope(const std::string &name);
    void PopScope();

  private:
    std::string m_name;
    std::shared_ptr<Collector> m_collector;
    std::shared_ptr<Reporter> m_reporter;
    std::list<std::string> m_stack;

    Profiler();
    ~Profiler();
  };

  class Scope {
  public:
    Scope(const std::string &name);
    ~Scope() { End();}
    void End();

  private:
    std::string m_name;
    Data m_start;
  };
}