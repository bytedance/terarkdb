#include <deque>
#include <map>

#include "db/db_impl.h"
#include "executor.h"
#include "string_view.hpp"
#include "util/autovector.h"

namespace cheapis {
class ExecutorMemImpl final : public Executor {
 private:
  struct Task {
    rocksdb::autovector<std::string> argv;
    Client* c;
    int fd;
  };

 public:
  explicit ExecutorMemImpl(rocksdb::DBImpl* db) : db_(db) {}

  ~ExecutorMemImpl() override = default;

  void Submit(const rocksdb::autovector<nonstd::string_view>& argv, Client* c,
              int fd) override {
    tasks_.emplace_back();
    Task& task = tasks_.back();
    for (const auto& arg : argv) {
      task.argv.emplace_back(arg);
    }
    task.c = c;
    task.fd = fd;
  }

  void Execute(size_t n, long /* curr_time */, EventLoop<Client>* el) override {
    for (size_t i = 0; i < n; tasks_.pop_front(), ++i) {
      Task& task = tasks_.front();
      Client* c = task.c;
      int fd = task.fd;

      --c->ref_count;
      if (c->close) {
        if (c->ref_count == 0) {
          el->Release(fd);
        }
        continue;
      }

      bool blocked = !c->output.empty();
      auto& argv = task.argv;
      if (argv[0] == "GET" && argv.size() == 2) {
        auto it = map_.find(argv[1]);
        if (it != map_.cend()) {
          RespMachine::AppendBulkString(&c->output, it->second);
        } else {
          RespMachine::AppendNullArray(&c->output);
        }
      } else if (argv[0] == "SET" && argv.size() == 3) {
        map_.emplace(std::move(argv[1]), std::move(argv[2]));
        RespMachine::AppendSimpleString(&c->output, "OK");
      } else if (argv[0] == "DEL" && argv.size() == 2) {
        map_.erase(argv[1]);
        RespMachine::AppendSimpleString(&c->output, "OK");
      } else if (argv[0] == "TERARKDB_OPS_FULL_COMPACT" && argv.size() == 1) {
        rocksdb::CompactRangeOptions cro{};
        cro.exclusive_manual_compaction = false;
        auto s = db_->CompactRange(cro, nullptr, nullptr);
        if (s.ok()) {
          RespMachine::AppendSimpleString(&c->output, "OK");
        } else {
          RespMachine::AppendError(
              &c->output,
              "Cannot do full compaction. Error message: " + s.ToString());
        }
      } else if (argv[0] == "PING" && argv.size() == 1) {
        RespMachine::AppendSimpleString(&c->output, "PONG");
      } else {
        RespMachine::AppendError(&c->output, "Unsupported Command");
      }

      if (!blocked) {
        ssize_t nwrite = write(fd, c->output.data(), c->output.size());
        if (nwrite > 0) {
          c->output.assign(c->output.data() + nwrite,
                           c->output.size() - nwrite);
        }
        if (!c->output.empty()) {
          el->AddEvent(fd, kWritable);
        }
      }
    }
  }

  size_t GetTaskCount() const override { return tasks_.size(); }

 private:
  std::deque<Task> tasks_;
  std::map<std::string, std::string> map_;
  rocksdb::DBImpl* db_;
};

std::unique_ptr<Executor> OpenExecutorMem(rocksdb::DBImpl* db) {
  return std::make_unique<ExecutorMemImpl>(db);
}
}  // namespace cheapis