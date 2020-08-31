#include "server.h"

#include <cerrno>
#include <cstring>

#include "anet.h"
#include "db/db_impl.h"
#include "executor.h"
#include "rocksdb/env.h"
#include "util/logging.h"

#if __clang__
#pragma clang diagnostic ignored "-Wunused-const-variable"
#endif
#ifdef __GNUC__
#pragma GCC diagnostic ignored "-Wunused-const-variable"
#endif

namespace cheapis {
using namespace rocksdb;

constexpr char kBindAddr[] = "0.0.0.0";
constexpr unsigned int kPort = 6379;
constexpr unsigned int kBacklog = 511;
constexpr unsigned int kCronInterval = 1;
constexpr unsigned int kMaxAcceptPerCall = 1000;
constexpr unsigned int kNetIPLength = 46;
constexpr unsigned int kTCPKeepAlive = 300;
constexpr unsigned int kTimeout = 360;
constexpr unsigned int kReadLength = 4096;
constexpr unsigned int kMaxInputBuffer = 10485760;
constexpr unsigned int kUnixSocketPerm = 700;

static void ReleaseOrMarkClient(int fd, Client *c, EventLoop<Client> *el) {
  if (c->ref_count == 0) {
    el->Release(fd);
  } else {
    c->close = true;
    el->DelEvent(fd, kReadable | kWritable);
  }
}

static void ReadFromClient(int fd, Client *c, long curr_time,
                           Executor *executor, EventLoop<Client> *el,
                           Logger *log) {
  char buf[kReadLength];
  ssize_t nread = read(fd, buf, kReadLength);
  if (nread == -1) {
    if (errno != EAGAIN) {
      ReleaseOrMarkClient(fd, c, el);
      ROCKS_LOG_WARN(log, "Failed reading. Error message: '%s'",
                     strerror(errno));
    }
    return;
  } else if (nread == 0) {
    ReleaseOrMarkClient(fd, c, el);
    ROCKS_LOG_DEBUG(log, "Client closed connection");
    return;
  }

  std::string &in = c->input;
  in.append(buf, static_cast<size_t>(nread));
  if (in.size() > kMaxInputBuffer) {
    ReleaseOrMarkClient(fd, c, el);
    ROCKS_LOG_WARN(log, "Client reached max input buffer length");
    return;
  }
  c->last_mod_time = curr_time;

  while (c->consume_len < in.size()) {
    size_t consume_len =
        c->resp.Input(in.data() + c->consume_len, in.size() - c->consume_len);
    c->consume_len += consume_len;

    auto state = c->resp.GetState();
    switch (state) {
      case RespMachine::kSuccess: {
        assert(consume_len != 0);
        executor->Submit(c->resp.GetArgv(), c, fd);
        ++c->ref_count;

        c->resp.Reset();
        in.assign(&in[c->consume_len], &in[in.size()]);
        c->consume_len = 0;
        break;
      }

      case RespMachine::kProcess: {
        return;
      }

      case RespMachine::kInit:
      default: {  // error
        ReleaseOrMarkClient(fd, c, el);
        ROCKS_LOG_WARN(log, "Failed parsing. Error state: %d", state);
        return;
      }
    }
  }
}

static void WriteToClient(int fd, Client *c, long curr_time,
                          EventLoop<Client> *el, Logger *log) {
  std::string &out = c->output;
  assert(!out.empty());
  ssize_t nwrite = write(fd, out.data(), out.size());
  if (nwrite <= 0) {
    if (nwrite == -1 && errno != EAGAIN) {
      ReleaseOrMarkClient(fd, c, el);
      ROCKS_LOG_WARN(log, "Failed writing. Error message: '%s'",
                     strerror(errno));
    }
    return;
  }
  c->last_mod_time = curr_time;

  out.assign(out.data() + nwrite, out.size() - nwrite);
  if (out.empty()) {
    el->DelEvent(fd, kWritable);
  }
}

static void ExecuteTasks(Executor *executor, long curr_time,
                         EventLoop<Client> *el) {
  size_t plan = (executor->GetTaskCount() + 1) / 2;
  executor->Execute(plan, curr_time, el);
}

static void ServerCron(int64_t *last_cron_time, int64_t curr_time,
                       EventLoop<Client> *el, Logger *log) {
  if (curr_time - *last_cron_time >= kCronInterval) {
    *last_cron_time = curr_time;

    auto &clients = el->GetResources();
    for (int i = 0; i <= el->GetMaxFD(); ++i) {
      auto &client = clients[i];
      if (client != nullptr && client->last_mod_time != -1) {
        assert(!client->close || client->ref_count);
        if (curr_time - client->last_mod_time > kTimeout) {
          ReleaseOrMarkClient(i, client.get(), el);
          ROCKS_LOG_DEBUG(log, "Client timed out");
        }
      }
    }
  }
}

int ServerMain(ServerRunner *runner, rocksdb::DBImpl *db,
               const std::string &path, Env *env, Logger *log) {
#ifdef TERARKDB_ENABLE_CONSOLE
  const int el_fd = EventLoop<Client>::Open();
  if (el_fd < 0) {
    ROCKS_LOG_ERROR(log, "Failed creating the event loop. Error message: '%s'",
                    strerror(errno));
    return 1;
  }
  EventLoop<Client> el(el_fd);

  auto executor = OpenExecutorMem(db);
  if (executor == nullptr) {
    ROCKS_LOG_ERROR(log, "Failed creating the executor");
    return 1;
  }

  char err[ANET_ERR_LEN];
  int ac_fd;
  if (path.empty()) {  // currently, it's just for debug
    ac_fd = anetTcpServer(err, kPort, const_cast<char *>(kBindAddr), kBacklog);
    if (ac_fd < 0) {
      ROCKS_LOG_ERROR(
          log, "Failed creating the TCP server. Error message: '%s'", err);
      return 1;
    }
  } else {
    std::string sock_path = path + "/CONSOLE";
    unlink(sock_path.c_str()); /* don't care if this fails */
    ac_fd = anetUnixServer(err, (char *)sock_path.c_str(), kUnixSocketPerm,
                           kBacklog);
    if (ac_fd < 0) {
      ROCKS_LOG_ERROR(
          log, "Failed creating the Unix socket server. Error message: '%s'",
          err);
      return 1;
    }
  }
  anetNonBlock(nullptr, ac_fd);

  int r = el.Acquire(ac_fd, std::make_unique<Client>());
  if (r != 0) {
    ROCKS_LOG_ERROR(log, "Failed acquiring the acceptor's fd");
    return 1;
  }

  r = el.AddEvent(ac_fd, kReadable);
  if (r != 0) {
    ROCKS_LOG_ERROR(
        log, "Failed adding the acceptor's readable event. Error message: '%s'",
        strerror(errno));
    return 1;
  }

  int64_t last_cron_time = 0;
  auto status = env->GetCurrentTime(&last_cron_time);
  if (!status.ok()) {
    ROCKS_LOG_ERROR(log, "Failed getting time. Error message: '%s'",
                    status.ToString().c_str());
  }

  struct timeval tv = {0};
  while (true) {
    if (runner->closing_) {
      runner->closed_ = true;
      return 0;
    }

    tv.tv_sec = 0;
    tv.tv_usec = 1000;
    r = el.Poll(&tv);
    if (r < 0) {
      ROCKS_LOG_ERROR(log, "Failed polling. Error message: '%s'",
                      strerror(errno));
      return 1;
    }

    int64_t curr_time = 0;
    status = env->GetCurrentTime(&curr_time);
    if (!status.ok()) {
      ROCKS_LOG_ERROR(log, "Failed getting time. Error message: '%s'",
                      status.ToString().c_str());
    }

    const auto &events = el.GetEvents();
    for (int i = 0; i < r; ++i) {
      const auto &event = events[i];

      int efd = EventLoop<Client>::GetEventFD(event);
      if (efd == ac_fd) {  // acceptor
        int cport, cfd, max = kMaxAcceptPerCall;
        char cip[kNetIPLength];

        while (max--) {
          cfd = anetTcpAccept(err, ac_fd, cip, sizeof(cip), &cport);
          if (cfd < 0) {
            if (errno != EAGAIN) {
              ROCKS_LOG_WARN(log, "Failed accepting. Error message: '%s'", err);
            }
            break;
          }

          r = el.Acquire(cfd, std::make_unique<Client>(curr_time));
          if (r != 0) {
            close(cfd);
            ROCKS_LOG_WARN(log, "Failed acquiring the client's fd");
            break;
          }
          r = el.AddEvent(cfd, kReadable);
          if (r != 0) {
            el.Release(cfd);
            ROCKS_LOG_WARN(
                log,
                "Failed adding the client's readable event. Error message: "
                "'%s'",
                strerror(errno));
            break;
          }
          anetNonBlock(nullptr, cfd);
          anetEnableTcpNoDelay(nullptr, cfd);
          anetKeepAlive(nullptr, cfd, kTCPKeepAlive);
          ROCKS_LOG_DEBUG(log, "Accepted %s:%d", cip, cport);
        }
      } else {  // processor
        auto &client = el.GetResource(efd);
        if (EventLoop<Client>::IsEventReadable(event)) {
          ReadFromClient(efd, client.get(), curr_time, executor.get(), &el,
                         log);
        }
        if (EventLoop<Client>::IsEventWritable(event) && client != nullptr) {
          WriteToClient(efd, client.get(), curr_time, &el, log);
        }
      }
    }

    ExecuteTasks(executor.get(), curr_time, &el);
    ServerCron(&last_cron_time, curr_time, &el, log);
  }
#else
  (void)runner;
  (void)db;
  (void)path;
  (void)env;
  (void)log;
  (void)ServerCron;
  (void)ExecuteTasks;
  (void)WriteToClient;
  (void)ReadFromClient;
  runner->closed_ = true;
  return 0;
#endif
}
}  // namespace cheapis
