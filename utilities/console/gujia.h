#pragma once
#ifndef GUJIA_GUJIA_H
#define GUJIA_GUJIA_H

#if defined(OS_LINUX)
#include <sys/epoll.h>

#define GUJIA_HAS_EPOLL

namespace gujia {
typedef struct epoll_event Event;
}
#elif defined(OS_MACOSX)
#include <sys/event.h>
#include <sys/time.h>
#include <sys/types.h>

#define GUJIA_HAS_KQUEUE

namespace gujia {
typedef struct kevent Event;
}
#endif

#include <unistd.h>
#include <array>
#include <cassert>
#include <memory>

namespace gujia {
constexpr size_t kDefaultSize = 1000;

enum {
  kNone = 0,
  kReadable = 1 << 0,
  kWritable = 1 << 1,
};

template <typename T, size_t SIZE = kDefaultSize>
class EventLoop {
 public:
  explicit EventLoop(int el_fd) : el_fd_(el_fd) {}

  ~EventLoop();

  int AddEvent(int fd, int mask);

  int DelEvent(int fd, int mask);

  int Poll(const struct timeval* tvp);

  const std::array<Event, SIZE>& GetEvents() const { return events_; }

  static int GetEventFD(const Event& e);

  static bool IsEventReadable(const Event& e);

  static bool IsEventWritable(const Event& e);

  static int Open();

 public:
  int Acquire(int fd, std::unique_ptr<T>&& resource);

  int Release(int fd);

  int GetMaxFD() const { return max_fd_; }

  std::unique_ptr<T>& GetResource(int fd) { return resources_[fd]; }

  std::array<std::unique_ptr<T>, SIZE>& GetResources() { return resources_; }

 private:
  int el_fd_;
  int max_fd_ = -1;
  std::array<Event, SIZE> events_;
  std::array<std::unique_ptr<T>, SIZE> resources_;

#if defined(GUJIA_HAS_EPOLL)
  std::array<int, SIZE> masks_{};
#endif
};
}  // namespace gujia

#endif  // GUJIA_GUJIA_H