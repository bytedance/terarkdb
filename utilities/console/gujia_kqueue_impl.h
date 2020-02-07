#pragma once
#ifndef GUJIA_GUJIA_KQUEUE_IMPL_H
#define GUJIA_GUJIA_KQUEUE_IMPL_H

#include "gujia.h"

#if defined(GUJIA_HAS_KQUEUE)
namespace gujia {
template <typename T, size_t SIZE>
int EventLoop<T, SIZE>::AddEvent(int fd, int mask) {
  struct kevent ke;
  if (mask & kReadable) {
    EV_SET(&ke, fd, EVFILT_READ, EV_ADD, 0, 0, nullptr);
    if (kevent(el_fd_, &ke, 1, nullptr, 0, nullptr) == -1) {
      return -1;
    }
  }
  if (mask & kWritable) {
    EV_SET(&ke, fd, EVFILT_WRITE, EV_ADD, 0, 0, nullptr);
    if (kevent(el_fd_, &ke, 1, nullptr, 0, nullptr) == -1) {
      return -1;
    }
  }
  return 0;
}

template <typename T, size_t SIZE>
int EventLoop<T, SIZE>::DelEvent(int fd, int mask) {
  struct kevent ke;
  if (mask & kReadable) {
    EV_SET(&ke, fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    kevent(el_fd_, &ke, 1, nullptr, 0, nullptr);
  }
  if (mask & kWritable) {
    EV_SET(&ke, fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    kevent(el_fd_, &ke, 1, nullptr, 0, nullptr);
  }
  return 0;
}

template <typename T, size_t SIZE>
int EventLoop<T, SIZE>::Poll(const struct timeval* tvp) {
  if (tvp != nullptr) {
    struct timespec timeout;
    timeout.tv_sec = tvp->tv_sec;
    timeout.tv_nsec = tvp->tv_usec * 1000;
    return kevent(el_fd_, nullptr, 0, events_.data(),
                  static_cast<int>(events_.size()), &timeout);
  } else {
    return kevent(el_fd_, nullptr, 0, events_.data(),
                  static_cast<int>(events_.size()), nullptr);
  }
}

template <typename T, size_t SIZE>
int EventLoop<T, SIZE>::GetEventFD(const Event& e) {
  return static_cast<int>(e.ident);
}

template <typename T, size_t SIZE>
bool EventLoop<T, SIZE>::IsEventReadable(const Event& e) {
  return e.filter == EVFILT_READ;
}

template <typename T, size_t SIZE>
bool EventLoop<T, SIZE>::IsEventWritable(const Event& e) {
  return e.filter == EVFILT_WRITE;
}

template <typename T, size_t SIZE>
int EventLoop<T, SIZE>::Open() {
  return kqueue();
}
}  // namespace gujia
#endif

#endif  // GUJIA_GUJIA_KQUEUE_IMPL_H