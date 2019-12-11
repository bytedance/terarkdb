#pragma once
#ifndef GUJIA_GUJIA_EPOLL_IMPL_H
#define GUJIA_GUJIA_EPOLL_IMPL_H

#include "gujia.h"

#if defined(GUJIA_HAS_EPOLL)
namespace gujia {
template <typename T, size_t SIZE>
int EventLoop<T, SIZE>::AddEvent(int fd, int mask) {
  mask |= masks_[fd]; /* Merge old events */
  if (mask == masks_[fd]) {
    return 0;
  }

  /* If the fd was already monitored for some event, we need a MOD
   * operation. Otherwise we need an ADD operation. */
  int op = masks_[fd] == kNone ? EPOLL_CTL_ADD : EPOLL_CTL_MOD;

  struct epoll_event ee = {0};
  if (mask & kReadable) ee.events |= EPOLLIN;
  if (mask & kWritable) ee.events |= EPOLLOUT;
  ee.data.fd = fd;
  if (epoll_ctl(el_fd_, op, fd, &ee) == -1) return -1;
  masks_[fd] = mask;
  return 0;
}

template <typename T, size_t SIZE>
int EventLoop<T, SIZE>::DelEvent(int fd, int del_mask) {
  int mask = masks_[fd] & (~del_mask);
  if (mask == masks_[fd]) {
    return 0;
  }

  struct epoll_event ee = {0};
  ee.data.fd = fd;
  if (mask != kNone) {
    if (mask & kReadable) ee.events |= EPOLLIN;
    if (mask & kWritable) ee.events |= EPOLLOUT;
    epoll_ctl(el_fd_, EPOLL_CTL_MOD, fd, &ee);
  } else {
    /* Note, Kernel < 2.6.9 requires a non null event pointer even for
     * EPOLL_CTL_DEL. */
    epoll_ctl(el_fd_, EPOLL_CTL_DEL, fd, &ee);
  }
  masks_[fd] = mask;
  return 0;
}

template <typename T, size_t SIZE>
int EventLoop<T, SIZE>::Poll(const struct timeval* tvp) {
  return epoll_wait(el_fd_, events_.data(), static_cast<int>(events_.size()),
                    tvp ? (tvp->tv_sec * 1000 + tvp->tv_usec / 1000) : -1);
}

template <typename T, size_t SIZE>
int EventLoop<T, SIZE>::GetEventFD(const Event& e) {
  return e.data.fd;
}

template <typename T, size_t SIZE>
bool EventLoop<T, SIZE>::IsEventReadable(const Event& e) {
  return e.events & EPOLLIN;
}

template <typename T, size_t SIZE>
bool EventLoop<T, SIZE>::IsEventWritable(const Event& e) {
  return e.events & (EPOLLOUT | EPOLLERR | EPOLLHUP);
}

template <typename T, size_t SIZE>
int EventLoop<T, SIZE>::Open() {
  return epoll_create(1024); /* 1024 is just a hint for the kernel */
}
}  // namespace gujia
#endif

#endif  // GUJIA_GUJIA_EPOLL_IMPL_H