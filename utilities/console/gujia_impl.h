#pragma once
#ifndef GUJIA_GUJIA_IMPL_H
#define GUJIA_GUJIA_IMPL_H

#include "gujia.h"

#if defined(GUJIA_HAS_EPOLL)
#include "gujia_epoll_impl.h"
#endif
#if defined(GUJIA_HAS_KQUEUE)
#include "gujia_kqueue_impl.h"
#endif

namespace gujia {
template <typename T, size_t SIZE>
EventLoop<T, SIZE>::~EventLoop() {
  close(el_fd_);
  for (int fd = 0; fd <= max_fd_; ++fd) {
    if (resources_[fd] != nullptr) {
      close(fd);
    }
  }
}

template <typename T, size_t SIZE>
int EventLoop<T, SIZE>::Acquire(int fd, std::unique_ptr<T>&& resource) {
  assert(fd >= 0 && resource != nullptr);
  if (fd >= static_cast<int>(resources_.size())) {
    return -1;
  }

  resources_[fd].swap(resource);
  assert(resource == nullptr);
  max_fd_ = std::max(max_fd_, fd);
  return 0;
}

template <typename T, size_t SIZE>
int EventLoop<T, SIZE>::Release(int fd) {
  assert(fd >= 0 && fd <= max_fd_);
  resources_[fd].reset();
  if (fd == max_fd_) {
    do {
      --max_fd_;
    } while (max_fd_ != -1 && resources_[max_fd_] == nullptr);
  }

#if defined(GUJIA_HAS_EPOLL)
  masks_[fd] = 0;
#endif
  return close(fd);
}
}  // namespace gujia

#endif  // GUJIA_GUJIA_IMPL_H