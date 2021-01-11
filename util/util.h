// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <utility>

#include "rocksdb/terark_namespace.h"

#ifndef FALLTHROUGH_INTENDED
#if defined(__clang__)
#define FALLTHROUGH_INTENDED [[clang::fallthrough]]
#elif defined(__GNUC__) && __GNUC__ >= 7
#define FALLTHROUGH_INTENDED [[gnu::fallthrough]]
#else
#define FALLTHROUGH_INTENDED \
  do {                       \
  } while (0)
#endif
#endif

namespace TERARKDB_NAMESPACE {
template <class T>
void call_destructor(T* ptr) {
  ptr->~T();
}

template <class T, class... Args>
void call_constructor(T* ptr, Args&&... args) {
  ::new (ptr) T(std::forward<Args>(args)...);
}
}  // namespace TERARKDB_NAMESPACE