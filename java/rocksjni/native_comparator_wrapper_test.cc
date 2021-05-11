// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <jni.h>

#include <string>

#include "include/org_rocksdb_NativeComparatorWrapperTest_NativeStringComparatorWrapper.h"
#include "rocksdb/comparator.h"
#include "rocksdb/slice.h"
#include "rocksdb/terark_namespace.h"

namespace TERARKDB_NAMESPACE {

class NativeComparatorWrapperTestStringComparator : public Comparator {
  const char* Name() const {
    return "NativeComparatorWrapperTestStringComparator";
  }

  int Compare(const Slice& a, const Slice& b) const {
    return a.ToString().compare(b.ToString());
  }

  void FindShortestSeparator(std::string* /*start*/,
                             const Slice& /*limit*/) const {
    return;
  }

  void FindShortSuccessor(std::string* /*key*/) const { return; }
};
}  // namespace TERARKDB_NAMESPACE

/*
 * Class: org_rocksdb_NativeComparatorWrapperTest_NativeStringComparatorWrapper
 * Method:    newStringComparator
 * Signature: ()J
 */
jlong Java_org_rocksdb_NativeComparatorWrapperTest_00024NativeStringComparatorWrapper_newStringComparator(
    JNIEnv* /*env*/, jobject /*jobj*/) {
  auto* comparator =
      new TERARKDB_NAMESPACE::NativeComparatorWrapperTestStringComparator();
  return reinterpret_cast<jlong>(comparator);
}
