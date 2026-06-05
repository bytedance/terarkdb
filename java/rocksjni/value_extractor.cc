// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// TERARKDB_NAMESPACE::CompactionFilter.

#include "rocksdb/value_extractor.h"

#include <jni.h>

#include "include/org_rocksdb_AbstractValueExtractorFactory.h"

// <editor-fold desc="org.rocksdb.AbstractCompactionFilter">

/*
 * Class:     org_rocksdb_AbstractCompactionFilter
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_AbstractValueExtractorFactory_disposeInternal(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong handle) {
  auto* vef =
      reinterpret_cast<TERARKDB_NAMESPACE::ValueExtractorFactory*>(handle);
  assert(vef != nullptr);
  delete vef;
}
// </editor-fold>
