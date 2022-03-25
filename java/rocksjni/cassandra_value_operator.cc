//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <memory>
#include <string>

#include "include/org_rocksdb_CassandraValueMergeOperator.h"
#include "terarkdb/db.h"
#include "terarkdb/memtablerep.h"
#include "terarkdb/merge_operator.h"
#include "terarkdb/options.h"
#include "terarkdb/slice_transform.h"
#include "terarkdb/statistics.h"
#include "terarkdb/table.h"
#include "rocksjni/portal.h"
#include "utilities/cassandra/merge_operator.h"

/*
 * Class:     org_rocksdb_CassandraValueMergeOperator
 * Method:    newSharedCassandraValueMergeOperator
 * Signature: (II)J
 */
jlong Java_org_rocksdb_CassandraValueMergeOperator_newSharedCassandraValueMergeOperator(
    JNIEnv* /*env*/, jclass /*jclazz*/, jint gcGracePeriodInSeconds,
    jint operands_limit) {
  auto* op = new std::shared_ptr<TERARKDB_NAMESPACE::MergeOperator>(
      new TERARKDB_NAMESPACE::cassandra::CassandraValueMergeOperator(
          gcGracePeriodInSeconds, operands_limit));
  return reinterpret_cast<jlong>(op);
}

/*
 * Class:     org_rocksdb_CassandraValueMergeOperator
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_CassandraValueMergeOperator_disposeInternal(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* op =
      reinterpret_cast<std::shared_ptr<TERARKDB_NAMESPACE::MergeOperator>*>(jhandle);
  delete op;
}
