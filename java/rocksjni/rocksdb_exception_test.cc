// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <jni.h>

#include "include/org_rocksdb_RocksDBExceptionTest.h"

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_RocksDBExceptionTest
 * Method:    raiseException
 * Signature: ()V
 */
void Java_org_rocksdb_RocksDBExceptionTest_raiseException(JNIEnv* env,
                                                          jobject /*jobj*/) {
  TERARKDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, std::string("test message"));
}

/*
 * Class:     org_rocksdb_RocksDBExceptionTest
 * Method:    raiseExceptionWithStatusCode
 * Signature: ()V
 */
void Java_org_rocksdb_RocksDBExceptionTest_raiseExceptionWithStatusCode(
    JNIEnv* env, jobject /*jobj*/) {
  TERARKDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, "test message",
                                         TERARKDB_NAMESPACE::Status::NotSupported());
}

/*
 * Class:     org_rocksdb_RocksDBExceptionTest
 * Method:    raiseExceptionNoMsgWithStatusCode
 * Signature: ()V
 */
void Java_org_rocksdb_RocksDBExceptionTest_raiseExceptionNoMsgWithStatusCode(
    JNIEnv* env, jobject /*jobj*/) {
  TERARKDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, TERARKDB_NAMESPACE::Status::NotSupported());
}

/*
 * Class:     org_rocksdb_RocksDBExceptionTest
 * Method:    raiseExceptionWithStatusCodeSubCode
 * Signature: ()V
 */
void Java_org_rocksdb_RocksDBExceptionTest_raiseExceptionWithStatusCodeSubCode(
    JNIEnv* env, jobject /*jobj*/) {
  TERARKDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
      env, "test message",
      TERARKDB_NAMESPACE::Status::TimedOut(TERARKDB_NAMESPACE::Status::SubCode::kLockTimeout));
}

/*
 * Class:     org_rocksdb_RocksDBExceptionTest
 * Method:    raiseExceptionNoMsgWithStatusCodeSubCode
 * Signature: ()V
 */
void Java_org_rocksdb_RocksDBExceptionTest_raiseExceptionNoMsgWithStatusCodeSubCode(
    JNIEnv* env, jobject /*jobj*/) {
  TERARKDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
      env, TERARKDB_NAMESPACE::Status::TimedOut(TERARKDB_NAMESPACE::Status::SubCode::kLockTimeout));
}

/*
 * Class:     org_rocksdb_RocksDBExceptionTest
 * Method:    raiseExceptionWithStatusCodeState
 * Signature: ()V
 */
void Java_org_rocksdb_RocksDBExceptionTest_raiseExceptionWithStatusCodeState(
    JNIEnv* env, jobject /*jobj*/) {
  TERARKDB_NAMESPACE::Slice state("test state");
  TERARKDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, "test message",
                                         TERARKDB_NAMESPACE::Status::NotSupported(state));
}
