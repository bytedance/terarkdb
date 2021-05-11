// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ TERARKDB_NAMESPACE::Env methods from Java side.

#include "rocksdb/env.h"
#include "include/org_rocksdb_Env.h"
#include "include/org_rocksdb_RocksEnv.h"
#include "include/org_rocksdb_RocksMemEnv.h"

/*
 * Class:     org_rocksdb_Env
 * Method:    getDefaultEnvInternal
 * Signature: ()J
 */
jlong Java_org_rocksdb_Env_getDefaultEnvInternal(JNIEnv* /*env*/,
                                                 jclass /*jclazz*/) {
  return reinterpret_cast<jlong>(TERARKDB_NAMESPACE::Env::Default());
}

/*
 * Class:     org_rocksdb_Env
 * Method:    setBackgroundThreads
 * Signature: (JII)V
 */
void Java_org_rocksdb_Env_setBackgroundThreads(JNIEnv* /*env*/,
                                               jobject /*jobj*/, jlong jhandle,
                                               jint num, jint priority) {
  auto* rocks_env = reinterpret_cast<TERARKDB_NAMESPACE::Env*>(jhandle);
  switch (priority) {
    case org_rocksdb_Env_FLUSH_POOL:
      rocks_env->SetBackgroundThreads(num, TERARKDB_NAMESPACE::Env::Priority::LOW);
      break;
    case org_rocksdb_Env_COMPACTION_POOL:
      rocks_env->SetBackgroundThreads(num, TERARKDB_NAMESPACE::Env::Priority::HIGH);
      break;
  }
}

/*
 * Class:     org_rocksdb_sEnv
 * Method:    getThreadPoolQueueLen
 * Signature: (JI)I
 */
jint Java_org_rocksdb_Env_getThreadPoolQueueLen(JNIEnv* /*env*/,
                                                jobject /*jobj*/, jlong jhandle,
                                                jint pool_id) {
  auto* rocks_env = reinterpret_cast<TERARKDB_NAMESPACE::Env*>(jhandle);
  switch (pool_id) {
    case org_rocksdb_RocksEnv_FLUSH_POOL:
      return rocks_env->GetThreadPoolQueueLen(TERARKDB_NAMESPACE::Env::Priority::LOW);
    case org_rocksdb_RocksEnv_COMPACTION_POOL:
      return rocks_env->GetThreadPoolQueueLen(TERARKDB_NAMESPACE::Env::Priority::HIGH);
  }
  return 0;
}

/*
 * Class:     org_rocksdb_RocksMemEnv
 * Method:    createMemEnv
 * Signature: ()J
 */
jlong Java_org_rocksdb_RocksMemEnv_createMemEnv(JNIEnv* /*env*/,
                                                jclass /*jclazz*/) {
  return reinterpret_cast<jlong>(TERARKDB_NAMESPACE::NewMemEnv(TERARKDB_NAMESPACE::Env::Default()));
}

/*
 * Class:     org_rocksdb_RocksMemEnv
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksMemEnv_disposeInternal(JNIEnv* /*env*/,
                                                  jobject /*jobj*/,
                                                  jlong jhandle) {
  auto* e = reinterpret_cast<TERARKDB_NAMESPACE::Env*>(jhandle);
  assert(e != nullptr);
  delete e;
}
