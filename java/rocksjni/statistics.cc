// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ TERARKDB_NAMESPACE::Statistics methods from Java side.

#include <jni.h>
#include <memory>
#include <set>

#include "include/org_rocksdb_Statistics.h"
#include "rocksdb/statistics.h"
#include "rocksjni/portal.h"
#include "rocksjni/statisticsjni.h"

/*
 * Class:     org_rocksdb_Statistics
 * Method:    newStatistics
 * Signature: ()J
 */
jlong Java_org_rocksdb_Statistics_newStatistics__(JNIEnv* env, jclass jcls) {
  return Java_org_rocksdb_Statistics_newStatistics___3BJ(env, jcls, nullptr, 0);
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    newStatistics
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Statistics_newStatistics__J(
    JNIEnv* env, jclass jcls, jlong jother_statistics_handle) {
  return Java_org_rocksdb_Statistics_newStatistics___3BJ(
      env, jcls, nullptr, jother_statistics_handle);
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    newStatistics
 * Signature: ([B)J
 */
jlong Java_org_rocksdb_Statistics_newStatistics___3B(JNIEnv* env, jclass jcls,
                                                     jbyteArray jhistograms) {
  return Java_org_rocksdb_Statistics_newStatistics___3BJ(env, jcls, jhistograms,
                                                         0);
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    newStatistics
 * Signature: ([BJ)J
 */
jlong Java_org_rocksdb_Statistics_newStatistics___3BJ(
    JNIEnv* env, jclass /*jcls*/, jbyteArray jhistograms,
    jlong jother_statistics_handle) {
  std::shared_ptr<TERARKDB_NAMESPACE::Statistics>* pSptr_other_statistics = nullptr;
  if (jother_statistics_handle > 0) {
    pSptr_other_statistics =
        reinterpret_cast<std::shared_ptr<TERARKDB_NAMESPACE::Statistics>*>(
            jother_statistics_handle);
  }

  std::set<uint32_t> histograms;
  if (jhistograms != nullptr) {
    const jsize len = env->GetArrayLength(jhistograms);
    if (len > 0) {
      jbyte* jhistogram = env->GetByteArrayElements(jhistograms, nullptr);
      if (jhistogram == nullptr) {
        // exception thrown: OutOfMemoryError
        return 0;
      }

      for (jsize i = 0; i < len; i++) {
        const TERARKDB_NAMESPACE::Histograms histogram =
            TERARKDB_NAMESPACE::HistogramTypeJni::toCppHistograms(jhistogram[i]);
        histograms.emplace(histogram);
      }

      env->ReleaseByteArrayElements(jhistograms, jhistogram, JNI_ABORT);
    }
  }

  std::shared_ptr<TERARKDB_NAMESPACE::Statistics> sptr_other_statistics = nullptr;
  if (pSptr_other_statistics != nullptr) {
    sptr_other_statistics = *pSptr_other_statistics;
  }

  auto* pSptr_statistics = new std::shared_ptr<TERARKDB_NAMESPACE::StatisticsJni>(
      new TERARKDB_NAMESPACE::StatisticsJni(sptr_other_statistics, histograms));

  return reinterpret_cast<jlong>(pSptr_statistics);
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_Statistics_disposeInternal(JNIEnv* /*env*/,
                                                 jobject /*jobj*/,
                                                 jlong jhandle) {
  if (jhandle > 0) {
    auto* pSptr_statistics =
        reinterpret_cast<std::shared_ptr<TERARKDB_NAMESPACE::Statistics>*>(jhandle);
    delete pSptr_statistics;
  }
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    statsLevel
 * Signature: (J)B
 */
jbyte Java_org_rocksdb_Statistics_statsLevel(JNIEnv* /*env*/, jobject /*jobj*/,
                                             jlong jhandle) {
  auto* pSptr_statistics =
      reinterpret_cast<std::shared_ptr<TERARKDB_NAMESPACE::Statistics>*>(jhandle);
  assert(pSptr_statistics != nullptr);
  return TERARKDB_NAMESPACE::StatsLevelJni::toJavaStatsLevel(
      pSptr_statistics->get()->stats_level_);
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    setStatsLevel
 * Signature: (JB)V
 */
void Java_org_rocksdb_Statistics_setStatsLevel(JNIEnv* /*env*/,
                                               jobject /*jobj*/, jlong jhandle,
                                               jbyte jstats_level) {
  auto* pSptr_statistics =
      reinterpret_cast<std::shared_ptr<TERARKDB_NAMESPACE::Statistics>*>(jhandle);
  assert(pSptr_statistics != nullptr);
  auto stats_level = TERARKDB_NAMESPACE::StatsLevelJni::toCppStatsLevel(jstats_level);
  pSptr_statistics->get()->stats_level_ = stats_level;
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    getTickerCount
 * Signature: (JB)J
 */
jlong Java_org_rocksdb_Statistics_getTickerCount(JNIEnv* /*env*/,
                                                 jobject /*jobj*/,
                                                 jlong jhandle,
                                                 jbyte jticker_type) {
  auto* pSptr_statistics =
      reinterpret_cast<std::shared_ptr<TERARKDB_NAMESPACE::Statistics>*>(jhandle);
  assert(pSptr_statistics != nullptr);
  auto ticker = TERARKDB_NAMESPACE::TickerTypeJni::toCppTickers(jticker_type);
  return pSptr_statistics->get()->getTickerCount(ticker);
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    getAndResetTickerCount
 * Signature: (JB)J
 */
jlong Java_org_rocksdb_Statistics_getAndResetTickerCount(JNIEnv* /*env*/,
                                                         jobject /*jobj*/,
                                                         jlong jhandle,
                                                         jbyte jticker_type) {
  auto* pSptr_statistics =
      reinterpret_cast<std::shared_ptr<TERARKDB_NAMESPACE::Statistics>*>(jhandle);
  assert(pSptr_statistics != nullptr);
  auto ticker = TERARKDB_NAMESPACE::TickerTypeJni::toCppTickers(jticker_type);
  return pSptr_statistics->get()->getAndResetTickerCount(ticker);
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    getHistogramData
 * Signature: (JB)Lorg/rocksdb/HistogramData;
 */
jobject Java_org_rocksdb_Statistics_getHistogramData(JNIEnv* env,
                                                     jobject /*jobj*/,
                                                     jlong jhandle,
                                                     jbyte jhistogram_type) {
  auto* pSptr_statistics =
      reinterpret_cast<std::shared_ptr<TERARKDB_NAMESPACE::Statistics>*>(jhandle);
  assert(pSptr_statistics != nullptr);

  TERARKDB_NAMESPACE::HistogramData
      data;  // TODO(AR) perhaps better to construct a Java Object Wrapper that
             // uses ptr to C++ `new HistogramData`
  auto histogram = TERARKDB_NAMESPACE::HistogramTypeJni::toCppHistograms(jhistogram_type);
  pSptr_statistics->get()->histogramData(
      static_cast<TERARKDB_NAMESPACE::Histograms>(histogram), &data);

  jclass jclazz = TERARKDB_NAMESPACE::HistogramDataJni::getJClass(env);
  if (jclazz == nullptr) {
    // exception occurred accessing class
    return nullptr;
  }

  jmethodID mid = TERARKDB_NAMESPACE::HistogramDataJni::getConstructorMethodId(env);
  if (mid == nullptr) {
    // exception occurred accessing method
    return nullptr;
  }

  return env->NewObject(jclazz, mid, data.median, data.percentile95,
                        data.percentile99, data.average,
                        data.standard_deviation);
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    getHistogramString
 * Signature: (JB)Ljava/lang/String;
 */
jstring Java_org_rocksdb_Statistics_getHistogramString(JNIEnv* env,
                                                       jobject /*jobj*/,
                                                       jlong jhandle,
                                                       jbyte jhistogram_type) {
  auto* pSptr_statistics =
      reinterpret_cast<std::shared_ptr<TERARKDB_NAMESPACE::Statistics>*>(jhandle);
  assert(pSptr_statistics != nullptr);
  auto histogram = TERARKDB_NAMESPACE::HistogramTypeJni::toCppHistograms(jhistogram_type);
  auto str = pSptr_statistics->get()->getHistogramString(histogram);
  return env->NewStringUTF(str.c_str());
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    reset
 * Signature: (J)V
 */
void Java_org_rocksdb_Statistics_reset(JNIEnv* env, jobject /*jobj*/,
                                       jlong jhandle) {
  auto* pSptr_statistics =
      reinterpret_cast<std::shared_ptr<TERARKDB_NAMESPACE::Statistics>*>(jhandle);
  assert(pSptr_statistics != nullptr);
  TERARKDB_NAMESPACE::Status s = pSptr_statistics->get()->Reset();
  if (!s.ok()) {
    TERARKDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_rocksdb_Statistics
 * Method:    toString
 * Signature: (J)Ljava/lang/String;
 */
jstring Java_org_rocksdb_Statistics_toString(JNIEnv* env, jobject /*jobj*/,
                                             jlong jhandle) {
  auto* pSptr_statistics =
      reinterpret_cast<std::shared_ptr<TERARKDB_NAMESPACE::Statistics>*>(jhandle);
  assert(pSptr_statistics != nullptr);
  auto str = pSptr_statistics->get()->ToString();
  return env->NewStringUTF(str.c_str());
}
