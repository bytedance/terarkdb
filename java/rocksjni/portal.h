// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// This file is designed for caching those frequently used IDs and provide
// efficient portal (i.e, a set of static functions) to access java code
// from c++.

#ifndef JAVA_ROCKSJNI_PORTAL_H_
#define JAVA_ROCKSJNI_PORTAL_H_

#include <jni.h>

#include <cstring>
#include <functional>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/status.h"
#include "rocksdb/terark_namespace.h"
#include "rocksdb/utilities/backupable_db.h"
#include "rocksdb/utilities/memory_util.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "rocksjni/compaction_filter_factory_jnicallback.h"
#include "rocksjni/comparatorjnicallback.h"
#include "rocksjni/loggerjnicallback.h"
#include "rocksjni/transaction_notifier_jnicallback.h"
#include "rocksjni/writebatchhandlerjnicallback.h"

// Remove macro on windows
#ifdef DELETE
#undef DELETE
#endif

namespace TERARKDB_NAMESPACE {

// Detect if jlong overflows size_t
inline Status check_if_jlong_fits_size_t(const jlong& jvalue) {
  Status s = Status::OK();
  if (static_cast<uint64_t>(jvalue) > std::numeric_limits<size_t>::max()) {
    s = Status::InvalidArgument(Slice("jlong overflows 32 bit value."));
  }
  return s;
}

class JavaClass {
 public:
  /**
   * Gets and initializes a Java Class
   *
   * @param env A pointer to the Java environment
   * @param jclazz_name The fully qualified JNI name of the Java Class
   *     e.g. "java/lang/String"
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env, const char* jclazz_name) {
    jclass jclazz = env->FindClass(jclazz_name);
    assert(jclazz != nullptr);
    return jclazz;
  }
};

// Native class template
template <class PTR, class DERIVED>
class RocksDBNativeClass : public JavaClass {};

// Native class template for sub-classes of RocksMutableObject
template <class PTR, class DERIVED>
class NativeRocksMutableObject : public RocksDBNativeClass<PTR, DERIVED> {
 public:
  /**
   * Gets the Java Method ID for the
   * RocksMutableObject#setNativeHandle(long, boolean) method
   *
   * @param env A pointer to the Java environment
   * @return The Java Method ID or nullptr the RocksMutableObject class cannot
   *     be accessed, or if one of the NoSuchMethodError,
   *     ExceptionInInitializerError or OutOfMemoryError exceptions is thrown
   */
  static jmethodID getSetNativeHandleMethod(JNIEnv* env) {
    static jclass jclazz = DERIVED::getJClass(env);
    if (jclazz == nullptr) {
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "setNativeHandle", "(JZ)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Sets the C++ object pointer handle in the Java object
   *
   * @param env A pointer to the Java environment
   * @param jobj The Java object on which to set the pointer handle
   * @param ptr The C++ object pointer
   * @param java_owns_handle JNI_TRUE if ownership of the C++ object is
   *     managed by the Java object
   *
   * @return true if a Java exception is pending, false otherwise
   */
  static bool setHandle(JNIEnv* env, jobject jobj, PTR ptr,
                        jboolean java_owns_handle) {
    assert(jobj != nullptr);
    static jmethodID mid = getSetNativeHandleMethod(env);
    if (mid == nullptr) {
      return true;  // signal exception
    }

    env->CallVoidMethod(jobj, mid, reinterpret_cast<jlong>(ptr),
                        java_owns_handle);
    if (env->ExceptionCheck()) {
      return true;  // signal exception
    }

    return false;
  }
};

// Java Exception template
template <class DERIVED>
class JavaException : public JavaClass {
 public:
  /**
   * Create and throw a java exception with the provided message
   *
   * @param env A pointer to the Java environment
   * @param msg The message for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const std::string& msg) {
    jclass jclazz = DERIVED::getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      std::cerr << "JavaException::ThrowNew - Error: unexpected exception!"
                << std::endl;
      return env->ExceptionCheck();
    }

    const jint rs = env->ThrowNew(jclazz, msg.c_str());
    if (rs != JNI_OK) {
      // exception could not be thrown
      std::cerr << "JavaException::ThrowNew - Fatal: could not throw exception!"
                << std::endl;
      return env->ExceptionCheck();
    }

    return true;
  }
};

// The portal class for org.rocksdb.RocksDB
class RocksDBJni
    : public RocksDBNativeClass<TERARKDB_NAMESPACE::DB*, RocksDBJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.RocksDB
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/RocksDB");
  }
};

// The portal class for org.rocksdb.Status.Code
class CodeJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.Status.Code
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/Status$Code");
  }

  /**
   * Get the Java Method: Status.Code#getValue
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getValueMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "getValue", "()b");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.Status.SubCode
class SubCodeJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.Status.SubCode
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/Status$SubCode");
  }

  /**
   * Get the Java Method: Status.SubCode#getValue
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getValueMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "getValue", "()b");
    assert(mid != nullptr);
    return mid;
  }

  static TERARKDB_NAMESPACE::Status::SubCode toCppSubCode(
      const jbyte jsub_code) {
    switch (jsub_code) {
      case 0x0:
        return TERARKDB_NAMESPACE::Status::SubCode::kNone;
      case 0x1:
        return TERARKDB_NAMESPACE::Status::SubCode::kMutexTimeout;
      case 0x2:
        return TERARKDB_NAMESPACE::Status::SubCode::kLockTimeout;
      case 0x3:
        return TERARKDB_NAMESPACE::Status::SubCode::kLockLimit;
      case 0x4:
        return TERARKDB_NAMESPACE::Status::SubCode::kNoSpace;
      case 0x5:
        return TERARKDB_NAMESPACE::Status::SubCode::kDeadlock;
      case 0x6:
        return TERARKDB_NAMESPACE::Status::SubCode::kStaleFile;
      case 0x7:
        return TERARKDB_NAMESPACE::Status::SubCode::kMemoryLimit;

      case 0x7F:
      default:
        return TERARKDB_NAMESPACE::Status::SubCode::kNone;
    }
  }
};

// The portal class for org.rocksdb.Status
class StatusJni
    : public RocksDBNativeClass<TERARKDB_NAMESPACE::Status*, StatusJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.Status
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/Status");
  }

  /**
   * Get the Java Method: Status#getCode
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getCodeMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "getCode", "()Lorg/rocksdb/Status$Code;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Status#getSubCode
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getSubCodeMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "getSubCode",
                                            "()Lorg/rocksdb/Status$SubCode;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Status#getState
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getStateMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "getState", "()Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Create a new Java org.rocksdb.Status object with the same properties as
   * the provided C++ TERARKDB_NAMESPACE::Status object
   *
   * @param env A pointer to the Java environment
   * @param status The TERARKDB_NAMESPACE::Status object
   *
   * @return A reference to a Java org.rocksdb.Status object, or nullptr
   *     if an an exception occurs
   */
  static jobject construct(JNIEnv* env, const Status& status) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid =
        env->GetMethodID(jclazz, "<init>", "(BBLjava/lang/String;)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    // convert the Status state for Java
    jstring jstate = nullptr;
    if (status.getState() != nullptr) {
      const char* const state = status.getState();
      jstate = env->NewStringUTF(state);
      if (env->ExceptionCheck()) {
        if (jstate != nullptr) {
          env->DeleteLocalRef(jstate);
        }
        return nullptr;
      }
    }

    jobject jstatus =
        env->NewObject(jclazz, mid, toJavaStatusCode(status.code()),
                       toJavaStatusSubCode(status.subcode()), jstate);
    if (env->ExceptionCheck()) {
      // exception occurred
      if (jstate != nullptr) {
        env->DeleteLocalRef(jstate);
      }
      return nullptr;
    }

    if (jstate != nullptr) {
      env->DeleteLocalRef(jstate);
    }

    return jstatus;
  }

  // Returns the equivalent org.rocksdb.Status.Code for the provided
  // C++ TERARKDB_NAMESPACE::Status::Code enum
  static jbyte toJavaStatusCode(const TERARKDB_NAMESPACE::Status::Code& code) {
    switch (code) {
      case TERARKDB_NAMESPACE::Status::Code::kOk:
        return 0x0;
      case TERARKDB_NAMESPACE::Status::Code::kNotFound:
        return 0x1;
      case TERARKDB_NAMESPACE::Status::Code::kCorruption:
        return 0x2;
      case TERARKDB_NAMESPACE::Status::Code::kNotSupported:
        return 0x3;
      case TERARKDB_NAMESPACE::Status::Code::kInvalidArgument:
        return 0x4;
      case TERARKDB_NAMESPACE::Status::Code::kIOError:
        return 0x5;
      case TERARKDB_NAMESPACE::Status::Code::kMergeInProgress:
        return 0x6;
      case TERARKDB_NAMESPACE::Status::Code::kIncomplete:
        return 0x7;
      case TERARKDB_NAMESPACE::Status::Code::kShutdownInProgress:
        return 0x8;
      case TERARKDB_NAMESPACE::Status::Code::kTimedOut:
        return 0x9;
      case TERARKDB_NAMESPACE::Status::Code::kAborted:
        return 0xA;
      case TERARKDB_NAMESPACE::Status::Code::kBusy:
        return 0xB;
      case TERARKDB_NAMESPACE::Status::Code::kExpired:
        return 0xC;
      case TERARKDB_NAMESPACE::Status::Code::kTryAgain:
        return 0xD;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent org.rocksdb.Status.SubCode for the provided
  // C++ TERARKDB_NAMESPACE::Status::SubCode enum
  static jbyte toJavaStatusSubCode(
      const TERARKDB_NAMESPACE::Status::SubCode& subCode) {
    switch (subCode) {
      case TERARKDB_NAMESPACE::Status::SubCode::kNone:
        return 0x0;
      case TERARKDB_NAMESPACE::Status::SubCode::kMutexTimeout:
        return 0x1;
      case TERARKDB_NAMESPACE::Status::SubCode::kLockTimeout:
        return 0x2;
      case TERARKDB_NAMESPACE::Status::SubCode::kLockLimit:
        return 0x3;
      case TERARKDB_NAMESPACE::Status::SubCode::kNoSpace:
        return 0x4;
      case TERARKDB_NAMESPACE::Status::SubCode::kDeadlock:
        return 0x5;
      case TERARKDB_NAMESPACE::Status::SubCode::kStaleFile:
        return 0x6;
      case TERARKDB_NAMESPACE::Status::SubCode::kMemoryLimit:
        return 0x7;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent TERARKDB_NAMESPACE::Status for the Java
  // org.rocksdb.Status
  static std::unique_ptr<TERARKDB_NAMESPACE::Status> toCppStatus(
      JNIEnv* env, const jobject jstatus) {
    jmethodID mid_code = getCodeMethod(env);
    if (mid_code == nullptr) {
      // exception occurred
      return nullptr;
    }
    jobject jcode = env->CallObjectMethod(jstatus, mid_code);
    if (env->ExceptionCheck()) {
      // exception occurred
      return nullptr;
    }

    jmethodID mid_code_value = TERARKDB_NAMESPACE::CodeJni::getValueMethod(env);
    if (mid_code_value == nullptr) {
      // exception occurred
      return nullptr;
    }
    jbyte jcode_value = env->CallByteMethod(jcode, mid_code_value);
    if (env->ExceptionCheck()) {
      // exception occurred
      if (jcode != nullptr) {
        env->DeleteLocalRef(jcode);
      }
      return nullptr;
    }

    jmethodID mid_subCode = getSubCodeMethod(env);
    if (mid_subCode == nullptr) {
      // exception occurred
      return nullptr;
    }
    jobject jsubCode = env->CallObjectMethod(jstatus, mid_subCode);
    if (env->ExceptionCheck()) {
      // exception occurred
      if (jcode != nullptr) {
        env->DeleteLocalRef(jcode);
      }
      return nullptr;
    }

    jbyte jsubCode_value = 0x0;  // None
    if (jsubCode != nullptr) {
      jmethodID mid_subCode_value =
          TERARKDB_NAMESPACE::SubCodeJni::getValueMethod(env);
      if (mid_subCode_value == nullptr) {
        // exception occurred
        return nullptr;
      }
      jsubCode_value = env->CallByteMethod(jsubCode, mid_subCode_value);
      if (env->ExceptionCheck()) {
        // exception occurred
        if (jcode != nullptr) {
          env->DeleteLocalRef(jcode);
        }
        return nullptr;
      }
    }

    jmethodID mid_state = getStateMethod(env);
    if (mid_state == nullptr) {
      // exception occurred
      return nullptr;
    }
    jobject jstate = env->CallObjectMethod(jstatus, mid_state);
    if (env->ExceptionCheck()) {
      // exception occurred
      if (jsubCode != nullptr) {
        env->DeleteLocalRef(jsubCode);
      }
      if (jcode != nullptr) {
        env->DeleteLocalRef(jcode);
      }
      return nullptr;
    }

    std::unique_ptr<TERARKDB_NAMESPACE::Status> status;
    switch (jcode_value) {
      case 0x0:
        // Ok
        status = std::unique_ptr<TERARKDB_NAMESPACE::Status>(
            new TERARKDB_NAMESPACE::Status(TERARKDB_NAMESPACE::Status::OK()));
        break;
      case 0x1:
        // NotFound
        status = std::unique_ptr<TERARKDB_NAMESPACE::Status>(
            new TERARKDB_NAMESPACE::Status(TERARKDB_NAMESPACE::Status::NotFound(
                TERARKDB_NAMESPACE::SubCodeJni::toCppSubCode(jsubCode_value))));
        break;
      case 0x2:
        // Corruption
        status = std::unique_ptr<TERARKDB_NAMESPACE::Status>(
            new TERARKDB_NAMESPACE::Status(
                TERARKDB_NAMESPACE::Status::Corruption(
                    TERARKDB_NAMESPACE::SubCodeJni::toCppSubCode(
                        jsubCode_value))));
        break;
      case 0x3:
        // NotSupported
        status = std::unique_ptr<TERARKDB_NAMESPACE::Status>(
            new TERARKDB_NAMESPACE::Status(
                TERARKDB_NAMESPACE::Status::NotSupported(
                    TERARKDB_NAMESPACE::SubCodeJni::toCppSubCode(
                        jsubCode_value))));
        break;
      case 0x4:
        // InvalidArgument
        status = std::unique_ptr<TERARKDB_NAMESPACE::Status>(
            new TERARKDB_NAMESPACE::Status(
                TERARKDB_NAMESPACE::Status::InvalidArgument(
                    TERARKDB_NAMESPACE::SubCodeJni::toCppSubCode(
                        jsubCode_value))));
        break;
      case 0x5:
        // IOError
        status = std::unique_ptr<TERARKDB_NAMESPACE::Status>(
            new TERARKDB_NAMESPACE::Status(TERARKDB_NAMESPACE::Status::IOError(
                TERARKDB_NAMESPACE::SubCodeJni::toCppSubCode(jsubCode_value))));
        break;
      case 0x6:
        // MergeInProgress
        status = std::unique_ptr<TERARKDB_NAMESPACE::Status>(
            new TERARKDB_NAMESPACE::Status(
                TERARKDB_NAMESPACE::Status::MergeInProgress(
                    TERARKDB_NAMESPACE::SubCodeJni::toCppSubCode(
                        jsubCode_value))));
        break;
      case 0x7:
        // Incomplete
        status = std::unique_ptr<TERARKDB_NAMESPACE::Status>(
            new TERARKDB_NAMESPACE::Status(
                TERARKDB_NAMESPACE::Status::Incomplete(
                    TERARKDB_NAMESPACE::SubCodeJni::toCppSubCode(
                        jsubCode_value))));
        break;
      case 0x8:
        // ShutdownInProgress
        status = std::unique_ptr<TERARKDB_NAMESPACE::Status>(
            new TERARKDB_NAMESPACE::Status(
                TERARKDB_NAMESPACE::Status::ShutdownInProgress(
                    TERARKDB_NAMESPACE::SubCodeJni::toCppSubCode(
                        jsubCode_value))));
        break;
      case 0x9:
        // TimedOut
        status = std::unique_ptr<TERARKDB_NAMESPACE::Status>(
            new TERARKDB_NAMESPACE::Status(TERARKDB_NAMESPACE::Status::TimedOut(
                TERARKDB_NAMESPACE::SubCodeJni::toCppSubCode(jsubCode_value))));
        break;
      case 0xA:
        // Aborted
        status = std::unique_ptr<TERARKDB_NAMESPACE::Status>(
            new TERARKDB_NAMESPACE::Status(TERARKDB_NAMESPACE::Status::Aborted(
                TERARKDB_NAMESPACE::SubCodeJni::toCppSubCode(jsubCode_value))));
        break;
      case 0xB:
        // Busy
        status = std::unique_ptr<TERARKDB_NAMESPACE::Status>(
            new TERARKDB_NAMESPACE::Status(TERARKDB_NAMESPACE::Status::Busy(
                TERARKDB_NAMESPACE::SubCodeJni::toCppSubCode(jsubCode_value))));
        break;
      case 0xC:
        // Expired
        status = std::unique_ptr<TERARKDB_NAMESPACE::Status>(
            new TERARKDB_NAMESPACE::Status(TERARKDB_NAMESPACE::Status::Expired(
                TERARKDB_NAMESPACE::SubCodeJni::toCppSubCode(jsubCode_value))));
        break;
      case 0xD:
        // TryAgain
        status = std::unique_ptr<TERARKDB_NAMESPACE::Status>(
            new TERARKDB_NAMESPACE::Status(TERARKDB_NAMESPACE::Status::TryAgain(
                TERARKDB_NAMESPACE::SubCodeJni::toCppSubCode(jsubCode_value))));
        break;
      case 0x7F:
      default:
        return nullptr;
    }

    // delete all local refs
    if (jstate != nullptr) {
      env->DeleteLocalRef(jstate);
    }
    if (jsubCode != nullptr) {
      env->DeleteLocalRef(jsubCode);
    }
    if (jcode != nullptr) {
      env->DeleteLocalRef(jcode);
    }

    return status;
  }
};

// The portal class for org.rocksdb.RocksDBException
class RocksDBExceptionJni : public JavaException<RocksDBExceptionJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.RocksDBException
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaException::getJClass(env, "org/rocksdb/RocksDBException");
  }

  /**
   * Create and throw a Java RocksDBException with the provided message
   *
   * @param env A pointer to the Java environment
   * @param msg The message for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const std::string& msg) {
    return JavaException::ThrowNew(env, msg);
  }

  /**
   * Create and throw a Java RocksDBException with the provided status
   *
   * If s->ok() == true, then this function will not throw any exception.
   *
   * @param env A pointer to the Java environment
   * @param s The status for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, std::unique_ptr<Status>& s) {
    return TERARKDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, *(s.get()));
  }

  /**
   * Create and throw a Java RocksDBException with the provided status
   *
   * If s.ok() == true, then this function will not throw any exception.
   *
   * @param env A pointer to the Java environment
   * @param s The status for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const Status& s) {
    assert(!s.ok());
    if (s.ok()) {
      return false;
    }

    // get the RocksDBException class
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      std::cerr << "RocksDBExceptionJni::ThrowNew/class - Error: unexpected "
                   "exception!"
                << std::endl;
      return env->ExceptionCheck();
    }

    // get the constructor of org.rocksdb.RocksDBException
    jmethodID mid =
        env->GetMethodID(jclazz, "<init>", "(Lorg/rocksdb/Status;)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      std::cerr
          << "RocksDBExceptionJni::ThrowNew/cstr - Error: unexpected exception!"
          << std::endl;
      return env->ExceptionCheck();
    }

    // get the Java status object
    jobject jstatus = StatusJni::construct(env, s);
    if (jstatus == nullptr) {
      // exception occcurred
      std::cerr << "RocksDBExceptionJni::ThrowNew/StatusJni - Error: "
                   "unexpected exception!"
                << std::endl;
      return env->ExceptionCheck();
    }

    // construct the RocksDBException
    jthrowable rocksdb_exception =
        reinterpret_cast<jthrowable>(env->NewObject(jclazz, mid, jstatus));
    if (env->ExceptionCheck()) {
      if (jstatus != nullptr) {
        env->DeleteLocalRef(jstatus);
      }
      if (rocksdb_exception != nullptr) {
        env->DeleteLocalRef(rocksdb_exception);
      }
      std::cerr << "RocksDBExceptionJni::ThrowNew/NewObject - Error: "
                   "unexpected exception!"
                << std::endl;
      return true;
    }

    // throw the RocksDBException
    const jint rs = env->Throw(rocksdb_exception);
    if (rs != JNI_OK) {
      // exception could not be thrown
      std::cerr
          << "RocksDBExceptionJni::ThrowNew - Fatal: could not throw exception!"
          << std::endl;
      if (jstatus != nullptr) {
        env->DeleteLocalRef(jstatus);
      }
      if (rocksdb_exception != nullptr) {
        env->DeleteLocalRef(rocksdb_exception);
      }
      return env->ExceptionCheck();
    }

    if (jstatus != nullptr) {
      env->DeleteLocalRef(jstatus);
    }
    if (rocksdb_exception != nullptr) {
      env->DeleteLocalRef(rocksdb_exception);
    }

    return true;
  }

  /**
   * Create and throw a Java RocksDBException with the provided message
   * and status
   *
   * If s.ok() == true, then this function will not throw any exception.
   *
   * @param env A pointer to the Java environment
   * @param msg The message for the exception
   * @param s The status for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const std::string& msg, const Status& s) {
    assert(!s.ok());
    if (s.ok()) {
      return false;
    }

    // get the RocksDBException class
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      std::cerr << "RocksDBExceptionJni::ThrowNew/class - Error: unexpected "
                   "exception!"
                << std::endl;
      return env->ExceptionCheck();
    }

    // get the constructor of org.rocksdb.RocksDBException
    jmethodID mid = env->GetMethodID(
        jclazz, "<init>", "(Ljava/lang/String;Lorg/rocksdb/Status;)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      std::cerr
          << "RocksDBExceptionJni::ThrowNew/cstr - Error: unexpected exception!"
          << std::endl;
      return env->ExceptionCheck();
    }

    jstring jmsg = env->NewStringUTF(msg.c_str());
    if (jmsg == nullptr) {
      // exception thrown: OutOfMemoryError
      std::cerr
          << "RocksDBExceptionJni::ThrowNew/msg - Error: unexpected exception!"
          << std::endl;
      return env->ExceptionCheck();
    }

    // get the Java status object
    jobject jstatus = StatusJni::construct(env, s);
    if (jstatus == nullptr) {
      // exception occcurred
      std::cerr << "RocksDBExceptionJni::ThrowNew/StatusJni - Error: "
                   "unexpected exception!"
                << std::endl;
      if (jmsg != nullptr) {
        env->DeleteLocalRef(jmsg);
      }
      return env->ExceptionCheck();
    }

    // construct the RocksDBException
    jthrowable rocksdb_exception = reinterpret_cast<jthrowable>(
        env->NewObject(jclazz, mid, jmsg, jstatus));
    if (env->ExceptionCheck()) {
      if (jstatus != nullptr) {
        env->DeleteLocalRef(jstatus);
      }
      if (jmsg != nullptr) {
        env->DeleteLocalRef(jmsg);
      }
      if (rocksdb_exception != nullptr) {
        env->DeleteLocalRef(rocksdb_exception);
      }
      std::cerr << "RocksDBExceptionJni::ThrowNew/NewObject - Error: "
                   "unexpected exception!"
                << std::endl;
      return true;
    }

    // throw the RocksDBException
    const jint rs = env->Throw(rocksdb_exception);
    if (rs != JNI_OK) {
      // exception could not be thrown
      std::cerr
          << "RocksDBExceptionJni::ThrowNew - Fatal: could not throw exception!"
          << std::endl;
      if (jstatus != nullptr) {
        env->DeleteLocalRef(jstatus);
      }
      if (jmsg != nullptr) {
        env->DeleteLocalRef(jmsg);
      }
      if (rocksdb_exception != nullptr) {
        env->DeleteLocalRef(rocksdb_exception);
      }
      return env->ExceptionCheck();
    }

    if (jstatus != nullptr) {
      env->DeleteLocalRef(jstatus);
    }
    if (jmsg != nullptr) {
      env->DeleteLocalRef(jmsg);
    }
    if (rocksdb_exception != nullptr) {
      env->DeleteLocalRef(rocksdb_exception);
    }

    return true;
  }

  /**
   * Get the Java Method: RocksDBException#getStatus
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getStatusMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "getStatus", "()Lorg/rocksdb/Status;");
    assert(mid != nullptr);
    return mid;
  }

  static std::unique_ptr<TERARKDB_NAMESPACE::Status> toCppStatus(
      JNIEnv* env, jthrowable jrocksdb_exception) {
    if (!env->IsInstanceOf(jrocksdb_exception, getJClass(env))) {
      // not an instance of RocksDBException
      return nullptr;
    }

    // get the java status object
    jmethodID mid = getStatusMethod(env);
    if (mid == nullptr) {
      // exception occurred accessing class or method
      return nullptr;
    }

    jobject jstatus = env->CallObjectMethod(jrocksdb_exception, mid);
    if (env->ExceptionCheck()) {
      // exception occurred
      return nullptr;
    }

    if (jstatus == nullptr) {
      return nullptr;  // no status available
    }

    return TERARKDB_NAMESPACE::StatusJni::toCppStatus(env, jstatus);
  }
};

// The portal class for java.lang.IllegalArgumentException
class IllegalArgumentExceptionJni
    : public JavaException<IllegalArgumentExceptionJni> {
 public:
  /**
   * Get the Java Class java.lang.IllegalArgumentException
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaException::getJClass(env, "java/lang/IllegalArgumentException");
  }

  /**
   * Create and throw a Java IllegalArgumentException with the provided status
   *
   * If s.ok() == true, then this function will not throw any exception.
   *
   * @param env A pointer to the Java environment
   * @param s The status for the exception
   *
   * @return true if an exception was thrown, false otherwise
   */
  static bool ThrowNew(JNIEnv* env, const Status& s) {
    assert(!s.ok());
    if (s.ok()) {
      return false;
    }

    // get the IllegalArgumentException class
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      std::cerr << "IllegalArgumentExceptionJni::ThrowNew/class - Error: "
                   "unexpected exception!"
                << std::endl;
      return env->ExceptionCheck();
    }

    return JavaException::ThrowNew(env, s.ToString());
  }
};

// The portal class for org.rocksdb.Options
class OptionsJni
    : public RocksDBNativeClass<TERARKDB_NAMESPACE::Options*, OptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.Options
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/Options");
  }
};

// The portal class for org.rocksdb.DBOptions
class DBOptionsJni
    : public RocksDBNativeClass<TERARKDB_NAMESPACE::DBOptions*, DBOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.DBOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/DBOptions");
  }
};

// The portal class for org.rocksdb.ColumnFamilyOptions
class ColumnFamilyOptionsJni
    : public RocksDBNativeClass<TERARKDB_NAMESPACE::ColumnFamilyOptions*,
                                ColumnFamilyOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.ColumnFamilyOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
                                         "org/rocksdb/ColumnFamilyOptions");
  }

  /**
   * Create a new Java org.rocksdb.ColumnFamilyOptions object with the same
   * properties as the provided C++ TERARKDB_NAMESPACE::ColumnFamilyOptions
   * object
   *
   * @param env A pointer to the Java environment
   * @param cfoptions A pointer to TERARKDB_NAMESPACE::ColumnFamilyOptions
   * object
   *
   * @return A reference to a Java org.rocksdb.ColumnFamilyOptions object, or
   * nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env, const ColumnFamilyOptions* cfoptions) {
    auto* cfo = new TERARKDB_NAMESPACE::ColumnFamilyOptions(*cfoptions);
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>", "(J)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jobject jcfd = env->NewObject(jclazz, mid, reinterpret_cast<jlong>(cfo));
    if (env->ExceptionCheck()) {
      return nullptr;
    }

    return jcfd;
  }
};

// The portal class for org.rocksdb.WriteOptions
class WriteOptionsJni
    : public RocksDBNativeClass<TERARKDB_NAMESPACE::WriteOptions*,
                                WriteOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.WriteOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/WriteOptions");
  }
};

// The portal class for org.rocksdb.ReadOptions
class ReadOptionsJni
    : public RocksDBNativeClass<TERARKDB_NAMESPACE::ReadOptions*,
                                ReadOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.ReadOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/ReadOptions");
  }
};

// The portal class for org.rocksdb.WriteBatch
class WriteBatchJni : public RocksDBNativeClass<TERARKDB_NAMESPACE::WriteBatch*,
                                                WriteBatchJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.WriteBatch
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/WriteBatch");
  }

  /**
   * Create a new Java org.rocksdb.WriteBatch object
   *
   * @param env A pointer to the Java environment
   * @param wb A pointer to TERARKDB_NAMESPACE::WriteBatch object
   *
   * @return A reference to a Java org.rocksdb.WriteBatch object, or
   * nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env, const WriteBatch* wb) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>", "(J)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jobject jwb = env->NewObject(jclazz, mid, reinterpret_cast<jlong>(wb));
    if (env->ExceptionCheck()) {
      return nullptr;
    }

    return jwb;
  }
};

// The portal class for org.rocksdb.WriteBatch.Handler
class WriteBatchHandlerJni
    : public RocksDBNativeClass<
          const TERARKDB_NAMESPACE::WriteBatchHandlerJniCallback*,
          WriteBatchHandlerJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.WriteBatch.Handler
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/WriteBatch$Handler");
  }

  /**
   * Get the Java Method: WriteBatch.Handler#put
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getPutCfMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "put", "(I[B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#put
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getPutMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "put", "([B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#merge
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getMergeCfMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "merge", "(I[B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#merge
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getMergeMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "merge", "([B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#delete
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getDeleteCfMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "delete", "(I[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#delete
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getDeleteMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "delete", "([B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#singleDelete
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getSingleDeleteCfMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "singleDelete", "(I[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#singleDelete
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getSingleDeleteMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "singleDelete", "([B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#deleteRange
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getDeleteRangeCfMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "deleteRange", "(I[B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#deleteRange
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getDeleteRangeMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "deleteRange", "([B[B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#logData
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getLogDataMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "logData", "([B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#putBlobIndex
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getPutBlobIndexCfMethodId(JNIEnv* env) {
    // jclass jclazz = getJClass(env);
    // if(jclazz == nullptr) {
    //  // exception occurred accessing class
    //  return nullptr;
    //}

    // static jmethodID mid = env->GetMethodID(jclazz, "putBlobIndex",
    // "(I[B[B)V"); assert(mid != nullptr); return mid;
    return nullptr;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#markBeginPrepare
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getMarkBeginPrepareMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "markBeginPrepare", "()V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#markEndPrepare
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getMarkEndPrepareMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "markEndPrepare", "([B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#markNoop
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getMarkNoopMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "markNoop", "(Z)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#markRollback
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getMarkRollbackMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "markRollback", "([B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#markCommit
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getMarkCommitMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "markCommit", "([B)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: WriteBatch.Handler#shouldContinue
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getContinueMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "shouldContinue", "()Z");
    assert(mid != nullptr);
    return mid;
  }
};

class WriteBatchSavePointJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.WriteBatch.SavePoint
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/WriteBatch$SavePoint");
  }

  /**
   * Get the Java Method: HistogramData constructor
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getConstructorMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "<init>", "(JJJ)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Create a new Java org.rocksdb.WriteBatch.SavePoint object
   *
   * @param env A pointer to the Java environment
   * @param savePoint A pointer to TERARKDB_NAMESPACE::WriteBatch::SavePoint
   * object
   *
   * @return A reference to a Java org.rocksdb.WriteBatch.SavePoint object, or
   * nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env, const SavePoint& save_point) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = getConstructorMethodId(env);
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jobject jsave_point =
        env->NewObject(jclazz, mid, static_cast<jlong>(save_point.size),
                       static_cast<jlong>(save_point.count),
                       static_cast<jlong>(save_point.content_flags));
    if (env->ExceptionCheck()) {
      return nullptr;
    }

    return jsave_point;
  }
};

// The portal class for org.rocksdb.WriteBatchWithIndex
class WriteBatchWithIndexJni
    : public RocksDBNativeClass<TERARKDB_NAMESPACE::WriteBatchWithIndex*,
                                WriteBatchWithIndexJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.WriteBatchWithIndex
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
                                         "org/rocksdb/WriteBatchWithIndex");
  }
};

// The portal class for org.rocksdb.HistogramData
class HistogramDataJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.HistogramData
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/HistogramData");
  }

  /**
   * Get the Java Method: HistogramData constructor
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getConstructorMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "<init>", "(DDDDD)V");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.BackupableDBOptions
class BackupableDBOptionsJni
    : public RocksDBNativeClass<TERARKDB_NAMESPACE::BackupableDBOptions*,
                                BackupableDBOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.BackupableDBOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env,
                                         "org/rocksdb/BackupableDBOptions");
  }
};

// The portal class for org.rocksdb.BackupEngine
class BackupEngineJni
    : public RocksDBNativeClass<TERARKDB_NAMESPACE::BackupEngine*,
                                BackupEngineJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.BackupableEngine
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/BackupEngine");
  }
};

// The portal class for org.rocksdb.RocksIterator
class IteratorJni
    : public RocksDBNativeClass<TERARKDB_NAMESPACE::Iterator*, IteratorJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.RocksIterator
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/RocksIterator");
  }
};

// The portal class for org.rocksdb.Filter
class FilterJni
    : public RocksDBNativeClass<
          std::shared_ptr<TERARKDB_NAMESPACE::FilterPolicy>*, FilterJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.Filter
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/Filter");
  }
};

// The portal class for org.rocksdb.ColumnFamilyHandle
class ColumnFamilyHandleJni
    : public RocksDBNativeClass<TERARKDB_NAMESPACE::ColumnFamilyHandle*,
                                ColumnFamilyHandleJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.ColumnFamilyHandle
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/ColumnFamilyHandle");
  }
};

// The portal class for org.rocksdb.FlushOptions
class FlushOptionsJni
    : public RocksDBNativeClass<TERARKDB_NAMESPACE::FlushOptions*,
                                FlushOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.FlushOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/FlushOptions");
  }
};

// The portal class for org.rocksdb.ComparatorOptions
class ComparatorOptionsJni
    : public RocksDBNativeClass<
          TERARKDB_NAMESPACE::ComparatorJniCallbackOptions*,
          ComparatorOptionsJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.ComparatorOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/ComparatorOptions");
  }
};

// The portal class for org.rocksdb.AbstractCompactionFilterFactory
class AbstractCompactionFilterFactoryJni
    : public RocksDBNativeClass<
          const TERARKDB_NAMESPACE::CompactionFilterFactoryJniCallback*,
          AbstractCompactionFilterFactoryJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.AbstractCompactionFilterFactory
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(
        env, "org/rocksdb/AbstractCompactionFilterFactory");
  }

  /**
   * Get the Java Method: AbstractCompactionFilterFactory#name
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getNameMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "name", "()Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: AbstractCompactionFilterFactory#createCompactionFilter
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getCreateCompactionFilterMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "createCompactionFilter", "(ZZ)J");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.AbstractTransactionNotifier
class AbstractTransactionNotifierJni
    : public RocksDBNativeClass<
          const TERARKDB_NAMESPACE::TransactionNotifierJniCallback*,
          AbstractTransactionNotifierJni> {
 public:
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(
        env, "org/rocksdb/AbstractTransactionNotifier");
  }

  // Get the java method `snapshotCreated`
  // of org.rocksdb.AbstractTransactionNotifier.
  static jmethodID getSnapshotCreatedMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "snapshotCreated", "(J)V");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.AbstractComparator
class AbstractComparatorJni
    : public RocksDBNativeClass<
          const TERARKDB_NAMESPACE::BaseComparatorJniCallback*,
          AbstractComparatorJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.AbstractComparator
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/AbstractComparator");
  }

  /**
   * Get the Java Method: Comparator#name
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getNameMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "name", "()Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Comparator#compare
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getCompareMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(
        jclazz, "compare",
        "(Lorg/rocksdb/AbstractSlice;Lorg/rocksdb/AbstractSlice;)I");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Comparator#findShortestSeparator
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getFindShortestSeparatorMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(
        jclazz, "findShortestSeparator",
        "(Ljava/lang/String;Lorg/rocksdb/AbstractSlice;)Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Comparator#findShortSuccessor
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getFindShortSuccessorMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(
        jclazz, "findShortSuccessor", "(Ljava/lang/String;)Ljava/lang/String;");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.AbstractSlice
class AbstractSliceJni
    : public NativeRocksMutableObject<const TERARKDB_NAMESPACE::Slice*,
                                      AbstractSliceJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.AbstractSlice
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/AbstractSlice");
  }
};

// The portal class for org.rocksdb.Slice
class SliceJni
    : public NativeRocksMutableObject<const TERARKDB_NAMESPACE::Slice*,
                                      AbstractSliceJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.Slice
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/Slice");
  }

  /**
   * Constructs a Slice object
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to a Java Slice object, or a nullptr if an
   *     exception occurs
   */
  static jobject construct0(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "<init>", "()V");
    if (mid == nullptr) {
      // exception occurred accessing method
      return nullptr;
    }

    jobject jslice = env->NewObject(jclazz, mid);
    if (env->ExceptionCheck()) {
      return nullptr;
    }

    return jslice;
  }
};

// The portal class for org.rocksdb.DirectSlice
class DirectSliceJni
    : public NativeRocksMutableObject<const TERARKDB_NAMESPACE::Slice*,
                                      AbstractSliceJni> {
 public:
  /**
   * Get the Java Class org.rocksdb.DirectSlice
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/DirectSlice");
  }

  /**
   * Constructs a DirectSlice object
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to a Java DirectSlice object, or a nullptr if an
   *     exception occurs
   */
  static jobject construct0(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "<init>", "()V");
    if (mid == nullptr) {
      // exception occurred accessing method
      return nullptr;
    }

    jobject jdirect_slice = env->NewObject(jclazz, mid);
    if (env->ExceptionCheck()) {
      return nullptr;
    }

    return jdirect_slice;
  }
};

// The portal class for java.util.List
class ListJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.util.List
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getListClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/util/List");
  }

  /**
   * Get the Java Class java.util.ArrayList
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getArrayListClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/util/ArrayList");
  }

  /**
   * Get the Java Class java.util.Iterator
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getIteratorClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/util/Iterator");
  }

  /**
   * Get the Java Method: List#iterator
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getIteratorMethod(JNIEnv* env) {
    jclass jlist_clazz = getListClass(env);
    if (jlist_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jlist_clazz, "iterator", "()Ljava/util/Iterator;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Iterator#hasNext
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getHasNextMethod(JNIEnv* env) {
    jclass jiterator_clazz = getIteratorClass(env);
    if (jiterator_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jiterator_clazz, "hasNext", "()Z");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: Iterator#next
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getNextMethod(JNIEnv* env) {
    jclass jiterator_clazz = getIteratorClass(env);
    if (jiterator_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jiterator_clazz, "next", "()Ljava/lang/Object;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: ArrayList constructor
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getArrayListConstructorMethodId(JNIEnv* env) {
    jclass jarray_list_clazz = getArrayListClass(env);
    if (jarray_list_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }
    static jmethodID mid =
        env->GetMethodID(jarray_list_clazz, "<init>", "(I)V");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: List#add
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getListAddMethodId(JNIEnv* env) {
    jclass jlist_clazz = getListClass(env);
    if (jlist_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jlist_clazz, "add", "(Ljava/lang/Object;)Z");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for java.lang.Byte
class ByteJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.lang.Byte
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/lang/Byte");
  }

  /**
   * Get the Java Class byte[]
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getArrayJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "[B");
  }

  /**
   * Creates a new 2-dimensional Java Byte Array byte[][]
   *
   * @param env A pointer to the Java environment
   * @param len The size of the first dimension
   *
   * @return A reference to the Java byte[][] or nullptr if an exception occurs
   */
  static jobjectArray new2dByteArray(JNIEnv* env, const jsize len) {
    jclass clazz = getArrayJClass(env);
    if (clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    return env->NewObjectArray(len, clazz, nullptr);
  }

  /**
   * Get the Java Method: Byte#byteValue
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retrieved
   */
  static jmethodID getByteValueMethod(JNIEnv* env) {
    jclass clazz = getJClass(env);
    if (clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(clazz, "byteValue", "()B");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Calls the Java Method: Byte#valueOf, returning a constructed Byte jobject
   *
   * @param env A pointer to the Java environment
   *
   * @return A constructing Byte object or nullptr if the class or method id
   * could not be retrieved, or an exception occurred
   */
  static jobject valueOf(JNIEnv* env, jbyte jprimitive_byte) {
    jclass clazz = getJClass(env);
    if (clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetStaticMethodID(clazz, "valueOf", "(B)Ljava/lang/Byte;");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    const jobject jbyte_obj =
        env->CallStaticObjectMethod(clazz, mid, jprimitive_byte);
    if (env->ExceptionCheck()) {
      // exception occurred
      return nullptr;
    }

    return jbyte_obj;
  }
};

// The portal class for java.lang.StringBuilder
class StringBuilderJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.lang.StringBuilder
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/lang/StringBuilder");
  }

  /**
   * Get the Java Method: StringBuilder#append
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getListAddMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(
        jclazz, "append", "(Ljava/lang/String;)Ljava/lang/StringBuilder;");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Appends a C-style string to a StringBuilder
   *
   * @param env A pointer to the Java environment
   * @param jstring_builder Reference to a java.lang.StringBuilder
   * @param c_str A C-style string to append to the StringBuilder
   *
   * @return A reference to the updated StringBuilder, or a nullptr if
   *     an exception occurs
   */
  static jobject append(JNIEnv* env, jobject jstring_builder,
                        const char* c_str) {
    jmethodID mid = getListAddMethodId(env);
    if (mid == nullptr) {
      // exception occurred accessing class or method
      return nullptr;
    }

    jstring new_value_str = env->NewStringUTF(c_str);
    if (new_value_str == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    jobject jresult_string_builder =
        env->CallObjectMethod(jstring_builder, mid, new_value_str);
    if (env->ExceptionCheck()) {
      // exception occurred
      env->DeleteLocalRef(new_value_str);
      return nullptr;
    }

    return jresult_string_builder;
  }
};

// The portal class for org.rocksdb.BackupInfo
class BackupInfoJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.BackupInfo
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/BackupInfo");
  }

  /**
   * Constructs a BackupInfo object
   *
   * @param env A pointer to the Java environment
   * @param backup_id id of the backup
   * @param timestamp timestamp of the backup
   * @param size size of the backup
   * @param number_files number of files related to the backup
   * @param app_metadata application specific metadata
   *
   * @return A reference to a Java BackupInfo object, or a nullptr if an
   *     exception occurs
   */
  static jobject construct0(JNIEnv* env, uint32_t backup_id, int64_t timestamp,
                            uint64_t size, uint32_t number_files,
                            const std::string& app_metadata) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid =
        env->GetMethodID(jclazz, "<init>", "(IJJILjava/lang/String;)V");
    if (mid == nullptr) {
      // exception occurred accessing method
      return nullptr;
    }

    jstring japp_metadata = nullptr;
    if (app_metadata != nullptr) {
      japp_metadata = env->NewStringUTF(app_metadata.c_str());
      if (japp_metadata == nullptr) {
        // exception occurred creating java string
        return nullptr;
      }
    }

    jobject jbackup_info = env->NewObject(jclazz, mid, backup_id, timestamp,
                                          size, number_files, japp_metadata);
    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(japp_metadata);
      return nullptr;
    }

    return jbackup_info;
  }
};

class BackupInfoListJni {
 public:
  /**
   * Converts a C++ std::vector<BackupInfo> object to
   * a Java ArrayList<org.rocksdb.BackupInfo> object
   *
   * @param env A pointer to the Java environment
   * @param backup_infos A vector of BackupInfo
   *
   * @return Either a reference to a Java ArrayList object, or a nullptr
   *     if an exception occurs
   */
  static jobject getBackupInfo(JNIEnv* env,
                               std::vector<BackupInfo> backup_infos) {
    jclass jarray_list_clazz =
        TERARKDB_NAMESPACE::ListJni::getArrayListClass(env);
    if (jarray_list_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID cstr_mid =
        TERARKDB_NAMESPACE::ListJni::getArrayListConstructorMethodId(env);
    if (cstr_mid == nullptr) {
      // exception occurred accessing method
      return nullptr;
    }

    jmethodID add_mid = TERARKDB_NAMESPACE::ListJni::getListAddMethodId(env);
    if (add_mid == nullptr) {
      // exception occurred accessing method
      return nullptr;
    }

    // create java list
    jobject jbackup_info_handle_list =
        env->NewObject(jarray_list_clazz, cstr_mid, backup_infos.size());
    if (env->ExceptionCheck()) {
      // exception occurred constructing object
      return nullptr;
    }

    // insert in java list
    auto end = backup_infos.end();
    for (auto it = backup_infos.begin(); it != end; ++it) {
      auto backup_info = *it;

      jobject obj = TERARKDB_NAMESPACE::BackupInfoJni::construct0(
          env, backup_info.backup_id, backup_info.timestamp, backup_info.size,
          backup_info.number_files, backup_info.app_metadata);
      if (env->ExceptionCheck()) {
        // exception occurred constructing object
        if (obj != nullptr) {
          env->DeleteLocalRef(obj);
        }
        if (jbackup_info_handle_list != nullptr) {
          env->DeleteLocalRef(jbackup_info_handle_list);
        }
        return nullptr;
      }

      jboolean rs =
          env->CallBooleanMethod(jbackup_info_handle_list, add_mid, obj);
      if (env->ExceptionCheck() || rs == JNI_FALSE) {
        // exception occurred calling method, or could not add
        if (obj != nullptr) {
          env->DeleteLocalRef(obj);
        }
        if (jbackup_info_handle_list != nullptr) {
          env->DeleteLocalRef(jbackup_info_handle_list);
        }
        return nullptr;
      }
    }

    return jbackup_info_handle_list;
  }
};

// The portal class for org.rocksdb.WBWIRocksIterator
class WBWIRocksIteratorJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.WBWIRocksIterator
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/WBWIRocksIterator");
  }

  /**
   * Get the Java Field: WBWIRocksIterator#entry
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Field ID or nullptr if the class or field id could not
   *     be retieved
   */
  static jfieldID getWriteEntryField(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jfieldID fid = env->GetFieldID(
        jclazz, "entry", "Lorg/rocksdb/WBWIRocksIterator$WriteEntry;");
    assert(fid != nullptr);
    return fid;
  }

  /**
   * Gets the value of the WBWIRocksIterator#entry
   *
   * @param env A pointer to the Java environment
   * @param jwbwi_rocks_iterator A reference to a WBWIIterator
   *
   * @return A reference to a Java WBWIRocksIterator.WriteEntry object, or
   *     a nullptr if an exception occurs
   */
  static jobject getWriteEntry(JNIEnv* env, jobject jwbwi_rocks_iterator) {
    assert(jwbwi_rocks_iterator != nullptr);

    jfieldID jwrite_entry_field = getWriteEntryField(env);
    if (jwrite_entry_field == nullptr) {
      // exception occurred accessing the field
      return nullptr;
    }

    jobject jwe = env->GetObjectField(jwbwi_rocks_iterator, jwrite_entry_field);
    assert(jwe != nullptr);
    return jwe;
  }
};

// The portal class for org.rocksdb.WBWIRocksIterator.WriteType
class WriteTypeJni : public JavaClass {
 public:
  /**
   * Get the PUT enum field value of WBWIRocksIterator.WriteType
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject PUT(JNIEnv* env) { return getEnum(env, "PUT"); }

  /**
   * Get the MERGE enum field value of WBWIRocksIterator.WriteType
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject MERGE(JNIEnv* env) { return getEnum(env, "MERGE"); }

  /**
   * Get the DELETE enum field value of WBWIRocksIterator.WriteType
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject DELETE(JNIEnv* env) { return getEnum(env, "DELETE"); }

  /**
   * Get the LOG enum field value of WBWIRocksIterator.WriteType
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject LOG(JNIEnv* env) { return getEnum(env, "LOG"); }

  // Returns the equivalent org.rocksdb.WBWIRocksIterator.WriteType for the
  // provided C++ TERARKDB_NAMESPACE::WriteType enum
  static jbyte toJavaWriteType(const TERARKDB_NAMESPACE::WriteType& writeType) {
    switch (writeType) {
      case TERARKDB_NAMESPACE::WriteType::kPutRecord:
        return 0x0;
      case TERARKDB_NAMESPACE::WriteType::kMergeRecord:
        return 0x1;
      case TERARKDB_NAMESPACE::WriteType::kDeleteRecord:
        return 0x2;
      case TERARKDB_NAMESPACE::WriteType::kSingleDeleteRecord:
        return 0x3;
      case TERARKDB_NAMESPACE::WriteType::kDeleteRangeRecord:
        return 0x4;
      case TERARKDB_NAMESPACE::WriteType::kLogDataRecord:
        return 0x5;
      case TERARKDB_NAMESPACE::WriteType::kXIDRecord:
        return 0x6;
      default:
        return 0x7F;  // undefined
    }
  }

 private:
  /**
   * Get the Java Class org.rocksdb.WBWIRocksIterator.WriteType
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/WBWIRocksIterator$WriteType");
  }

  /**
   * Get an enum field of org.rocksdb.WBWIRocksIterator.WriteType
   *
   * @param env A pointer to the Java environment
   * @param name The name of the enum field
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject getEnum(JNIEnv* env, const char name[]) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jfieldID jfid = env->GetStaticFieldID(
        jclazz, name, "Lorg/rocksdb/WBWIRocksIterator$WriteType;");
    if (env->ExceptionCheck()) {
      // exception occurred while getting field
      return nullptr;
    } else if (jfid == nullptr) {
      return nullptr;
    }

    jobject jwrite_type = env->GetStaticObjectField(jclazz, jfid);
    assert(jwrite_type != nullptr);
    return jwrite_type;
  }
};

// The portal class for org.rocksdb.WBWIRocksIterator.WriteEntry
class WriteEntryJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.WBWIRocksIterator.WriteEntry
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env,
                                "org/rocksdb/WBWIRocksIterator$WriteEntry");
  }
};

// The portal class for org.rocksdb.InfoLogLevel
class InfoLogLevelJni : public JavaClass {
 public:
  /**
   * Get the DEBUG_LEVEL enum field value of InfoLogLevel
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject DEBUG_LEVEL(JNIEnv* env) {
    return getEnum(env, "DEBUG_LEVEL");
  }

  /**
   * Get the INFO_LEVEL enum field value of InfoLogLevel
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject INFO_LEVEL(JNIEnv* env) { return getEnum(env, "INFO_LEVEL"); }

  /**
   * Get the WARN_LEVEL enum field value of InfoLogLevel
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject WARN_LEVEL(JNIEnv* env) { return getEnum(env, "WARN_LEVEL"); }

  /**
   * Get the ERROR_LEVEL enum field value of InfoLogLevel
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject ERROR_LEVEL(JNIEnv* env) {
    return getEnum(env, "ERROR_LEVEL");
  }

  /**
   * Get the FATAL_LEVEL enum field value of InfoLogLevel
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject FATAL_LEVEL(JNIEnv* env) {
    return getEnum(env, "FATAL_LEVEL");
  }

  /**
   * Get the HEADER_LEVEL enum field value of InfoLogLevel
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject HEADER_LEVEL(JNIEnv* env) {
    return getEnum(env, "HEADER_LEVEL");
  }

 private:
  /**
   * Get the Java Class org.rocksdb.InfoLogLevel
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/InfoLogLevel");
  }

  /**
   * Get an enum field of org.rocksdb.InfoLogLevel
   *
   * @param env A pointer to the Java environment
   * @param name The name of the enum field
   *
   * @return A reference to the enum field value or a nullptr if
   *     the enum field value could not be retrieved
   */
  static jobject getEnum(JNIEnv* env, const char name[]) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jfieldID jfid =
        env->GetStaticFieldID(jclazz, name, "Lorg/rocksdb/InfoLogLevel;");
    if (env->ExceptionCheck()) {
      // exception occurred while getting field
      return nullptr;
    } else if (jfid == nullptr) {
      return nullptr;
    }

    jobject jinfo_log_level = env->GetStaticObjectField(jclazz, jfid);
    assert(jinfo_log_level != nullptr);
    return jinfo_log_level;
  }
};

// The portal class for org.rocksdb.Logger
class LoggerJni
    : public RocksDBNativeClass<
          std::shared_ptr<TERARKDB_NAMESPACE::LoggerJniCallback>*, LoggerJni> {
 public:
  /**
   * Get the Java Class org/rocksdb/Logger
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return RocksDBNativeClass::getJClass(env, "org/rocksdb/Logger");
  }

  /**
   * Get the Java Method: Logger#log
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getLogMethodId(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(
        jclazz, "log", "(Lorg/rocksdb/InfoLogLevel;Ljava/lang/String;)V");
    assert(mid != nullptr);
    return mid;
  }
};

// The portal class for org.rocksdb.TransactionLogIterator.BatchResult
class BatchResultJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.TransactionLogIterator.BatchResult
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(
        env, "org/rocksdb/TransactionLogIterator$BatchResult");
  }

  /**
   * Create a new Java org.rocksdb.TransactionLogIterator.BatchResult object
   * with the same properties as the provided C++
   * TERARKDB_NAMESPACE::BatchResult object
   *
   * @param env A pointer to the Java environment
   * @param batch_result The TERARKDB_NAMESPACE::BatchResult object
   *
   * @return A reference to a Java
   *     org.rocksdb.TransactionLogIterator.BatchResult object,
   *     or nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env,
                           TERARKDB_NAMESPACE::BatchResult& batch_result) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>", "(JJ)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jobject jbatch_result = env->NewObject(jclazz, mid, batch_result.sequence,
                                           batch_result.writeBatchPtr.get());
    if (jbatch_result == nullptr) {
      // exception thrown: InstantiationException or OutOfMemoryError
      return nullptr;
    }

    batch_result.writeBatchPtr.release();
    return jbatch_result;
  }
};

// The portal class for org.rocksdb.BottommostLevelCompaction
class BottommostLevelCompactionJni {
 public:
  // Returns the equivalent org.rocksdb.BottommostLevelCompaction for the
  // provided C++ TERARKDB_NAMESPACE::BottommostLevelCompaction enum
  static jint toJavaBottommostLevelCompaction(
      const TERARKDB_NAMESPACE::BottommostLevelCompaction&
          bottommost_level_compaction) {
    switch (bottommost_level_compaction) {
      case TERARKDB_NAMESPACE::BottommostLevelCompaction::kSkip:
        return 0x0;
      case TERARKDB_NAMESPACE::BottommostLevelCompaction::
          kIfHaveCompactionFilter:
        return 0x1;
      case TERARKDB_NAMESPACE::BottommostLevelCompaction::kForce:
        return 0x2;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ TERARKDB_NAMESPACE::BottommostLevelCompaction
  // enum for the provided Java org.rocksdb.BottommostLevelCompaction
  static TERARKDB_NAMESPACE::BottommostLevelCompaction
  toCppBottommostLevelCompaction(jint bottommost_level_compaction) {
    switch (bottommost_level_compaction) {
      case 0x0:
        return TERARKDB_NAMESPACE::BottommostLevelCompaction::kSkip;
      case 0x1:
        return TERARKDB_NAMESPACE::BottommostLevelCompaction::
            kIfHaveCompactionFilter;
      case 0x2:
        return TERARKDB_NAMESPACE::BottommostLevelCompaction::kForce;
      default:
        // undefined/default
        return TERARKDB_NAMESPACE::BottommostLevelCompaction::
            kIfHaveCompactionFilter;
    }
  }
};

// The portal class for org.rocksdb.CompactionStopStyle
class CompactionStopStyleJni {
 public:
  // Returns the equivalent org.rocksdb.CompactionStopStyle for the provided
  // C++ TERARKDB_NAMESPACE::CompactionStopStyle enum
  static jbyte toJavaCompactionStopStyle(
      const TERARKDB_NAMESPACE::CompactionStopStyle& compaction_stop_style) {
    switch (compaction_stop_style) {
      case TERARKDB_NAMESPACE::CompactionStopStyle::
          kCompactionStopStyleSimilarSize:
        return 0x0;
      case TERARKDB_NAMESPACE::CompactionStopStyle::
          kCompactionStopStyleTotalSize:
        return 0x1;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ TERARKDB_NAMESPACE::CompactionStopStyle enum for
  // the provided Java org.rocksdb.CompactionStopStyle
  static TERARKDB_NAMESPACE::CompactionStopStyle toCppCompactionStopStyle(
      jbyte jcompaction_stop_style) {
    switch (jcompaction_stop_style) {
      case 0x0:
        return TERARKDB_NAMESPACE::CompactionStopStyle::
            kCompactionStopStyleSimilarSize;
      case 0x1:
        return TERARKDB_NAMESPACE::CompactionStopStyle::
            kCompactionStopStyleTotalSize;
      default:
        // undefined/default
        return TERARKDB_NAMESPACE::CompactionStopStyle::
            kCompactionStopStyleSimilarSize;
    }
  }
};

// The portal class for org.rocksdb.CompressionType
class CompressionTypeJni {
 public:
  // Returns the equivalent org.rocksdb.CompressionType for the provided
  // C++ TERARKDB_NAMESPACE::CompressionType enum
  static jbyte toJavaCompressionType(
      const TERARKDB_NAMESPACE::CompressionType& compression_type) {
    switch (compression_type) {
      case TERARKDB_NAMESPACE::CompressionType::kNoCompression:
        return 0x0;
      case TERARKDB_NAMESPACE::CompressionType::kSnappyCompression:
        return 0x1;
      case TERARKDB_NAMESPACE::CompressionType::kZlibCompression:
        return 0x2;
      case TERARKDB_NAMESPACE::CompressionType::kBZip2Compression:
        return 0x3;
      case TERARKDB_NAMESPACE::CompressionType::kLZ4Compression:
        return 0x4;
      case TERARKDB_NAMESPACE::CompressionType::kLZ4HCCompression:
        return 0x5;
      case TERARKDB_NAMESPACE::CompressionType::kXpressCompression:
        return 0x6;
      case TERARKDB_NAMESPACE::CompressionType::kZSTD:
        return 0x7;
      case TERARKDB_NAMESPACE::CompressionType::kDisableCompressionOption:
      default:
        return 0x7F;
    }
  }

  // Returns the equivalent C++ TERARKDB_NAMESPACE::CompressionType enum for the
  // provided Java org.rocksdb.CompressionType
  static TERARKDB_NAMESPACE::CompressionType toCppCompressionType(
      jbyte jcompression_type) {
    switch (jcompression_type) {
      case 0x0:
        return TERARKDB_NAMESPACE::CompressionType::kNoCompression;
      case 0x1:
        return TERARKDB_NAMESPACE::CompressionType::kSnappyCompression;
      case 0x2:
        return TERARKDB_NAMESPACE::CompressionType::kZlibCompression;
      case 0x3:
        return TERARKDB_NAMESPACE::CompressionType::kBZip2Compression;
      case 0x4:
        return TERARKDB_NAMESPACE::CompressionType::kLZ4Compression;
      case 0x5:
        return TERARKDB_NAMESPACE::CompressionType::kLZ4HCCompression;
      case 0x6:
        return TERARKDB_NAMESPACE::CompressionType::kXpressCompression;
      case 0x7:
        return TERARKDB_NAMESPACE::CompressionType::kZSTD;
      case 0x7F:
      default:
        return TERARKDB_NAMESPACE::CompressionType::kDisableCompressionOption;
    }
  }
};

// The portal class for org.rocksdb.CompactionPriority
class CompactionPriorityJni {
 public:
  // Returns the equivalent org.rocksdb.CompactionPriority for the provided
  // C++ TERARKDB_NAMESPACE::CompactionPri enum
  static jbyte toJavaCompactionPriority(
      const TERARKDB_NAMESPACE::CompactionPri& compaction_priority) {
    switch (compaction_priority) {
      case TERARKDB_NAMESPACE::CompactionPri::kByCompensatedSize:
        return 0x0;
      case TERARKDB_NAMESPACE::CompactionPri::kOldestLargestSeqFirst:
        return 0x1;
      case TERARKDB_NAMESPACE::CompactionPri::kOldestSmallestSeqFirst:
        return 0x2;
      case TERARKDB_NAMESPACE::CompactionPri::kMinOverlappingRatio:
        return 0x3;
      default:
        return 0x0;  // undefined
    }
  }

  // Returns the equivalent C++ TERARKDB_NAMESPACE::CompactionPri enum for the
  // provided Java org.rocksdb.CompactionPriority
  static TERARKDB_NAMESPACE::CompactionPri toCppCompactionPriority(
      jbyte jcompaction_priority) {
    switch (jcompaction_priority) {
      case 0x0:
        return TERARKDB_NAMESPACE::CompactionPri::kByCompensatedSize;
      case 0x1:
        return TERARKDB_NAMESPACE::CompactionPri::kOldestLargestSeqFirst;
      case 0x2:
        return TERARKDB_NAMESPACE::CompactionPri::kOldestSmallestSeqFirst;
      case 0x3:
        return TERARKDB_NAMESPACE::CompactionPri::kMinOverlappingRatio;
      default:
        // undefined/default
        return TERARKDB_NAMESPACE::CompactionPri::kByCompensatedSize;
    }
  }
};

// The portal class for org.rocksdb.AccessHint
class AccessHintJni {
 public:
  // Returns the equivalent org.rocksdb.AccessHint for the provided
  // C++ TERARKDB_NAMESPACE::DBOptions::AccessHint enum
  static jbyte toJavaAccessHint(
      const TERARKDB_NAMESPACE::DBOptions::AccessHint& access_hint) {
    switch (access_hint) {
      case TERARKDB_NAMESPACE::DBOptions::AccessHint::NONE:
        return 0x0;
      case TERARKDB_NAMESPACE::DBOptions::AccessHint::NORMAL:
        return 0x1;
      case TERARKDB_NAMESPACE::DBOptions::AccessHint::SEQUENTIAL:
        return 0x2;
      case TERARKDB_NAMESPACE::DBOptions::AccessHint::WILLNEED:
        return 0x3;
      default:
        // undefined/default
        return 0x1;
    }
  }

  // Returns the equivalent C++ TERARKDB_NAMESPACE::DBOptions::AccessHint enum
  // for the provided Java org.rocksdb.AccessHint
  static TERARKDB_NAMESPACE::DBOptions::AccessHint toCppAccessHint(
      jbyte jaccess_hint) {
    switch (jaccess_hint) {
      case 0x0:
        return TERARKDB_NAMESPACE::DBOptions::AccessHint::NONE;
      case 0x1:
        return TERARKDB_NAMESPACE::DBOptions::AccessHint::NORMAL;
      case 0x2:
        return TERARKDB_NAMESPACE::DBOptions::AccessHint::SEQUENTIAL;
      case 0x3:
        return TERARKDB_NAMESPACE::DBOptions::AccessHint::WILLNEED;
      default:
        // undefined/default
        return TERARKDB_NAMESPACE::DBOptions::AccessHint::NORMAL;
    }
  }
};

// The portal class for org.rocksdb.WALRecoveryMode
class WALRecoveryModeJni {
 public:
  // Returns the equivalent org.rocksdb.WALRecoveryMode for the provided
  // C++ TERARKDB_NAMESPACE::WALRecoveryMode enum
  static jbyte toJavaWALRecoveryMode(
      const TERARKDB_NAMESPACE::WALRecoveryMode& wal_recovery_mode) {
    switch (wal_recovery_mode) {
      case TERARKDB_NAMESPACE::WALRecoveryMode::kTolerateCorruptedTailRecords:
        return 0x0;
      case TERARKDB_NAMESPACE::WALRecoveryMode::kAbsoluteConsistency:
        return 0x1;
      case TERARKDB_NAMESPACE::WALRecoveryMode::kPointInTimeRecovery:
        return 0x2;
      case TERARKDB_NAMESPACE::WALRecoveryMode::kSkipAnyCorruptedRecords:
        return 0x3;
      default:
        // undefined/default
        return 0x2;
    }
  }

  // Returns the equivalent C++ TERARKDB_NAMESPACE::WALRecoveryMode enum for the
  // provided Java org.rocksdb.WALRecoveryMode
  static TERARKDB_NAMESPACE::WALRecoveryMode toCppWALRecoveryMode(
      jbyte jwal_recovery_mode) {
    switch (jwal_recovery_mode) {
      case 0x0:
        return TERARKDB_NAMESPACE::WALRecoveryMode::
            kTolerateCorruptedTailRecords;
      case 0x1:
        return TERARKDB_NAMESPACE::WALRecoveryMode::kAbsoluteConsistency;
      case 0x2:
        return TERARKDB_NAMESPACE::WALRecoveryMode::kPointInTimeRecovery;
      case 0x3:
        return TERARKDB_NAMESPACE::WALRecoveryMode::kSkipAnyCorruptedRecords;
      default:
        // undefined/default
        return TERARKDB_NAMESPACE::WALRecoveryMode::kPointInTimeRecovery;
    }
  }
};

// The portal class for org.rocksdb.TickerType
class TickerTypeJni {
 public:
  // Returns the equivalent org.rocksdb.TickerType for the provided
  // C++ TERARKDB_NAMESPACE::Tickers enum
  static jbyte toJavaTickerType(const TERARKDB_NAMESPACE::Tickers& tickers) {
    switch (tickers) {
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_MISS:
        return 0x0;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_HIT:
        return 0x1;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_ADD:
        return 0x2;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_ADD_FAILURES:
        return 0x3;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_INDEX_MISS:
        return 0x4;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_INDEX_HIT:
        return 0x5;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_INDEX_ADD:
        return 0x6;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_INDEX_BYTES_INSERT:
        return 0x7;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_INDEX_BYTES_EVICT:
        return 0x8;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_FILTER_MISS:
        return 0x9;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_FILTER_HIT:
        return 0xA;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_FILTER_ADD:
        return 0xB;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_FILTER_BYTES_INSERT:
        return 0xC;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_FILTER_BYTES_EVICT:
        return 0xD;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_DATA_MISS:
        return 0xE;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_DATA_HIT:
        return 0xF;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_DATA_ADD:
        return 0x10;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_DATA_BYTES_INSERT:
        return 0x11;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_BYTES_READ:
        return 0x12;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_BYTES_WRITE:
        return 0x13;
      case TERARKDB_NAMESPACE::Tickers::BLOOM_FILTER_USEFUL:
        return 0x14;
      case TERARKDB_NAMESPACE::Tickers::PERSISTENT_CACHE_HIT:
        return 0x15;
      case TERARKDB_NAMESPACE::Tickers::PERSISTENT_CACHE_MISS:
        return 0x16;
      case TERARKDB_NAMESPACE::Tickers::SIM_BLOCK_CACHE_HIT:
        return 0x17;
      case TERARKDB_NAMESPACE::Tickers::SIM_BLOCK_CACHE_MISS:
        return 0x18;
      case TERARKDB_NAMESPACE::Tickers::MEMTABLE_HIT:
        return 0x19;
      case TERARKDB_NAMESPACE::Tickers::MEMTABLE_MISS:
        return 0x1A;
      case TERARKDB_NAMESPACE::Tickers::GET_HIT_L0:
        return 0x1B;
      case TERARKDB_NAMESPACE::Tickers::GET_HIT_L1:
        return 0x1C;
      case TERARKDB_NAMESPACE::Tickers::GET_HIT_L2_AND_UP:
        return 0x1D;
      case TERARKDB_NAMESPACE::Tickers::COMPACTION_KEY_DROP_NEWER_ENTRY:
        return 0x1E;
      case TERARKDB_NAMESPACE::Tickers::COMPACTION_KEY_DROP_OBSOLETE:
        return 0x1F;
      case TERARKDB_NAMESPACE::Tickers::COMPACTION_KEY_DROP_RANGE_DEL:
        return 0x20;
      case TERARKDB_NAMESPACE::Tickers::COMPACTION_KEY_DROP_USER:
        return 0x21;
      case TERARKDB_NAMESPACE::Tickers::COMPACTION_RANGE_DEL_DROP_OBSOLETE:
        return 0x22;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_KEYS_WRITTEN:
        return 0x23;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_KEYS_READ:
        return 0x24;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_KEYS_UPDATED:
        return 0x25;
      case TERARKDB_NAMESPACE::Tickers::BYTES_WRITTEN:
        return 0x26;
      case TERARKDB_NAMESPACE::Tickers::BYTES_READ:
        return 0x27;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_DB_SEEK:
        return 0x28;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_DB_NEXT:
        return 0x29;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_DB_PREV:
        return 0x2A;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_DB_SEEK_FOUND:
        return 0x2B;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_DB_NEXT_FOUND:
        return 0x2C;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_DB_PREV_FOUND:
        return 0x2D;
      case TERARKDB_NAMESPACE::Tickers::ITER_BYTES_READ:
        return 0x2E;
      case TERARKDB_NAMESPACE::Tickers::NO_FILE_CLOSES:
        return 0x2F;
      case TERARKDB_NAMESPACE::Tickers::NO_FILE_OPENS:
        return 0x30;
      case TERARKDB_NAMESPACE::Tickers::NO_FILE_ERRORS:
        return 0x31;
      case TERARKDB_NAMESPACE::Tickers::STALL_L0_SLOWDOWN_MICROS:
        return 0x32;
      case TERARKDB_NAMESPACE::Tickers::STALL_MEMTABLE_COMPACTION_MICROS:
        return 0x33;
      case TERARKDB_NAMESPACE::Tickers::STALL_L0_NUM_FILES_MICROS:
        return 0x34;
      case TERARKDB_NAMESPACE::Tickers::STALL_MICROS:
        return 0x35;
      case TERARKDB_NAMESPACE::Tickers::DB_MUTEX_WAIT_MICROS:
        return 0x36;
      case TERARKDB_NAMESPACE::Tickers::RATE_LIMIT_DELAY_MILLIS:
        return 0x37;
      case TERARKDB_NAMESPACE::Tickers::NO_ITERATORS:
        return 0x38;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_MULTIGET_CALLS:
        return 0x39;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_MULTIGET_KEYS_READ:
        return 0x3A;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_MULTIGET_BYTES_READ:
        return 0x3B;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_FILTERED_DELETES:
        return 0x3C;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_MERGE_FAILURES:
        return 0x3D;
      case TERARKDB_NAMESPACE::Tickers::BLOOM_FILTER_PREFIX_CHECKED:
        return 0x3E;
      case TERARKDB_NAMESPACE::Tickers::BLOOM_FILTER_PREFIX_USEFUL:
        return 0x3F;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_OF_RESEEKS_IN_ITERATION:
        return 0x40;
      case TERARKDB_NAMESPACE::Tickers::GET_UPDATES_SINCE_CALLS:
        return 0x41;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_COMPRESSED_MISS:
        return 0x42;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_COMPRESSED_HIT:
        return 0x43;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_COMPRESSED_ADD:
        return 0x44;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_COMPRESSED_ADD_FAILURES:
        return 0x45;
      case TERARKDB_NAMESPACE::Tickers::WAL_FILE_SYNCED:
        return 0x46;
      case TERARKDB_NAMESPACE::Tickers::WAL_FILE_BYTES:
        return 0x47;
      case TERARKDB_NAMESPACE::Tickers::WRITE_DONE_BY_SELF:
        return 0x48;
      case TERARKDB_NAMESPACE::Tickers::WRITE_DONE_BY_OTHER:
        return 0x49;
      case TERARKDB_NAMESPACE::Tickers::WRITE_TIMEDOUT:
        return 0x4A;
      case TERARKDB_NAMESPACE::Tickers::WRITE_WITH_WAL:
        return 0x4B;
      case TERARKDB_NAMESPACE::Tickers::COMPACT_READ_BYTES:
        return 0x4C;
      case TERARKDB_NAMESPACE::Tickers::COMPACT_WRITE_BYTES:
        return 0x4D;
      case TERARKDB_NAMESPACE::Tickers::FLUSH_WRITE_BYTES:
        return 0x4E;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_DIRECT_LOAD_TABLE_PROPERTIES:
        return 0x4F;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_SUPERVERSION_ACQUIRES:
        return 0x50;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_SUPERVERSION_RELEASES:
        return 0x51;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_SUPERVERSION_CLEANUPS:
        return 0x52;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_BLOCK_COMPRESSED:
        return 0x53;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_BLOCK_DECOMPRESSED:
        return 0x54;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_BLOCK_NOT_COMPRESSED:
        return 0x55;
      case TERARKDB_NAMESPACE::Tickers::MERGE_OPERATION_TOTAL_TIME:
        return 0x56;
      case TERARKDB_NAMESPACE::Tickers::FILTER_OPERATION_TOTAL_TIME:
        return 0x57;
      case TERARKDB_NAMESPACE::Tickers::ROW_CACHE_HIT:
        return 0x58;
      case TERARKDB_NAMESPACE::Tickers::ROW_CACHE_MISS:
        return 0x59;
      case TERARKDB_NAMESPACE::Tickers::READ_AMP_ESTIMATE_USEFUL_BYTES:
        return 0x5A;
      case TERARKDB_NAMESPACE::Tickers::READ_AMP_TOTAL_READ_BYTES:
        return 0x5B;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_RATE_LIMITER_DRAINS:
        return 0x5C;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_ITER_SKIP:
        return 0x5D;
      case TERARKDB_NAMESPACE::Tickers::NUMBER_MULTIGET_KEYS_FOUND:
        return 0x5E;
      case TERARKDB_NAMESPACE::Tickers::NO_ITERATOR_CREATED:
        return 0x5F;
      case TERARKDB_NAMESPACE::Tickers::NO_ITERATOR_DELETED:
        return 0x60;
      case TERARKDB_NAMESPACE::Tickers::GC_GET_KEYS:
        return 0x61;
      case TERARKDB_NAMESPACE::Tickers::GC_TOUCH_FILES:
        return 0x62;
      case TERARKDB_NAMESPACE::Tickers::GC_SKIP_GET_BY_SEQ:
        return 0x63;
      case TERARKDB_NAMESPACE::Tickers::GC_SKIP_GET_BY_FILE:
        return 0x64;
      case TERARKDB_NAMESPACE::Tickers::READ_BLOB_VALID:
        return 0x65;
      case TERARKDB_NAMESPACE::Tickers::READ_BLOB_INVALID:
        return 0x66;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_ERASE:
        return 0x67;
      case TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_ERASE_FAILURES:
        return 0x68;
      case TERARKDB_NAMESPACE::Tickers::TICKER_ENUM_MAX:
        return 0x69;
      default:
        // undefined/default
        return 0x0;
    }
  }

  // Returns the equivalent C++ TERARKDB_NAMESPACE::Tickers enum for the
  // provided Java org.rocksdb.TickerType
  static TERARKDB_NAMESPACE::Tickers toCppTickers(jbyte jticker_type) {
    switch (jticker_type) {
      case 0x0:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_MISS;
      case 0x1:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_HIT;
      case 0x2:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_ADD;
      case 0x3:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_ADD_FAILURES;
      case 0x4:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_INDEX_MISS;
      case 0x5:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_INDEX_HIT;
      case 0x6:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_INDEX_ADD;
      case 0x7:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_INDEX_BYTES_INSERT;
      case 0x8:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_INDEX_BYTES_EVICT;
      case 0x9:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_FILTER_MISS;
      case 0xA:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_FILTER_HIT;
      case 0xB:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_FILTER_ADD;
      case 0xC:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_FILTER_BYTES_INSERT;
      case 0xD:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_FILTER_BYTES_EVICT;
      case 0xE:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_DATA_MISS;
      case 0xF:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_DATA_HIT;
      case 0x10:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_DATA_ADD;
      case 0x11:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_DATA_BYTES_INSERT;
      case 0x12:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_BYTES_READ;
      case 0x13:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_BYTES_WRITE;
      case 0x14:
        return TERARKDB_NAMESPACE::Tickers::BLOOM_FILTER_USEFUL;
      case 0x15:
        return TERARKDB_NAMESPACE::Tickers::PERSISTENT_CACHE_HIT;
      case 0x16:
        return TERARKDB_NAMESPACE::Tickers::PERSISTENT_CACHE_MISS;
      case 0x17:
        return TERARKDB_NAMESPACE::Tickers::SIM_BLOCK_CACHE_HIT;
      case 0x18:
        return TERARKDB_NAMESPACE::Tickers::SIM_BLOCK_CACHE_MISS;
      case 0x19:
        return TERARKDB_NAMESPACE::Tickers::MEMTABLE_HIT;
      case 0x1A:
        return TERARKDB_NAMESPACE::Tickers::MEMTABLE_MISS;
      case 0x1B:
        return TERARKDB_NAMESPACE::Tickers::GET_HIT_L0;
      case 0x1C:
        return TERARKDB_NAMESPACE::Tickers::GET_HIT_L1;
      case 0x1D:
        return TERARKDB_NAMESPACE::Tickers::GET_HIT_L2_AND_UP;
      case 0x1E:
        return TERARKDB_NAMESPACE::Tickers::COMPACTION_KEY_DROP_NEWER_ENTRY;
      case 0x1F:
        return TERARKDB_NAMESPACE::Tickers::COMPACTION_KEY_DROP_OBSOLETE;
      case 0x20:
        return TERARKDB_NAMESPACE::Tickers::COMPACTION_KEY_DROP_RANGE_DEL;
      case 0x21:
        return TERARKDB_NAMESPACE::Tickers::COMPACTION_KEY_DROP_USER;
      case 0x22:
        return TERARKDB_NAMESPACE::Tickers::COMPACTION_RANGE_DEL_DROP_OBSOLETE;
      case 0x23:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_KEYS_WRITTEN;
      case 0x24:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_KEYS_READ;
      case 0x25:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_KEYS_UPDATED;
      case 0x26:
        return TERARKDB_NAMESPACE::Tickers::BYTES_WRITTEN;
      case 0x27:
        return TERARKDB_NAMESPACE::Tickers::BYTES_READ;
      case 0x28:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_DB_SEEK;
      case 0x29:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_DB_NEXT;
      case 0x2A:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_DB_PREV;
      case 0x2B:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_DB_SEEK_FOUND;
      case 0x2C:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_DB_NEXT_FOUND;
      case 0x2D:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_DB_PREV_FOUND;
      case 0x2E:
        return TERARKDB_NAMESPACE::Tickers::ITER_BYTES_READ;
      case 0x2F:
        return TERARKDB_NAMESPACE::Tickers::NO_FILE_CLOSES;
      case 0x30:
        return TERARKDB_NAMESPACE::Tickers::NO_FILE_OPENS;
      case 0x31:
        return TERARKDB_NAMESPACE::Tickers::NO_FILE_ERRORS;
      case 0x32:
        return TERARKDB_NAMESPACE::Tickers::STALL_L0_SLOWDOWN_MICROS;
      case 0x33:
        return TERARKDB_NAMESPACE::Tickers::STALL_MEMTABLE_COMPACTION_MICROS;
      case 0x34:
        return TERARKDB_NAMESPACE::Tickers::STALL_L0_NUM_FILES_MICROS;
      case 0x35:
        return TERARKDB_NAMESPACE::Tickers::STALL_MICROS;
      case 0x36:
        return TERARKDB_NAMESPACE::Tickers::DB_MUTEX_WAIT_MICROS;
      case 0x37:
        return TERARKDB_NAMESPACE::Tickers::RATE_LIMIT_DELAY_MILLIS;
      case 0x38:
        return TERARKDB_NAMESPACE::Tickers::NO_ITERATORS;
      case 0x39:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_MULTIGET_CALLS;
      case 0x3A:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_MULTIGET_KEYS_READ;
      case 0x3B:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_MULTIGET_BYTES_READ;
      case 0x3C:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_FILTERED_DELETES;
      case 0x3D:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_MERGE_FAILURES;
      case 0x3E:
        return TERARKDB_NAMESPACE::Tickers::BLOOM_FILTER_PREFIX_CHECKED;
      case 0x3F:
        return TERARKDB_NAMESPACE::Tickers::BLOOM_FILTER_PREFIX_USEFUL;
      case 0x40:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_OF_RESEEKS_IN_ITERATION;
      case 0x41:
        return TERARKDB_NAMESPACE::Tickers::GET_UPDATES_SINCE_CALLS;
      case 0x42:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_COMPRESSED_MISS;
      case 0x43:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_COMPRESSED_HIT;
      case 0x44:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_COMPRESSED_ADD;
      case 0x45:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_COMPRESSED_ADD_FAILURES;
      case 0x46:
        return TERARKDB_NAMESPACE::Tickers::WAL_FILE_SYNCED;
      case 0x47:
        return TERARKDB_NAMESPACE::Tickers::WAL_FILE_BYTES;
      case 0x48:
        return TERARKDB_NAMESPACE::Tickers::WRITE_DONE_BY_SELF;
      case 0x49:
        return TERARKDB_NAMESPACE::Tickers::WRITE_DONE_BY_OTHER;
      case 0x4A:
        return TERARKDB_NAMESPACE::Tickers::WRITE_TIMEDOUT;
      case 0x4B:
        return TERARKDB_NAMESPACE::Tickers::WRITE_WITH_WAL;
      case 0x4C:
        return TERARKDB_NAMESPACE::Tickers::COMPACT_READ_BYTES;
      case 0x4D:
        return TERARKDB_NAMESPACE::Tickers::COMPACT_WRITE_BYTES;
      case 0x4E:
        return TERARKDB_NAMESPACE::Tickers::FLUSH_WRITE_BYTES;
      case 0x4F:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_DIRECT_LOAD_TABLE_PROPERTIES;
      case 0x50:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_SUPERVERSION_ACQUIRES;
      case 0x51:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_SUPERVERSION_RELEASES;
      case 0x52:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_SUPERVERSION_CLEANUPS;
      case 0x53:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_BLOCK_COMPRESSED;
      case 0x54:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_BLOCK_DECOMPRESSED;
      case 0x55:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_BLOCK_NOT_COMPRESSED;
      case 0x56:
        return TERARKDB_NAMESPACE::Tickers::MERGE_OPERATION_TOTAL_TIME;
      case 0x57:
        return TERARKDB_NAMESPACE::Tickers::FILTER_OPERATION_TOTAL_TIME;
      case 0x58:
        return TERARKDB_NAMESPACE::Tickers::ROW_CACHE_HIT;
      case 0x59:
        return TERARKDB_NAMESPACE::Tickers::ROW_CACHE_MISS;
      case 0x5A:
        return TERARKDB_NAMESPACE::Tickers::READ_AMP_ESTIMATE_USEFUL_BYTES;
      case 0x5B:
        return TERARKDB_NAMESPACE::Tickers::READ_AMP_TOTAL_READ_BYTES;
      case 0x5C:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_RATE_LIMITER_DRAINS;
      case 0x5D:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_ITER_SKIP;
      case 0x5E:
        return TERARKDB_NAMESPACE::Tickers::NUMBER_MULTIGET_KEYS_FOUND;
      case 0x5F:
        return TERARKDB_NAMESPACE::Tickers::NO_ITERATOR_CREATED;
      case 0x60:
        return TERARKDB_NAMESPACE::Tickers::NO_ITERATOR_DELETED;
      case 0x61:
        return TERARKDB_NAMESPACE::Tickers::GC_GET_KEYS;
      case 0x62:
        return TERARKDB_NAMESPACE::Tickers::GC_TOUCH_FILES;
      case 0x63:
        return TERARKDB_NAMESPACE::Tickers::GC_SKIP_GET_BY_SEQ;
      case 0x64:
        return TERARKDB_NAMESPACE::Tickers::GC_SKIP_GET_BY_FILE;
      case 0x65:
        return TERARKDB_NAMESPACE::Tickers::READ_BLOB_VALID;
      case 0x66:
        return TERARKDB_NAMESPACE::Tickers::READ_BLOB_INVALID;
      case 0x67:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_ERASE;
      case 0x68:
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_ERASE_FAILURES;
      case 0x69:
        return TERARKDB_NAMESPACE::Tickers::TICKER_ENUM_MAX;

      default:
        // undefined/default
        return TERARKDB_NAMESPACE::Tickers::BLOCK_CACHE_MISS;
    }
  }
};

// The portal class for org.rocksdb.HistogramType
class HistogramTypeJni {
 public:
  // Returns the equivalent org.rocksdb.HistogramType for the provided
  // C++ TERARKDB_NAMESPACE::Histograms enum
  static jbyte toJavaHistogramsType(
      const TERARKDB_NAMESPACE::Histograms& histograms) {
    switch (histograms) {
      case TERARKDB_NAMESPACE::Histograms::DB_GET:
        return 0x0;
      case TERARKDB_NAMESPACE::Histograms::DB_WRITE:
        return 0x1;
      case TERARKDB_NAMESPACE::Histograms::COMPACTION_TIME:
        return 0x2;
      case TERARKDB_NAMESPACE::Histograms::SUBCOMPACTION_SETUP_TIME:
        return 0x3;
      case TERARKDB_NAMESPACE::Histograms::TABLE_SYNC_MICROS:
        return 0x4;
      case TERARKDB_NAMESPACE::Histograms::COMPACTION_OUTFILE_SYNC_MICROS:
        return 0x5;
      case TERARKDB_NAMESPACE::Histograms::WAL_FILE_SYNC_MICROS:
        return 0x6;
      case TERARKDB_NAMESPACE::Histograms::MANIFEST_FILE_SYNC_MICROS:
        return 0x7;
      case TERARKDB_NAMESPACE::Histograms::TABLE_OPEN_IO_MICROS:
        return 0x8;
      case TERARKDB_NAMESPACE::Histograms::DB_MULTIGET:
        return 0x9;
      case TERARKDB_NAMESPACE::Histograms::READ_BLOCK_COMPACTION_MICROS:
        return 0xA;
      case TERARKDB_NAMESPACE::Histograms::READ_BLOCK_GET_MICROS:
        return 0xB;
      case TERARKDB_NAMESPACE::Histograms::WRITE_RAW_BLOCK_MICROS:
        return 0xC;
      case TERARKDB_NAMESPACE::Histograms::STALL_L0_SLOWDOWN_COUNT:
        return 0xD;
      case TERARKDB_NAMESPACE::Histograms::STALL_MEMTABLE_COMPACTION_COUNT:
        return 0xE;
      case TERARKDB_NAMESPACE::Histograms::STALL_L0_NUM_FILES_COUNT:
        return 0xF;
      case TERARKDB_NAMESPACE::Histograms::HARD_RATE_LIMIT_DELAY_COUNT:
        return 0x10;
      case TERARKDB_NAMESPACE::Histograms::SOFT_RATE_LIMIT_DELAY_COUNT:
        return 0x11;
      case TERARKDB_NAMESPACE::Histograms::NUM_FILES_IN_SINGLE_COMPACTION:
        return 0x12;
      case TERARKDB_NAMESPACE::Histograms::DB_SEEK:
        return 0x13;
      case TERARKDB_NAMESPACE::Histograms::WRITE_STALL:
        return 0x14;
      case TERARKDB_NAMESPACE::Histograms::SST_READ_MICROS:
        return 0x15;
      case TERARKDB_NAMESPACE::Histograms::NUM_SUBCOMPACTIONS_SCHEDULED:
        return 0x16;
      case TERARKDB_NAMESPACE::Histograms::BYTES_PER_READ:
        return 0x17;
      case TERARKDB_NAMESPACE::Histograms::BYTES_PER_WRITE:
        return 0x18;
      case TERARKDB_NAMESPACE::Histograms::BYTES_PER_MULTIGET:
        return 0x19;
      case TERARKDB_NAMESPACE::Histograms::BYTES_COMPRESSED:
        return 0x1A;
      case TERARKDB_NAMESPACE::Histograms::BYTES_DECOMPRESSED:
        return 0x1B;
      case TERARKDB_NAMESPACE::Histograms::COMPRESSION_TIMES_NANOS:
        return 0x1C;
      case TERARKDB_NAMESPACE::Histograms::DECOMPRESSION_TIMES_NANOS:
        return 0x1D;
      case TERARKDB_NAMESPACE::Histograms::READ_NUM_MERGE_OPERANDS:
        return 0x1E;
      case TERARKDB_NAMESPACE::Histograms::FLUSH_TIME:
        return 0x1F;
      case TERARKDB_NAMESPACE::Histograms::PICK_COMPACTION_TIME:
        return 0x20;
      case TERARKDB_NAMESPACE::Histograms::PICK_GARBAGE_COLLECTION_TIME:
        return 0x21;
      case TERARKDB_NAMESPACE::Histograms::INSTALL_SUPER_VERSION_TIME:
        return 0x22;
      case TERARKDB_NAMESPACE::Histograms::BUILD_VERSION_TIME:
        return 0x23;
      case TERARKDB_NAMESPACE::Histograms::HISTOGRAM_ENUM_MAX:
        return 0x24;

      default:
        // undefined/default
        return 0x0;
    }
  }

  // Returns the equivalent C++ TERARKDB_NAMESPACE::Histograms enum for the
  // provided Java org.rocksdb.HistogramsType
  static TERARKDB_NAMESPACE::Histograms toCppHistograms(
      jbyte jhistograms_type) {
    switch (jhistograms_type) {
      case 0x0:
        return TERARKDB_NAMESPACE::Histograms::DB_GET;
      case 0x1:
        return TERARKDB_NAMESPACE::Histograms::DB_WRITE;
      case 0x2:
        return TERARKDB_NAMESPACE::Histograms::COMPACTION_TIME;
      case 0x3:
        return TERARKDB_NAMESPACE::Histograms::SUBCOMPACTION_SETUP_TIME;
      case 0x4:
        return TERARKDB_NAMESPACE::Histograms::TABLE_SYNC_MICROS;
      case 0x5:
        return TERARKDB_NAMESPACE::Histograms::COMPACTION_OUTFILE_SYNC_MICROS;
      case 0x6:
        return TERARKDB_NAMESPACE::Histograms::WAL_FILE_SYNC_MICROS;
      case 0x7:
        return TERARKDB_NAMESPACE::Histograms::MANIFEST_FILE_SYNC_MICROS;
      case 0x8:
        return TERARKDB_NAMESPACE::Histograms::TABLE_OPEN_IO_MICROS;
      case 0x9:
        return TERARKDB_NAMESPACE::Histograms::DB_MULTIGET;
      case 0xA:
        return TERARKDB_NAMESPACE::Histograms::READ_BLOCK_COMPACTION_MICROS;
      case 0xB:
        return TERARKDB_NAMESPACE::Histograms::READ_BLOCK_GET_MICROS;
      case 0xC:
        return TERARKDB_NAMESPACE::Histograms::WRITE_RAW_BLOCK_MICROS;
      case 0xD:
        return TERARKDB_NAMESPACE::Histograms::STALL_L0_SLOWDOWN_COUNT;
      case 0xE:
        return TERARKDB_NAMESPACE::Histograms::STALL_MEMTABLE_COMPACTION_COUNT;
      case 0xF:
        return TERARKDB_NAMESPACE::Histograms::STALL_L0_NUM_FILES_COUNT;
      case 0x10:
        return TERARKDB_NAMESPACE::Histograms::HARD_RATE_LIMIT_DELAY_COUNT;
      case 0x11:
        return TERARKDB_NAMESPACE::Histograms::SOFT_RATE_LIMIT_DELAY_COUNT;
      case 0x12:
        return TERARKDB_NAMESPACE::Histograms::NUM_FILES_IN_SINGLE_COMPACTION;
      case 0x13:
        return TERARKDB_NAMESPACE::Histograms::DB_SEEK;
      case 0x14:
        return TERARKDB_NAMESPACE::Histograms::WRITE_STALL;
      case 0x15:
        return TERARKDB_NAMESPACE::Histograms::SST_READ_MICROS;
      case 0x16:
        return TERARKDB_NAMESPACE::Histograms::NUM_SUBCOMPACTIONS_SCHEDULED;
      case 0x17:
        return TERARKDB_NAMESPACE::Histograms::BYTES_PER_READ;
      case 0x18:
        return TERARKDB_NAMESPACE::Histograms::BYTES_PER_WRITE;
      case 0x19:
        return TERARKDB_NAMESPACE::Histograms::BYTES_PER_MULTIGET;
      case 0x1A:
        return TERARKDB_NAMESPACE::Histograms::BYTES_COMPRESSED;
      case 0x1B:
        return TERARKDB_NAMESPACE::Histograms::BYTES_DECOMPRESSED;
      case 0x1C:
        return TERARKDB_NAMESPACE::Histograms::COMPRESSION_TIMES_NANOS;
      case 0x1D:
        return TERARKDB_NAMESPACE::Histograms::DECOMPRESSION_TIMES_NANOS;
      case 0x1E:
        return TERARKDB_NAMESPACE::Histograms::READ_NUM_MERGE_OPERANDS;
      case 0x1F:
        return TERARKDB_NAMESPACE::Histograms::FLUSH_TIME;
      case 0x20:
        return TERARKDB_NAMESPACE::Histograms::PICK_COMPACTION_TIME;
      case 0x21:
        return TERARKDB_NAMESPACE::Histograms::PICK_GARBAGE_COLLECTION_TIME;
      case 0x22:
        return TERARKDB_NAMESPACE::Histograms::INSTALL_SUPER_VERSION_TIME;
      case 0x23:
        return TERARKDB_NAMESPACE::Histograms::BUILD_VERSION_TIME;
      case 0x24:
        return TERARKDB_NAMESPACE::Histograms::HISTOGRAM_ENUM_MAX;

      default:
        // undefined/default
        return TERARKDB_NAMESPACE::Histograms::DB_GET;
    }
  }
};

// The portal class for org.rocksdb.StatsLevel
class StatsLevelJni {
 public:
  // Returns the equivalent org.rocksdb.StatsLevel for the provided
  // C++ TERARKDB_NAMESPACE::StatsLevel enum
  static jbyte toJavaStatsLevel(
      const TERARKDB_NAMESPACE::StatsLevel& stats_level) {
    switch (stats_level) {
      case TERARKDB_NAMESPACE::StatsLevel::kExceptDetailedTimers:
        return 0x0;
      case TERARKDB_NAMESPACE::StatsLevel::kExceptTimeForMutex:
        return 0x1;
      case TERARKDB_NAMESPACE::StatsLevel::kAll:
        return 0x2;

      default:
        // undefined/default
        return 0x0;
    }
  }

  // Returns the equivalent C++ TERARKDB_NAMESPACE::StatsLevel enum for the
  // provided Java org.rocksdb.StatsLevel
  static TERARKDB_NAMESPACE::StatsLevel toCppStatsLevel(jbyte jstats_level) {
    switch (jstats_level) {
      case 0x0:
        return TERARKDB_NAMESPACE::StatsLevel::kExceptDetailedTimers;
      case 0x1:
        return TERARKDB_NAMESPACE::StatsLevel::kExceptTimeForMutex;
      case 0x2:
        return TERARKDB_NAMESPACE::StatsLevel::kAll;

      default:
        // undefined/default
        return TERARKDB_NAMESPACE::StatsLevel::kExceptDetailedTimers;
    }
  }
};

// The portal class for org.rocksdb.RateLimiterMode
class RateLimiterModeJni {
 public:
  // Returns the equivalent org.rocksdb.RateLimiterMode for the provided
  // C++ TERARKDB_NAMESPACE::RateLimiter::Mode enum
  static jbyte toJavaRateLimiterMode(
      const TERARKDB_NAMESPACE::RateLimiter::Mode& rate_limiter_mode) {
    switch (rate_limiter_mode) {
      case TERARKDB_NAMESPACE::RateLimiter::Mode::kReadsOnly:
        return 0x0;
      case TERARKDB_NAMESPACE::RateLimiter::Mode::kWritesOnly:
        return 0x1;
      case TERARKDB_NAMESPACE::RateLimiter::Mode::kAllIo:
        return 0x2;

      default:
        // undefined/default
        return 0x1;
    }
  }

  // Returns the equivalent C++ TERARKDB_NAMESPACE::RateLimiter::Mode enum for
  // the provided Java org.rocksdb.RateLimiterMode
  static TERARKDB_NAMESPACE::RateLimiter::Mode toCppRateLimiterMode(
      jbyte jrate_limiter_mode) {
    switch (jrate_limiter_mode) {
      case 0x0:
        return TERARKDB_NAMESPACE::RateLimiter::Mode::kReadsOnly;
      case 0x1:
        return TERARKDB_NAMESPACE::RateLimiter::Mode::kWritesOnly;
      case 0x2:
        return TERARKDB_NAMESPACE::RateLimiter::Mode::kAllIo;

      default:
        // undefined/default
        return TERARKDB_NAMESPACE::RateLimiter::Mode::kWritesOnly;
    }
  }
};

// The portal class for org.rocksdb.MemoryUsageType
class MemoryUsageTypeJni {
 public:
  // Returns the equivalent org.rocksdb.MemoryUsageType for the provided
  // C++ TERARKDB_NAMESPACE::MemoryUtil::UsageType enum
  static jbyte toJavaMemoryUsageType(
      const TERARKDB_NAMESPACE::MemoryUtil::UsageType& usage_type) {
    switch (usage_type) {
      case TERARKDB_NAMESPACE::MemoryUtil::UsageType::kMemTableTotal:
        return 0x0;
      case TERARKDB_NAMESPACE::MemoryUtil::UsageType::kMemTableUnFlushed:
        return 0x1;
      case TERARKDB_NAMESPACE::MemoryUtil::UsageType::kTableReadersTotal:
        return 0x2;
      case TERARKDB_NAMESPACE::MemoryUtil::UsageType::kCacheTotal:
        return 0x3;
      default:
        // undefined: use kNumUsageTypes
        return 0x4;
    }
  }

  // Returns the equivalent C++ TERARKDB_NAMESPACE::MemoryUtil::UsageType enum
  // for the provided Java org.rocksdb.MemoryUsageType
  static TERARKDB_NAMESPACE::MemoryUtil::UsageType toCppMemoryUsageType(
      jbyte usage_type) {
    switch (usage_type) {
      case 0x0:
        return TERARKDB_NAMESPACE::MemoryUtil::UsageType::kMemTableTotal;
      case 0x1:
        return TERARKDB_NAMESPACE::MemoryUtil::UsageType::kMemTableUnFlushed;
      case 0x2:
        return TERARKDB_NAMESPACE::MemoryUtil::UsageType::kTableReadersTotal;
      case 0x3:
        return TERARKDB_NAMESPACE::MemoryUtil::UsageType::kCacheTotal;
      default:
        // undefined/default: use kNumUsageTypes
        return TERARKDB_NAMESPACE::MemoryUtil::UsageType::kNumUsageTypes;
    }
  }
};

// The portal class for org.rocksdb.Transaction
class TransactionJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.Transaction
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/Transaction");
  }

  /**
   * Create a new Java org.rocksdb.Transaction.WaitingTransactions object
   *
   * @param env A pointer to the Java environment
   * @param jtransaction A Java org.rocksdb.Transaction object
   * @param column_family_id The id of the column family
   * @param key The key
   * @param transaction_ids The transaction ids
   *
   * @return A reference to a Java
   *     org.rocksdb.Transaction.WaitingTransactions object,
   *     or nullptr if an an exception occurs
   */
  static jobject newWaitingTransactions(
      JNIEnv* env, jobject jtransaction, const uint32_t column_family_id,
      const std::string& key,
      const std::vector<TransactionID>& transaction_ids) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(
        jclazz, "newWaitingTransactions",
        "(JLjava/lang/String;[J)Lorg/rocksdb/Transaction$WaitingTransactions;");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jstring jkey = env->NewStringUTF(key.c_str());
    if (jkey == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    const size_t len = transaction_ids.size();
    jlongArray jtransaction_ids = env->NewLongArray(static_cast<jsize>(len));
    if (jtransaction_ids == nullptr) {
      // exception thrown: OutOfMemoryError
      env->DeleteLocalRef(jkey);
      return nullptr;
    }

    jlong* body = env->GetLongArrayElements(jtransaction_ids, nullptr);
    if (body == nullptr) {
      // exception thrown: OutOfMemoryError
      env->DeleteLocalRef(jkey);
      env->DeleteLocalRef(jtransaction_ids);
      return nullptr;
    }
    for (size_t i = 0; i < len; ++i) {
      body[i] = static_cast<jlong>(transaction_ids[i]);
    }
    env->ReleaseLongArrayElements(jtransaction_ids, body, 0);

    jobject jwaiting_transactions = env->CallObjectMethod(
        jtransaction, mid, static_cast<jlong>(column_family_id), jkey,
        jtransaction_ids);
    if (env->ExceptionCheck()) {
      // exception thrown: InstantiationException or OutOfMemoryError
      env->DeleteLocalRef(jkey);
      env->DeleteLocalRef(jtransaction_ids);
      return nullptr;
    }

    return jwaiting_transactions;
  }
};

// The portal class for org.rocksdb.TransactionDB
class TransactionDBJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.TransactionDB
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/TransactionDB");
  }

  /**
   * Create a new Java org.rocksdb.TransactionDB.DeadlockInfo object
   *
   * @param env A pointer to the Java environment
   * @param jtransaction A Java org.rocksdb.Transaction object
   * @param column_family_id The id of the column family
   * @param key The key
   * @param transaction_ids The transaction ids
   *
   * @return A reference to a Java
   *     org.rocksdb.Transaction.WaitingTransactions object,
   *     or nullptr if an an exception occurs
   */
  static jobject newDeadlockInfo(
      JNIEnv* env, jobject jtransaction_db,
      const TERARKDB_NAMESPACE::TransactionID transaction_id,
      const uint32_t column_family_id, const std::string& waiting_key,
      const bool exclusive) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(
        jclazz, "newDeadlockInfo",
        "(JJLjava/lang/String;Z)Lorg/rocksdb/TransactionDB$DeadlockInfo;");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jstring jwaiting_key = env->NewStringUTF(waiting_key.c_str());
    if (jwaiting_key == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    // resolve the column family id to a ColumnFamilyHandle
    jobject jdeadlock_info = env->CallObjectMethod(
        jtransaction_db, mid, transaction_id,
        static_cast<jlong>(column_family_id), jwaiting_key, exclusive);
    if (env->ExceptionCheck()) {
      // exception thrown: InstantiationException or OutOfMemoryError
      env->DeleteLocalRef(jwaiting_key);
      return nullptr;
    }

    return jdeadlock_info;
  }
};

// The portal class for org.rocksdb.TxnDBWritePolicy
class TxnDBWritePolicyJni {
 public:
  // Returns the equivalent org.rocksdb.TxnDBWritePolicy for the provided
  // C++ TERARKDB_NAMESPACE::TxnDBWritePolicy enum
  static jbyte toJavaTxnDBWritePolicy(
      const TERARKDB_NAMESPACE::TxnDBWritePolicy& txndb_write_policy) {
    switch (txndb_write_policy) {
      case TERARKDB_NAMESPACE::TxnDBWritePolicy::WRITE_COMMITTED:
        return 0x0;
      case TERARKDB_NAMESPACE::TxnDBWritePolicy::WRITE_PREPARED:
        return 0x1;
      case TERARKDB_NAMESPACE::TxnDBWritePolicy::WRITE_UNPREPARED:
        return 0x2;
      default:
        return 0x7F;  // undefined
    }
  }

  // Returns the equivalent C++ TERARKDB_NAMESPACE::TxnDBWritePolicy enum for
  // the provided Java org.rocksdb.TxnDBWritePolicy
  static TERARKDB_NAMESPACE::TxnDBWritePolicy toCppTxnDBWritePolicy(
      jbyte jtxndb_write_policy) {
    switch (jtxndb_write_policy) {
      case 0x0:
        return TERARKDB_NAMESPACE::TxnDBWritePolicy::WRITE_COMMITTED;
      case 0x1:
        return TERARKDB_NAMESPACE::TxnDBWritePolicy::WRITE_PREPARED;
      case 0x2:
        return TERARKDB_NAMESPACE::TxnDBWritePolicy::WRITE_UNPREPARED;
      default:
        // undefined/default
        return TERARKDB_NAMESPACE::TxnDBWritePolicy::WRITE_COMMITTED;
    }
  }
};

// The portal class for org.rocksdb.TransactionDB.KeyLockInfo
class KeyLockInfoJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.TransactionDB.KeyLockInfo
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/TransactionDB$KeyLockInfo");
  }

  /**
   * Create a new Java org.rocksdb.TransactionDB.KeyLockInfo object
   * with the same properties as the provided C++
   * TERARKDB_NAMESPACE::KeyLockInfo object
   *
   * @param env A pointer to the Java environment
   * @param key_lock_info The TERARKDB_NAMESPACE::KeyLockInfo object
   *
   * @return A reference to a Java
   *     org.rocksdb.TransactionDB.KeyLockInfo object,
   *     or nullptr if an an exception occurs
   */
  static jobject construct(
      JNIEnv* env, const TERARKDB_NAMESPACE::KeyLockInfo& key_lock_info) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid =
        env->GetMethodID(jclazz, "<init>", "(Ljava/lang/String;[JZ)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jstring jkey = env->NewStringUTF(key_lock_info.key.c_str());
    if (jkey == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    const jsize jtransaction_ids_len =
        static_cast<jsize>(key_lock_info.ids.size());
    jlongArray jtransactions_ids = env->NewLongArray(jtransaction_ids_len);
    if (jtransactions_ids == nullptr) {
      // exception thrown: OutOfMemoryError
      env->DeleteLocalRef(jkey);
      return nullptr;
    }

    const jobject jkey_lock_info = env->NewObject(
        jclazz, mid, jkey, jtransactions_ids, key_lock_info.exclusive);
    if (jkey_lock_info == nullptr) {
      // exception thrown: InstantiationException or OutOfMemoryError
      env->DeleteLocalRef(jtransactions_ids);
      env->DeleteLocalRef(jkey);
      return nullptr;
    }

    return jkey_lock_info;
  }
};

// The portal class for org.rocksdb.TransactionDB.DeadlockInfo
class DeadlockInfoJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.TransactionDB.DeadlockInfo
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/TransactionDB$DeadlockInfo");
  }
};

// The portal class for org.rocksdb.TransactionDB.DeadlockPath
class DeadlockPathJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.TransactionDB.DeadlockPath
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/TransactionDB$DeadlockPath");
  }

  /**
   * Create a new Java org.rocksdb.TransactionDB.DeadlockPath object
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to a Java
   *     org.rocksdb.TransactionDB.DeadlockPath object,
   *     or nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env, const jobjectArray jdeadlock_infos,
                           const bool limit_exceeded) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>", "([LDeadlockInfo;Z)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    const jobject jdeadlock_path =
        env->NewObject(jclazz, mid, jdeadlock_infos, limit_exceeded);
    if (jdeadlock_path == nullptr) {
      // exception thrown: InstantiationException or OutOfMemoryError
      return nullptr;
    }

    return jdeadlock_path;
  }
};

// various utility functions for working with RocksDB and JNI
class JniUtil {
 public:
  /**
   * Obtains a reference to the JNIEnv from
   * the JVM
   *
   * If the current thread is not attached to the JavaVM
   * then it will be attached so as to retrieve the JNIEnv
   *
   * If a thread is attached, it must later be manually
   * released by calling JavaVM::DetachCurrentThread.
   * This can be handled by always matching calls to this
   * function with calls to {@link JniUtil::releaseJniEnv(JavaVM*, jboolean)}
   *
   * @param jvm (IN) A pointer to the JavaVM instance
   * @param attached (OUT) A pointer to a boolean which
   *     will be set to JNI_TRUE if we had to attach the thread
   *
   * @return A pointer to the JNIEnv or nullptr if a fatal error
   *     occurs and the JNIEnv cannot be retrieved
   */
  static JNIEnv* getJniEnv(JavaVM* jvm, jboolean* attached) {
    assert(jvm != nullptr);

    JNIEnv* env;
    const jint env_rs =
        jvm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_2);

    if (env_rs == JNI_OK) {
      // current thread is already attached, return the JNIEnv
      *attached = JNI_FALSE;
      return env;
    } else if (env_rs == JNI_EDETACHED) {
      // current thread is not attached, attempt to attach
      const jint rs_attach =
          jvm->AttachCurrentThread(reinterpret_cast<void**>(&env), NULL);
      if (rs_attach == JNI_OK) {
        *attached = JNI_TRUE;
        return env;
      } else {
        // error, could not attach the thread
        std::cerr << "JniUtil::getJinEnv - Fatal: could not attach current "
                     "thread to JVM!"
                  << std::endl;
        return nullptr;
      }
    } else if (env_rs == JNI_EVERSION) {
      // error, JDK does not support JNI_VERSION_1_2+
      std::cerr
          << "JniUtil::getJinEnv - Fatal: JDK does not support JNI_VERSION_1_2"
          << std::endl;
      return nullptr;
    } else {
      std::cerr << "JniUtil::getJinEnv - Fatal: Unknown error: env_rs="
                << env_rs << std::endl;
      return nullptr;
    }
  }

  /**
   * Counterpart to {@link JniUtil::getJniEnv(JavaVM*, jboolean*)}
   *
   * Detachess the current thread from the JVM if it was previously
   * attached
   *
   * @param jvm (IN) A pointer to the JavaVM instance
   * @param attached (IN) JNI_TRUE if we previously had to attach the thread
   *     to the JavaVM to get the JNIEnv
   */
  static void releaseJniEnv(JavaVM* jvm, jboolean& attached) {
    assert(jvm != nullptr);
    if (attached == JNI_TRUE) {
      const jint rs_detach = jvm->DetachCurrentThread();
      assert(rs_detach == JNI_OK);
      if (rs_detach != JNI_OK) {
        std::cerr << "JniUtil::getJinEnv - Warn: Unable to detach current "
                     "thread from JVM!"
                  << std::endl;
      }
    }
  }

  /**
   * Copies a Java String[] to a C++ std::vector<std::string>
   *
   * @param env (IN) A pointer to the java environment
   * @param jss (IN) The Java String array to copy
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an OutOfMemoryError or ArrayIndexOutOfBoundsException
   *     exception occurs
   *
   * @return A std::vector<std::string> containing copies of the Java strings
   */
  static std::vector<std::string> copyStrings(JNIEnv* env, jobjectArray jss,
                                              jboolean* has_exception) {
    return TERARKDB_NAMESPACE::JniUtil::copyStrings(
        env, jss, env->GetArrayLength(jss), has_exception);
  }

  /**
   * Copies a Java String[] to a C++ std::vector<std::string>
   *
   * @param env (IN) A pointer to the java environment
   * @param jss (IN) The Java String array to copy
   * @param jss_len (IN) The length of the Java String array to copy
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an OutOfMemoryError or ArrayIndexOutOfBoundsException
   *     exception occurs
   *
   * @return A std::vector<std::string> containing copies of the Java strings
   */
  static std::vector<std::string> copyStrings(JNIEnv* env, jobjectArray jss,
                                              const jsize jss_len,
                                              jboolean* has_exception) {
    std::vector<std::string> strs;
    for (jsize i = 0; i < jss_len; i++) {
      jobject js = env->GetObjectArrayElement(jss, i);
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        *has_exception = JNI_TRUE;
        return strs;
      }

      jstring jstr = static_cast<jstring>(js);
      const char* str = env->GetStringUTFChars(jstr, nullptr);
      if (str == nullptr) {
        // exception thrown: OutOfMemoryError
        env->DeleteLocalRef(js);
        *has_exception = JNI_TRUE;
        return strs;
      }

      strs.push_back(std::string(str));

      env->ReleaseStringUTFChars(jstr, str);
      env->DeleteLocalRef(js);
    }

    *has_exception = JNI_FALSE;
    return strs;
  }

  /**
   * Copies a jstring to a C-style null-terminated byte string
   * and releases the original jstring
   *
   * The jstring is copied as UTF-8
   *
   * If an exception occurs, then JNIEnv::ExceptionCheck()
   * will have been called
   *
   * @param env (IN) A pointer to the java environment
   * @param js (IN) The java string to copy
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an OutOfMemoryError exception occurs
   *
   * @return A pointer to the copied string, or a
   *     nullptr if has_exception == JNI_TRUE
   */
  static std::unique_ptr<char[]> copyString(JNIEnv* env, jstring js,
                                            jboolean* has_exception) {
    const char* utf = env->GetStringUTFChars(js, nullptr);
    if (utf == nullptr) {
      // exception thrown: OutOfMemoryError
      env->ExceptionCheck();
      *has_exception = JNI_TRUE;
      return nullptr;
    } else if (env->ExceptionCheck()) {
      // exception thrown
      env->ReleaseStringUTFChars(js, utf);
      *has_exception = JNI_TRUE;
      return nullptr;
    }

    const jsize utf_len = env->GetStringUTFLength(js);
    std::unique_ptr<char[]> str(
        new char[utf_len +
                 1]);  // Note: + 1 is needed for the c_str null terminator
    std::strcpy(str.get(), utf);
    env->ReleaseStringUTFChars(js, utf);
    *has_exception = JNI_FALSE;
    return str;
  }

  /**
   * Copies a jstring to a std::string
   * and releases the original jstring
   *
   * If an exception occurs, then JNIEnv::ExceptionCheck()
   * will have been called
   *
   * @param env (IN) A pointer to the java environment
   * @param js (IN) The java string to copy
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an OutOfMemoryError exception occurs
   *
   * @return A std::string copy of the jstring, or an
   *     empty std::string if has_exception == JNI_TRUE
   */
  static std::string copyStdString(JNIEnv* env, jstring js,
                                   jboolean* has_exception) {
    const char* utf = env->GetStringUTFChars(js, nullptr);
    if (utf == nullptr) {
      // exception thrown: OutOfMemoryError
      env->ExceptionCheck();
      *has_exception = JNI_TRUE;
      return std::string();
    } else if (env->ExceptionCheck()) {
      // exception thrown
      env->ReleaseStringUTFChars(js, utf);
      *has_exception = JNI_TRUE;
      return std::string();
    }

    std::string name(utf);
    env->ReleaseStringUTFChars(js, utf);
    *has_exception = JNI_FALSE;
    return name;
  }

  /**
   * Copies bytes from a std::string to a jByteArray
   *
   * @param env A pointer to the java environment
   * @param bytes The bytes to copy
   *
   * @return the Java byte[] or nullptr if an exception occurs
   *
   * @throws RocksDBException thrown
   *   if memory size to copy exceeds general java specific array size
   * limitation.
   */
  static jbyteArray copyBytes(JNIEnv* env, std::string bytes) {
    return createJavaByteArrayWithSizeCheck(env, bytes.c_str(), bytes.size());
  }

  /**
   * Given a Java byte[][] which is an array of java.lang.Strings
   * where each String is a byte[], the passed function `string_fn`
   * will be called on each String, the result is the collected by
   * calling the passed function `collector_fn`
   *
   * @param env (IN) A pointer to the java environment
   * @param jbyte_strings (IN) A Java array of Strings expressed as bytes
   * @param string_fn (IN) A transform function to call for each String
   * @param collector_fn (IN) A collector which is called for the result
   *     of each `string_fn`
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an ArrayIndexOutOfBoundsException or OutOfMemoryError
   *     exception occurs
   */
  template <typename T>
  static void byteStrings(JNIEnv* env, jobjectArray jbyte_strings,
                          std::function<T(const char*, const size_t)> string_fn,
                          std::function<void(size_t, T)> collector_fn,
                          jboolean* has_exception) {
    const jsize jlen = env->GetArrayLength(jbyte_strings);

    for (jsize i = 0; i < jlen; i++) {
      jobject jbyte_string_obj = env->GetObjectArrayElement(jbyte_strings, i);
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        *has_exception = JNI_TRUE;  // signal error
        return;
      }

      jbyteArray jbyte_string_ary =
          reinterpret_cast<jbyteArray>(jbyte_string_obj);
      T result = byteString(env, jbyte_string_ary, string_fn, has_exception);

      env->DeleteLocalRef(jbyte_string_obj);

      if (*has_exception == JNI_TRUE) {
        // exception thrown: OutOfMemoryError
        return;
      }

      collector_fn(i, result);
    }

    *has_exception = JNI_FALSE;
  }

  /**
   * Given a Java String which is expressed as a Java Byte Array byte[],
   * the passed function `string_fn` will be called on the String
   * and the result returned
   *
   * @param env (IN) A pointer to the java environment
   * @param jbyte_string_ary (IN) A Java String expressed in bytes
   * @param string_fn (IN) A transform function to call on the String
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an OutOfMemoryError exception occurs
   */
  template <typename T>
  static T byteString(JNIEnv* env, jbyteArray jbyte_string_ary,
                      std::function<T(const char*, const size_t)> string_fn,
                      jboolean* has_exception) {
    const jsize jbyte_string_len = env->GetArrayLength(jbyte_string_ary);
    return byteString<T>(env, jbyte_string_ary, jbyte_string_len, string_fn,
                         has_exception);
  }

  /**
   * Given a Java String which is expressed as a Java Byte Array byte[],
   * the passed function `string_fn` will be called on the String
   * and the result returned
   *
   * @param env (IN) A pointer to the java environment
   * @param jbyte_string_ary (IN) A Java String expressed in bytes
   * @param jbyte_string_len (IN) The length of the Java String
   *     expressed in bytes
   * @param string_fn (IN) A transform function to call on the String
   * @param has_exception (OUT) will be set to JNI_TRUE
   *     if an OutOfMemoryError exception occurs
   */
  template <typename T>
  static T byteString(JNIEnv* env, jbyteArray jbyte_string_ary,
                      const jsize jbyte_string_len,
                      std::function<T(const char*, const size_t)> string_fn,
                      jboolean* has_exception) {
    jbyte* jbyte_string = env->GetByteArrayElements(jbyte_string_ary, nullptr);
    if (jbyte_string == nullptr) {
      // exception thrown: OutOfMemoryError
      *has_exception = JNI_TRUE;
      return nullptr;  // signal error
    }

    T result =
        string_fn(reinterpret_cast<char*>(jbyte_string), jbyte_string_len);

    env->ReleaseByteArrayElements(jbyte_string_ary, jbyte_string, JNI_ABORT);

    *has_exception = JNI_FALSE;
    return result;
  }

  /**
   * Converts a std::vector<string> to a Java byte[][] where each Java String
   * is expressed as a Java Byte Array byte[].
   *
   * @param env A pointer to the java environment
   * @param strings A vector of Strings
   *
   * @return A Java array of Strings expressed as bytes
   */
  static jobjectArray stringsBytes(JNIEnv* env,
                                   std::vector<std::string> strings) {
    jclass jcls_ba = ByteJni::getArrayJClass(env);
    if (jcls_ba == nullptr) {
      // exception occurred
      return nullptr;
    }

    const jsize len = static_cast<jsize>(strings.size());

    jobjectArray jbyte_strings = env->NewObjectArray(len, jcls_ba, nullptr);
    if (jbyte_strings == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    for (jsize i = 0; i < len; i++) {
      std::string* str = &strings[i];
      const jsize str_len = static_cast<jsize>(str->size());

      jbyteArray jbyte_string_ary = env->NewByteArray(str_len);
      if (jbyte_string_ary == nullptr) {
        // exception thrown: OutOfMemoryError
        env->DeleteLocalRef(jbyte_strings);
        return nullptr;
      }

      env->SetByteArrayRegion(
          jbyte_string_ary, 0, str_len,
          const_cast<jbyte*>(reinterpret_cast<const jbyte*>(str->c_str())));
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        env->DeleteLocalRef(jbyte_string_ary);
        env->DeleteLocalRef(jbyte_strings);
        return nullptr;
      }

      env->SetObjectArrayElement(jbyte_strings, i, jbyte_string_ary);
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        // or ArrayStoreException
        env->DeleteLocalRef(jbyte_string_ary);
        env->DeleteLocalRef(jbyte_strings);
        return nullptr;
      }

      env->DeleteLocalRef(jbyte_string_ary);
    }

    return jbyte_strings;
  }

  /**
   * Copies bytes to a new jByteArray with the check of java array size
   * limitation.
   *
   * @param bytes pointer to memory to copy to a new jByteArray
   * @param size number of bytes to copy
   *
   * @return the Java byte[] or nullptr if an exception occurs
   *
   * @throws RocksDBException thrown
   *   if memory size to copy exceeds general java array size limitation to
   * avoid overflow.
   */
  static jbyteArray createJavaByteArrayWithSizeCheck(JNIEnv* env,
                                                     const char* bytes,
                                                     const size_t size) {
    // Limitation for java array size is vm specific
    // In general it cannot exceed Integer.MAX_VALUE (2^31 - 1)
    // Current HotSpot VM limitation for array size is Integer.MAX_VALUE - 5
    // (2^31 - 1 - 5) It means that the next call to env->NewByteArray can still
    // end with OutOfMemoryError("Requested array size exceeds VM limit") coming
    // from VM
    static const size_t MAX_JARRAY_SIZE = (static_cast<size_t>(1)) << 31;
    if (size > MAX_JARRAY_SIZE) {
      TERARKDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
          env, "Requested array size exceeds VM limit");
      return nullptr;
    }

    const jsize jlen = static_cast<jsize>(size);
    jbyteArray jbytes = env->NewByteArray(jlen);
    if (jbytes == nullptr) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    env->SetByteArrayRegion(
        jbytes, 0, jlen,
        const_cast<jbyte*>(reinterpret_cast<const jbyte*>(bytes)));
    if (env->ExceptionCheck()) {
      // exception thrown: ArrayIndexOutOfBoundsException
      env->DeleteLocalRef(jbytes);
      return nullptr;
    }

    return jbytes;
  }

  /**
   * Copies bytes from a TERARKDB_NAMESPACE::Slice to a jByteArray
   *
   * @param env A pointer to the java environment
   * @param bytes The bytes to copy
   *
   * @return the Java byte[] or nullptr if an exception occurs
   *
   * @throws RocksDBException thrown
   *   if memory size to copy exceeds general java specific array size
   * limitation.
   */
  static jbyteArray copyBytes(JNIEnv* env, const Slice& bytes) {
    return createJavaByteArrayWithSizeCheck(env, bytes.data(), bytes.size());
  }

  /*
   * Helper for operations on a key and value
   * for example WriteBatch->Put
   *
   * TODO(AR) could be used for RocksDB->Put etc.
   */
  static std::unique_ptr<TERARKDB_NAMESPACE::Status> kv_op(
      std::function<TERARKDB_NAMESPACE::Status(TERARKDB_NAMESPACE::Slice,
                                               TERARKDB_NAMESPACE::Slice)>
          op,
      JNIEnv* env, jobject /*jobj*/, jbyteArray jkey, jint jkey_len,
      jbyteArray jvalue, jint jvalue_len) {
    jbyte* key = env->GetByteArrayElements(jkey, nullptr);
    if (env->ExceptionCheck()) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    jbyte* value = env->GetByteArrayElements(jvalue, nullptr);
    if (env->ExceptionCheck()) {
      // exception thrown: OutOfMemoryError
      if (key != nullptr) {
        env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
      }
      return nullptr;
    }

    TERARKDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);
    TERARKDB_NAMESPACE::Slice value_slice(reinterpret_cast<char*>(value),
                                          jvalue_len);

    auto status = op(key_slice, value_slice);

    if (value != nullptr) {
      env->ReleaseByteArrayElements(jvalue, value, JNI_ABORT);
    }
    if (key != nullptr) {
      env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
    }

    return std::unique_ptr<TERARKDB_NAMESPACE::Status>(
        new TERARKDB_NAMESPACE::Status(status));
  }

  /*
   * Helper for operations on a key
   * for example WriteBatch->Delete
   *
   * TODO(AR) could be used for RocksDB->Delete etc.
   */
  static std::unique_ptr<TERARKDB_NAMESPACE::Status> k_op(
      std::function<TERARKDB_NAMESPACE::Status(TERARKDB_NAMESPACE::Slice)> op,
      JNIEnv* env, jobject /*jobj*/, jbyteArray jkey, jint jkey_len) {
    jbyte* key = env->GetByteArrayElements(jkey, nullptr);
    if (env->ExceptionCheck()) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    TERARKDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

    auto status = op(key_slice);

    if (key != nullptr) {
      env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
    }

    return std::unique_ptr<TERARKDB_NAMESPACE::Status>(
        new TERARKDB_NAMESPACE::Status(status));
  }

  /*
   * Helper for operations on a value
   * for example WriteBatchWithIndex->GetFromBatch
   */
  static jbyteArray v_op(std::function<TERARKDB_NAMESPACE::Status(
                             TERARKDB_NAMESPACE::Slice, std::string*)>
                             op,
                         JNIEnv* env, jbyteArray jkey, jint jkey_len) {
    jbyte* key = env->GetByteArrayElements(jkey, nullptr);
    if (env->ExceptionCheck()) {
      // exception thrown: OutOfMemoryError
      return nullptr;
    }

    TERARKDB_NAMESPACE::Slice key_slice(reinterpret_cast<char*>(key), jkey_len);

    std::string value;
    TERARKDB_NAMESPACE::Status s = op(key_slice, &value);

    if (key != nullptr) {
      env->ReleaseByteArrayElements(jkey, key, JNI_ABORT);
    }

    if (s.IsNotFound()) {
      return nullptr;
    }

    if (s.ok()) {
      jbyteArray jret_value =
          env->NewByteArray(static_cast<jsize>(value.size()));
      if (jret_value == nullptr) {
        // exception thrown: OutOfMemoryError
        return nullptr;
      }

      env->SetByteArrayRegion(
          jret_value, 0, static_cast<jsize>(value.size()),
          const_cast<jbyte*>(reinterpret_cast<const jbyte*>(value.c_str())));
      if (env->ExceptionCheck()) {
        // exception thrown: ArrayIndexOutOfBoundsException
        if (jret_value != nullptr) {
          env->DeleteLocalRef(jret_value);
        }
        return nullptr;
      }

      return jret_value;
    }

    TERARKDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
    return nullptr;
  }
};

class ColumnFamilyDescriptorJni : public JavaClass {
 public:
  /**
   * Get the Java Class org.rocksdb.ColumnFamilyDescriptor
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "org/rocksdb/ColumnFamilyDescriptor");
  }

  /**
   * Create a new Java org.rocksdb.ColumnFamilyDescriptor object with the same
   * properties as the provided C++ TERARKDB_NAMESPACE::ColumnFamilyDescriptor
   * object
   *
   * @param env A pointer to the Java environment
   * @param cfd A pointer to TERARKDB_NAMESPACE::ColumnFamilyDescriptor object
   *
   * @return A reference to a Java org.rocksdb.ColumnFamilyDescriptor object, or
   * nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env, ColumnFamilyDescriptor* cfd) {
    jbyteArray jcf_name = JniUtil::copyBytes(env, cfd->name);
    jobject cfopts = ColumnFamilyOptionsJni::construct(env, &(cfd->options));

    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>",
                                     "([BLorg/rocksdb/ColumnFamilyOptions;)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      env->DeleteLocalRef(jcf_name);
      return nullptr;
    }

    jobject jcfd = env->NewObject(jclazz, mid, jcf_name, cfopts);
    if (env->ExceptionCheck()) {
      env->DeleteLocalRef(jcf_name);
      return nullptr;
    }

    return jcfd;
  }

  /**
   * Get the Java Method: ColumnFamilyDescriptor#columnFamilyName
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getColumnFamilyNameMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(jclazz, "columnFamilyName", "()[B");
    assert(mid != nullptr);
    return mid;
  }

  /**
   * Get the Java Method: ColumnFamilyDescriptor#columnFamilyOptions
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getColumnFamilyOptionsMethod(JNIEnv* env) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(
        jclazz, "columnFamilyOptions", "()Lorg/rocksdb/ColumnFamilyOptions;");
    assert(mid != nullptr);
    return mid;
  }
};

class MapJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.util.Map
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/util/Map");
  }

  /**
   * Get the Java Method: Map#put
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Method ID or nullptr if the class or method id could not
   *     be retieved
   */
  static jmethodID getMapPutMethodId(JNIEnv* env) {
    jclass jlist_clazz = getClass(env);
    if (jlist_clazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    static jmethodID mid = env->GetMethodID(
        jlist_clazz, "put",
        "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;");
    assert(mid != nullptr);
    return mid;
  }
};

class HashMapJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.util.HashMap
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/util/HashMap");
  }

  /**
   * Create a new Java java.util.HashMap object.
   *
   * @param env A pointer to the Java environment
   *
   * @return A reference to a Java java.util.HashMap object, or
   * nullptr if an an exception occurs
   */
  static jobject construct(JNIEnv* env, const uint32_t initial_capacity = 16) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid = env->GetMethodID(jclazz, "<init>", "(I)V");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    jobject jhash_map =
        env->NewObject(jclazz, mid, static_cast<jint>(initial_capacity));
    if (env->ExceptionCheck()) {
      return nullptr;
    }

    return jhash_map;
  }

  /**
   * A function which maps a std::pair<K,V> to a std::pair<jobject, jobject>
   *
   * @return Either a pointer to a std::pair<jobject, jobject>, or nullptr
   *     if an error occurs during the mapping
   */
  template <typename K, typename V>
  using FnMapKV = std::function<std::unique_ptr<std::pair<jobject, jobject>>(
      const std::pair<K, V>&)>;

  // template <class I, typename K, typename V, typename K1, typename V1,
  // typename std::enable_if<std::is_same<typename
  // std::iterator_traits<I>::value_type, std::pair<const K,V>>::value,
  // int32_t>::type = 0> static void putAll(JNIEnv* env, const jobject
  // jhash_map, I iterator, const FnMapKV<const K,V,K1,V1> &fn_map_kv) {
  /**
   * Returns true if it succeeds, false if an error occurs
   */
  template <class iterator_type, typename K, typename V>
  static bool putAll(JNIEnv* env, const jobject jhash_map,
                     iterator_type iterator, iterator_type end,
                     const FnMapKV<K, V>& fn_map_kv) {
    const jmethodID jmid_put =
        TERARKDB_NAMESPACE::MapJni::getMapPutMethodId(env);
    if (jmid_put == nullptr) {
      return false;
    }

    for (auto it = iterator; it != end; ++it) {
      const std::unique_ptr<std::pair<jobject, jobject>> result =
          fn_map_kv(*it);
      if (result == nullptr) {
        // an error occurred during fn_map_kv
        return false;
      }
      env->CallObjectMethod(jhash_map, jmid_put, result->first, result->second);
      if (env->ExceptionCheck()) {
        // exception occurred
        env->DeleteLocalRef(result->second);
        env->DeleteLocalRef(result->first);
        return false;
      }

      // release local references
      env->DeleteLocalRef(result->second);
      env->DeleteLocalRef(result->first);
    }

    return true;
  }
};

class LongJni : public JavaClass {
 public:
  /**
   * Get the Java Class java.lang.Long
   *
   * @param env A pointer to the Java environment
   *
   * @return The Java Class or nullptr if one of the
   *     ClassFormatError, ClassCircularityError, NoClassDefFoundError,
   *     OutOfMemoryError or ExceptionInInitializerError exceptions is thrown
   */
  static jclass getJClass(JNIEnv* env) {
    return JavaClass::getJClass(env, "java/lang/Long");
  }

  static jobject valueOf(JNIEnv* env, jlong jprimitive_long) {
    jclass jclazz = getJClass(env);
    if (jclazz == nullptr) {
      // exception occurred accessing class
      return nullptr;
    }

    jmethodID mid =
        env->GetStaticMethodID(jclazz, "valueOf", "(J)Ljava/lang/Long;");
    if (mid == nullptr) {
      // exception thrown: NoSuchMethodException or OutOfMemoryError
      return nullptr;
    }

    const jobject jlong_obj =
        env->CallStaticObjectMethod(jclazz, mid, jprimitive_long);
    if (env->ExceptionCheck()) {
      // exception occurred
      return nullptr;
    }

    return jlong_obj;
  }
};
}  // namespace TERARKDB_NAMESPACE
#endif  // JAVA_ROCKSJNI_PORTAL_H_
