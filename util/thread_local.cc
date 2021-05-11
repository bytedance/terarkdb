//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/thread_local.h"

#include <stdlib.h>

#include <atomic>
#include <memory>
#include <unordered_map>
#include <vector>

#include "port/likely.h"
#include "port/port.h"
#include "rocksdb/terark_namespace.h"
#include "util/mutexlock.h"

#define MY_USE_FIBER_LOCAL_STORAGE 0  // drop boost lib

#if MY_USE_FIBER_LOCAL_STORAGE
#include <boost/fiber/fss.hpp>
#endif

namespace TERARKDB_NAMESPACE {

struct Entry {
  Entry() : ptr(nullptr) {}
  Entry(const Entry& e) : ptr(e.ptr.load(std::memory_order_relaxed)) {}
  std::atomic<void*> ptr;
};

class StaticMeta;

// This is the structure that is declared as "thread_local" storage.
// The vector keep list of atomic pointer for all instances for "current"
// thread. The vector is indexed by an Id that is unique in process and
// associated with one ThreadLocalPtr instance. The Id is assigned by a
// global StaticMeta singleton. So if we instantiated 3 ThreadLocalPtr
// instances, each thread will have a ThreadData with a vector of size 3:
//     ---------------------------------------------------
//     |          | instance 1 | instance 2 | instnace 3 |
//     ---------------------------------------------------
//     | thread 1 |    void*   |    void*   |    void*   | <- ThreadData
//     ---------------------------------------------------
//     | thread 2 |    void*   |    void*   |    void*   | <- ThreadData
//     ---------------------------------------------------
//     | thread 3 |    void*   |    void*   |    void*   | <- ThreadData
//     ---------------------------------------------------
struct ThreadData {
  explicit ThreadData(StaticMeta* _inst)
      : entries(), next(nullptr), prev(nullptr), inst(_inst) {}
  std::vector<Entry> entries;
  ThreadData* next;
  ThreadData* prev;
  StaticMeta* inst;
};

class StaticMeta {
 public:
  using FoldFunc = ThreadLocalPtr::FoldFunc;

  StaticMeta();

  // Return the next available Id
  uint32_t GetId();
  // Return the next available Id without claiming it
  uint32_t PeekId() const;
  // Return the given Id back to the free pool. This also triggers
  // UnrefHandler for associated pointer value (if not NULL) for all threads.
  void ReclaimId(uint32_t id);

  // Return the pointer value for the given id for the current thread.
  void* Get(uint32_t id) const;
  // Reset the pointer value for the given id for the current thread.
  void Reset(uint32_t id, void* ptr);
  // Atomically swap the supplied ptr and return the previous value
  void* Swap(uint32_t id, void* ptr);
  // Atomically compare and swap the provided value only if it equals
  // to expected value.
  bool CompareAndSwap(uint32_t id, void* ptr, void*& expected);
  // Reset all thread local data to replacement, and return non-nullptr
  // data for all existing threads
  void Scrape(uint32_t id, autovector<void*>* ptrs, void* const replacement);
  // Update res by applying func on each thread-local value. Holds a lock that
  // prevents unref handler from running during this call, but clients must
  // still provide external synchronization since the owning thread can
  // access the values without internal locking, e.g., via Get() and Reset().
  void Fold(uint32_t id, FoldFunc func, void* res);

  // Register the UnrefHandler for id
  void SetHandler(uint32_t id, UnrefHandler handler);

  // protect inst, next_instance_id_, free_instance_ids_, head_,
  // ThreadData.entries
  //
  // Note that here we prefer function static variable instead of the usual
  // global static variable.  The reason is that c++ destruction order of
  // static variables in the reverse order of their construction order.
  // However, C++ does not guarantee any construction order when global
  // static variables are defined in different files, while the function
  // static variables are initialized when their function are first called.
  // As a result, the construction order of the function static variables
  // can be controlled by properly invoke their first function calls in
  // the right order.
  //
  // For instance, the following function contains a function static
  // variable.  We place a dummy function call of this inside
  // Env::Default() to ensure the construction order of the construction
  // order.
  static port::RWMutex* Mutex();

  // Returns the member mutex of the current StaticMeta.  In general,
  // Mutex() should be used instead of this one.  However, in case where
  // the static variable inside Instance() goes out of scope, MemberMutex()
  // should be used.  One example is OnThreadExit() function.
  port::RWMutex* MemberMutex() { return &mutex_; }

 private:
  // Get UnrefHandler for id with acquiring mutex
  // REQUIRES: mutex locked
  UnrefHandler GetHandler(uint32_t id);

  typedef void (*OnThreadExit_type)(ThreadData*);

  // Triggered before a thread terminates
  static void OnThreadExit(void* ptr);

  // Add current thread's ThreadData to the global chain
  // REQUIRES: mutex locked
  void AddThreadData(ThreadData* d);

  // Remove current thread's ThreadData from the global chain
  // REQUIRES: mutex locked
  void RemoveThreadData(ThreadData* d);

  static ThreadData* GetThreadLocal();

  uint32_t next_instance_id_;
  // Used to recycle Ids in case ThreadLocalPtr is instantiated and destroyed
  // frequently. This also prevents it from blowing up the vector space.
  autovector<uint32_t> free_instance_ids_;
  // Chain all thread local structure together. This is necessary since
  // when one ThreadLocalPtr gets destroyed, we need to loop over each
  // thread's version of pointer corresponding to that instance and
  // call UnrefHandler for it.
  ThreadData head_;

  std::unordered_map<uint32_t, UnrefHandler> handler_map_;

  // The private mutex.  Developers should always use Mutex() instead of
  // using this variable directly.
  port::RWMutex mutex_;
#if MY_USE_FIBER_LOCAL_STORAGE
  static boost::fibers::fiber_specific_ptr<ThreadData> tls_;
#else
  static thread_local std::unique_ptr<ThreadData, OnThreadExit_type> tls_;
#endif
};

#if MY_USE_FIBER_LOCAL_STORAGE
boost::fibers::fiber_specific_ptr<ThreadData> StaticMeta::tls_(
    (boost::fibers::context::active(),
     (OnThreadExit_type)&StaticMeta::OnThreadExit));
#else
thread_local std::unique_ptr<ThreadData, StaticMeta::OnThreadExit_type>
    StaticMeta::tls_(NULL, (OnThreadExit_type)&StaticMeta::OnThreadExit);
#endif

static StaticMeta* Instance() {
  // Here we prefer function static variable instead of global
  // static variable as function static variable is initialized
  // when the function is first call.  As a result, we can properly
  // control their construction order by properly preparing their
  // first function call.
  //
  // Note that here we decide to make "inst" a static pointer w/o deleting
  // it at the end instead of a static variable.  This is to avoid the following
  // destruction order disaster happens when a child thread using ThreadLocalPtr
  // dies AFTER the main thread dies:  When a child thread happens to use
  // ThreadLocalPtr, it will try to delete its thread-local data on its
  // OnThreadExit when the child thread dies.  However, OnThreadExit depends
  // on the following variable.  As a result, if the main thread dies before any
  // child thread happen to use ThreadLocalPtr dies, then the destruction of
  // the following variable will go first, then OnThreadExit, therefore causing
  // invalid access.
  //
  // The above problem can be solved by using thread_local to store tls_ instead
  // of using __thread.  The major difference between thread_local and __thread
  // is that thread_local supports dynamic construction and destruction of
  // non-primitive typed variables.  As a result, we can guarantee the
  // destruction order even when the main thread dies before any child threads.
  // However, thread_local is not supported in all compilers that accept
  // -std=c++11 (e.g., eg Mac with XCode < 8. XCode 8+ supports thread_local).
  static StaticMeta* inst = new StaticMeta();
  return inst;
}

void ThreadLocalPtr::InitSingletons() { Instance(); }

port::RWMutex* StaticMeta::Mutex() { return &Instance()->mutex_; }

void StaticMeta::OnThreadExit(void* ptr) {
  // printf("OnThreadExit(%p)\n", ptr);

  auto* tls = static_cast<ThreadData*>(ptr);
  assert(tls != nullptr);

  // Use the cached StaticMeta::Instance() instead of directly calling
  // the variable inside StaticMeta::Instance() might already go out of
  // scope here in case this OnThreadExit is called after the main thread
  // dies.
  auto* inst = tls->inst;

  WriteLock l(inst->MemberMutex());
  inst->RemoveThreadData(tls);
  // Unref stored pointers of current thread from all instances
  uint32_t id = 0;
  for (auto& e : tls->entries) {
    void* raw = e.ptr.load();
    if (raw != nullptr) {
      auto unref = inst->GetHandler(id);
      if (unref != nullptr) {
        unref(raw);
      }
    }
    ++id;
  }
  // Delete thread local structure no matter if it is Mac platform
  delete tls;
}

StaticMeta::StaticMeta() : next_instance_id_(0), head_(this) {
  head_.next = &head_;
  head_.prev = &head_;
}

void StaticMeta::AddThreadData(ThreadData* d) {
  Mutex()->AssertHeld();
  d->next = &head_;
  d->prev = head_.prev;
  head_.prev->next = d;
  head_.prev = d;
}

void StaticMeta::RemoveThreadData(ThreadData* d) {
  Mutex()->AssertHeld();
  d->next->prev = d->prev;
  d->prev->next = d->next;
  d->next = d->prev = d;
}

ThreadData* StaticMeta::GetThreadLocal() {
  if (UNLIKELY(tls_.get() == nullptr)) {
    auto* inst = Instance();
    auto* td = new ThreadData(inst);
    WriteLock l(Mutex());
    inst->AddThreadData(td);
    tls_.reset(td);
    // printf("OnThreadMake(%p)\n", td);
  }
  return tls_.get();
}

void* StaticMeta::Get(uint32_t id) const {
  auto* tls = GetThreadLocal();
  if (UNLIKELY(id >= tls->entries.size())) {
    return nullptr;
  }
  return tls->entries[id].ptr.load(std::memory_order_acquire);
}

void StaticMeta::Reset(uint32_t id, void* ptr) {
  auto* tls = GetThreadLocal();
  if (UNLIKELY(id >= tls->entries.size())) {
    // Need mutex to protect entries access within ReclaimId
    WriteLock l(Mutex());
    tls->entries.resize(id + 1);
  }
  tls->entries[id].ptr.store(ptr, std::memory_order_release);
}

void* StaticMeta::Swap(uint32_t id, void* ptr) {
  auto* tls = GetThreadLocal();
  if (UNLIKELY(id >= tls->entries.size())) {
    // Need mutex to protect entries access within ReclaimId
    WriteLock l(Mutex());
    tls->entries.resize(id + 1);
  }
  return tls->entries[id].ptr.exchange(ptr, std::memory_order_acquire);
}

bool StaticMeta::CompareAndSwap(uint32_t id, void* ptr, void*& expected) {
  auto* tls = GetThreadLocal();
  if (UNLIKELY(id >= tls->entries.size())) {
    // Need mutex to protect entries access within ReclaimId
    WriteLock l(Mutex());
    tls->entries.resize(id + 1);
  }
  return tls->entries[id].ptr.compare_exchange_strong(
      expected, ptr, std::memory_order_release, std::memory_order_relaxed);
}

void StaticMeta::Scrape(uint32_t id, autovector<void*>* ptrs,
                        void* const replacement) {
  ReadLock l(Mutex());
  for (ThreadData* t = head_.next; t != &head_; t = t->next) {
    if (id < t->entries.size()) {
      void* ptr =
          t->entries[id].ptr.exchange(replacement, std::memory_order_acquire);
      if (ptr != nullptr) {
        ptrs->push_back(ptr);
      }
    }
  }
}

void StaticMeta::Fold(uint32_t id, FoldFunc func, void* res) {
  ReadLock l(Mutex());
  for (ThreadData* t = head_.next; t != &head_; t = t->next) {
    if (id < t->entries.size()) {
      void* ptr = t->entries[id].ptr.load();
      if (ptr != nullptr) {
        func(ptr, res);
      }
    }
  }
}

uint32_t ThreadLocalPtr::TEST_PeekId() { return Instance()->PeekId(); }

void StaticMeta::SetHandler(uint32_t id, UnrefHandler handler) {
  WriteLock l(Mutex());
  handler_map_[id] = handler;
}

UnrefHandler StaticMeta::GetHandler(uint32_t id) {
  Mutex()->AssertHeld();
  auto iter = handler_map_.find(id);
  if (iter == handler_map_.end()) {
    return nullptr;
  }
  return iter->second;
}

uint32_t StaticMeta::GetId() {
  WriteLock l(Mutex());
  if (free_instance_ids_.empty()) {
    return next_instance_id_++;
  }

  uint32_t id = free_instance_ids_.back();
  free_instance_ids_.pop_back();
  return id;
}

uint32_t StaticMeta::PeekId() const {
  WriteLock l(Mutex());
  if (!free_instance_ids_.empty()) {
    return free_instance_ids_.back();
  }
  return next_instance_id_;
}

void StaticMeta::ReclaimId(uint32_t id) {
  // This id is not used, go through all thread local data and release
  // corresponding value
  WriteLock l(Mutex());
  auto unref = GetHandler(id);
  for (ThreadData* t = head_.next; t != &head_; t = t->next) {
    if (id < t->entries.size()) {
      void* ptr = t->entries[id].ptr.exchange(nullptr);
      if (ptr != nullptr && unref != nullptr) {
        unref(ptr);
      }
    }
  }
  handler_map_[id] = nullptr;
  free_instance_ids_.push_back(id);
}

ThreadLocalPtr::ThreadLocalPtr(UnrefHandler handler)
    : id_(Instance()->GetId()) {
  // printf("ThreadLocalPtr(id=%d, handler=%p)\n", id_, handler);
  if (handler != nullptr) {
    Instance()->SetHandler(id_, handler);
  }
}

ThreadLocalPtr::~ThreadLocalPtr() {
  // printf("~ThreadLocalPtr(id=%d)\n", id_);
  Instance()->ReclaimId(id_);
}

void* ThreadLocalPtr::Get() const { return Instance()->Get(id_); }

void ThreadLocalPtr::Reset(void* ptr) { Instance()->Reset(id_, ptr); }

void* ThreadLocalPtr::Swap(void* ptr) { return Instance()->Swap(id_, ptr); }

bool ThreadLocalPtr::CompareAndSwap(void* ptr, void*& expected) {
  return Instance()->CompareAndSwap(id_, ptr, expected);
}

void ThreadLocalPtr::Scrape(autovector<void*>* ptrs, void* const replacement) {
  Instance()->Scrape(id_, ptrs, replacement);
}

void ThreadLocalPtr::Fold(FoldFunc func, void* res) {
  Instance()->Fold(id_, func, res);
}

}  // namespace TERARKDB_NAMESPACE
