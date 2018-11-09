//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <iterator>
#include <memory>
#include <queue>
#include <utility>
#include <vector>

//temp
#include "../terark/src/terark/thread/instance_tls.hpp"
#include "../terark/src/terark/heap_ext.hpp"
//shall be fixed

#include "db/memtable.h"
#include "rocksdb/memtablerep.h"
#include "util/arena.h"
#include "util/mutexlock.h"
#include "util/threaded_rbtree.h"


namespace rocksdb {
namespace {

class ThreadedRBTreeRep : public MemTableRep {
 private:
  typedef size_t index_t;

  template <class index_t>
  struct trbt_rep_node_t {
    index_t lch;
    index_t rch;
    uint8_t color : 1;
    uint8_t lchType : 1;
    uint8_t rchType : 1;
    uint8_t keyLen0 : 5;
    unsigned char datum[1];

    bool is_black() const { return !color; }
    bool is_red() const { return color; }
    bool left_is_child() const { return !lchType; }
    bool left_is_thread() const { return lchType; }
    bool right_is_child() const { return !rchType; }
    bool right_is_thread() const { return rchType; }

    void set_black() { color = 0; }
    void set_red() { color = 1; }
    void left_set_child() { lchType = 0; }
    void left_set_thread() { lchType = 1; }
    void right_set_child() { rchType = 0; }
    void right_set_thread() { rchType = 1; }

    index_t left_get_link() { return lch; }
    index_t right_get_link() { return rch; }
    void left_set_link(index_t &l) { lch = l; }
    void right_set_link(index_t &r) { rch = r; }
  };

  template <class node_t, size_t max_depth = 64>
  struct trbt_rep_stack_t {

    size_t height, path = 0;
    index_t offsets[max_depth];

    bool is_left(size_t k) {
      assert(k < max_depth);
      return (path >> k) ^ 1;
    }

    bool is_right(size_t k) {
      assert(k < max_depth);
      return (path >> k) & 1;
    }

    index_t get_index(size_t k) {
      assert(k < max_depth);
      return offsets[k];
    }

    void push_index(index_t index, bool left) {
      if (height == max_depth) {
        throw std::length_error("thread_tb_tree_stack overflow");
      }
      if (left)
        path &= ~(1 << height);
      else
        path |= (1 << height);
      offsets[height++] = index;
    }

    void update_index(size_t k, index_t index, bool left) {
      assert(k < max_depth);
      offsets[k] = index_type(index);
      if (left)
        path &= ~(1 << k);
      else
        path |= (1 << k);
    }

    void update_index(size_t k, index_t index) {
      assert(k < max_depth);
      offsets[k] = index_type(mask_type(index);
    }
  };

  template <class node_t, class cmp_t, class stack_t>
  class rbtree_t {
   private:
    bool readOnly_ = false;
    size_t memSize_ = 0;
    size_t size_ = 0;
    size_t limit_ = 0;
    cmp_t cmp_;
    port::RWMutex lock_;

    double ANumEntriesRadio(const Slice &start_ikey, const Slice &end_ikey) {
      return 0;
    }  // TODO 

   public:
    
    rbtree_t() {}
    ~rbtree_t() {}
   
    class Iterator {
     private:
     public:
      explicit Iterator();
      ~Iterator();
      void Next() {}
      void Prev() {}
      void Seek(const char *key) {}
      void SeekForPrev(const char *key) {}
      void SeekToFirst() {}
      void SeekToLast() {}
      const char* key() const { return null; }
    };

    bool InsertKeyValue(const Slice &internal_key, const Slice &value) {
      if (readOnly_) return false;
      bool largeKey = internal_key.size() > 38;
      size_t capacity =
          internal_key.size() +
          (largeKey ? VarintLength(internal_key.size()) : 0) +
          value.size() + VarintLength(value.size());
      memSize_ += capacity;
      alloc(); //TODO
      return 0;
    }

    Iterator *NewIterator() { return nullptr; }

    void MarkReadOnly() { readOnly_ = true; }

    size_t Memsize() { return memsize_; }

    uint64_t ANumEntries(const Slice &start_ikey, const Slice &end_ikey) {
      return round(size * ANumEntriesRadio(const Slice &start_ikey,
                                           const Slice &end_ikey));
    }
  };

  class KeyComparator {
   public:
    typedef rocksdb::Slice DecodedType;

    virtual DecodedType decode_key(const char *key) const {
      return GetLengthPrefixedSlice(key);
    }

    virtual int operator()(const char *prefix_len_key1,
                           const char *prefix_len_key2) const = 0;

    virtual int operator()(const char *prefix_len_key,
                           const Slice &key) const = 0;

    virtual const InternalKeyComparator *icomparator() const = 0;

    virtual ~KeyComparator() {}
  };

  using my_node_t = trbt_rep_node_t<index_t>;
  using my_stack_t = trbt_rep_stack_t<my_node_t>;

  using my_rbtree_t = rbtree_t<my_node_t, InternalKeyComparator, my_stack_t>;

  terark::instance_tls<my_rbtree_t *> rbtree_ref_;
  std::list<my_rbtree_t> rbt_list_;
  std::atomic_bool immutable_;
  const SliceTransform *transform_;
  const MemTableRep::KeyComparator& cmp_;
  const InternalKeyComparator *icmp_;
  size_t iter_num_ = 0;

 public:
  explicit ThreadedRBTreeRep(size_t reserve_size,
                             const MemTableRep::KeyComparator &compare,
                             Allocator *allocator,
                             const SliceTransform *transform)
      : MemTableRep(allocator), 
        transform_(transform),
        immutable_(false),
        cmp_(compare) {
    icmp_ = cmp_.icomparator();
  }

  virtual ~ThreadedRBTreeRep() override { assert(iter_num_); }

  virtual KeyHandle Allocate(const size_t len, char **buf) override {
    assert(false);
    return nullptr;
  }

  virtual void Insert(KeyHandle handle) override { assert(false); }

  char *EncodeKey(const Slice &user_key) { return nullptr; }

  virtual void Get(const LookupKey &k, void *callback_args,
                   bool (*callback_func)(void *arg,
                                         const KeyValuePair *)) override {
  }

  class HeapIterator : public MemTableRep::Iterator {
   private:
    std::vector<my_rbtree_t::Iterator> heap_;
    ThreadedRBTreeRep* rep_;
    const InternalKeyComparator *icmp_;
    bool forward = true;
    std::atomic<size_t> iter_cnt_ = 0;

   public:
    explicit HeapIterator(ThreadedRBTreeRep* rep) : rep_(rep) {
      icmp_ = rep_->cmp_.icomparator();
      for (auto& rbt : rep_->rbt_list_) heap_.emplace_back(rbt.NewIterator());
      ++iter_cnt_;
    }

    virtual ~HeapIterator() override { --iter_cnt_; }

    struct ForwardCmp {
      const InternalKeyComparator *icmp_;
      bool operator()(my_rbtree_t::Iterator &l,
                      my_rbtree_t::Iterator &r) const {
        return icmp_->Compare(l.key(), r.key()) < 0;
      }
    };

    struct BackwardCmp {
      const InternalKeyComparator *icmp_;
      bool operator()(my_rbtree_t::Iterator &l,
                      my_rbtree_t::Iterator &r) const {
        return icmp_->Compare(l.key(), r.key()) > 0;
      }
    };

    virtual bool Valid() const override { return !heap_.empty(); }

    virtual void Next() override {
      if (!forward)
        std::make_heap(heap_.begin(), heap_.end(), ForwardCmp{icmp_});
      heap_.begin()->Next();
      terark::adjust_heap_top(heap_, heap_.size(), ForwardCmp{icmp_});
    }

    virtual void Prev() override {
      if (forward)
        std::make_heap(heap_.begin(), heap_.end(), BackwardCmp{icmp_});
      heap_.begin()->Prev();
      terark::adjust_heap_top(heap_, heap_.size(), BackwardCmp{icmp_});
    }

    virtual void Seek(const Slice &user_key,
                      const char *memtable_key) override {
      for (auto iter : heap_)
        if (memtable_key != nullptr)
          iter.Seek(memtable_key);
        else
          iter.Seek(rep_->EncodeKey(user_key));
      std::make_heap(heap_.begin(), heap_.end(), ForwardCmp{icmp_});
    }

    virtual void SeekForPrev(const Slice &user_key,
                             const char *memtable_key) override {
      for (auto iter : heap_)
        if (memtable_key != nullptr)
          iter.SeekForPrev(memtable_key);
        else
          iter.SeekForPrev(rep_->EncodeKey(user_key));
      std::make_heap(heap_.begin(), heap_.end(), BackwardCmp{icmp_});
    }

    virtual const char* key() const override { return heap_.front().key(); }

    virtual void SeekToFirst() override {
      for (auto iter : heap_) iter.SeekToFirst();
      std::make_heap(heap_.begin(), heap_.end(), ForwardCmp{icmp_});
      forward = true;
    }

    virtual void SeekToLast() override {
      for (auto iter : heap_) iter.SeekToLast();
      std::make_heap(heap_.begin(), heap_.end(), BackwardCmp{icmp_});
      forward = false;
    }

  };

  virtual MemTableRep::Iterator *GetIterator(Arena *arena = nullptr) override {
    typedef ThreadedRBTreeRep::HeapIterator i_t;
    return arena ? new (arena->AllocateAligned(sizeof(i_t))) i_t(this) : new i_t(this);
  }
  
  virtual bool InsertKeyValue(const Slice &internal_key,
                              const Slice &value) override{
    if (immutable_) return false;
    return rbtree_ref_.get()->InsertKeyValue(internal_key, value);
  }
  
  virtual void MarkReadOnly() override {
    for (auto& rbt : rbt_list_) rbt.MarkReadOnly();
    immutable_ = true;
  }

  virtual size_t ApproximateMemoryUsage() override {
    size_t sz = 0;
    for (auto& rbt : rbt_list_) sz += rbt.Memsize();
    return sz;
  }

  virtual uint64_t ApproximateNumEntries(const Slice &start_ikey,
                                         const Slice &end_ikey) override {
    size_t n = 0;
    for (auto& rbt : rbt_list_) n += rbt.ANumEntries(start_ikey, end_ikey);
    return n;
  }

  };

}  // namespace
}  // namespace rocksdb
