//  CopyriaAght (c) 2011-present, Facebook, Inc.  All rights reserved.
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

// c
#include <terark/heap_ext.hpp>
#include <terark/mempool.hpp>
#include <terark/stdtypes.hpp>
#include <terark/thread/instance_tls.hpp>
// C

#include "db/memtable.h"
#include "rocksdb/memtablerep.h"
#include "util/arena.h"
#include "util/mutexlock.h"
#include "util/threaded_rbtree.h"

namespace rocksdb {
namespace {

class ThreadedRBTreeRep : public MemTableRep {
 private:
  template <size_t padding = 2>
  class rbtree_impl {
   protected:
    using my_mempool_t = terark::MemPool<1ull << padding>;

    template <class index_t, class key_t>
    struct trbt_node_t {
      typedef typename index_t index_type;
      typedef typename key_t key_type;

      static const index_type nil_sentinel = index_type(-1);

      index_type lch = nil_sentinel;
      index_type rch = nil_sentinel;
      uint8_t clr : 1;
      uint8_t lcT : 1;
      uint8_t rcT : 1;
      uint8_t kln : 5;
      char datum[1];

      bool is_black() const { return !clr; }
      bool is_red() const { return clr; }
      bool left_is_child() const { return !lcT; }
      bool left_is_thread() const { return lcT; }
      bool right_is_child() const { return !rcT; }
      bool right_is_thread() const { return rcT; }

      void set_used() {}
      void set_black() { clr = 0; }
      void set_red() { clr = 1; }
      void left_set_child() { lcT = 0; }
      void left_set_thread() { lcT = 1; }
      void right_set_child() { rcT = 0; }
      void right_set_thread() { rcT = 1; }

      index_type left_get_link() { return lch; }
      index_type right_get_link() { return rch; }
      void left_set_link(index_type l) { lch = l; }
      void right_set_link(index_type r) { rch = r; }

      key_type key() const {
        if (kln < 31)
          return key_type{datum, size_t(kln) + 8};
        else {
          uint32_t ret = 0;
          auto p = GetVarint32Ptr(datum, datum + 8, &ret);
          return key_type{p, ret + 39};
        }
      }

      key_type value() const {
        auto k = key();
        auto p = k.data() + k.size();
        uint32_t vlen;
        p = GetVarint32Ptr(p, p + 8, &vlen);
        return key_type(p, vlen);
      }

      void set_key_value(const key_type &key, const key_type &val,
                         uint32_t klnln, uint32_t vlnln, bool mag) {
        char *p = datum;
        assert((key.size() < 39) ^ mag);
        kln = mag ? 31 : key.size() - 8;
        if (mag) {
          EncodeVarint32(p, static_cast<uint32_t>(key.size() - 39));
          p += klnln;
        }
        memcpy(p, key.data(), key.size());
        p += key.size();
        EncodeVarint32(p, static_cast<uint32_t>(val.size()));
        p += vlnln;
        memcpy(p, val.data(), val.size());
      }

      size_t capicity() {
        uint32_t rvln, ret = 0;
        if (kln < 31) {
          GetVarint32Ptr(datum + kln, datum + kln + 8, &rvln);
          ret = 9 + kln + rvln;
        } else {
          uint32_t rkln;
          auto p = GetVarint32Ptr(datum, datum + 8, &rkln);
          GetVarint32Ptr(p + rkln, p + rkln + 8, &rvln);
          ret = 9 + rkln + rvln;
        }
        return ret;
      }
    };

    template <class node_t>
    struct trbt_deref_node_t {
      my_mempool_t *handle;
      node_t &operator()(typename node_t::index_type idx) const {
        return handle->at<node_t>(idx << padding);
      }
    };

    template <class node_t>
    struct trbt_deref_key_t {
      my_mempool_t *handle;
      typename const node_t::key_type operator()(
          typename node_t::index_type idx) const {
        return handle->at<node_t>(idx << padding).key();
      }
    };

    template <class node_t, size_t max_depth>
    struct trbt_rep_stack_t {
      typename typedef node_t::index_type index_t;

      size_t height = 0, path = 0;
      index_t offsets[max_depth];

      bool is_left(size_t k) {
        assert(k < max_depth);
        return !((path >> k) & 1);
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
          throw std::length_error("thread_rbtree_stack overflow");
        }
        if (left)
          path &= ~(1ull << height);
        else
          path |= (1ull << height);
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
        offsets[k] = index_type(mask_type(index));
      }
    };

    template <class deref_t>
    struct trbt_rep_cmp_t {
      bool operator()(size_t x, size_t y) const { return compare(x, y) < 0; }
      bool operator()(const Slice &x, const Slice &y) const {
        return compare(x, y) < 0;
      }
      int compare(size_t x, size_t y) const {
        return c->Compare(deref(x), deref(y));
      }
      int compare(const Slice &x, const Slice &y) const {
        return c->Compare(x, y);
      }
      deref_t deref;
      const InternalKeyComparator *c;
    };

    using my_index_t = uint32_t;
    using my_key_t = rocksdb::Slice;
    using my_node_t = trbt_node_t<my_index_t, my_key_t>;
    using my_deref_node_t = trbt_deref_node_t<my_node_t>;
    using my_deref_key_t = trbt_deref_key_t<my_node_t>;
    using my_cmp_t = trbt_rep_cmp_t<my_deref_key_t>;
    using my_stack_t = trbt_rep_stack_t<my_node_t, 64>;
    using my_root_t =
        threaded_rbtree_root_t<my_node_t, std::true_type, std::true_type>;

   private:
    my_mempool_t data_;
    my_root_t root_;
    size_t item_num_ = 0, mem_used_ = 0, iter_cnt_ = 0;
    port::RWMutex mutex_;
    bool destory_ = false;
    bool readOnly_ = false;
    ThreadedRBTreeRep *rep_;
    static const size_t lowCapicity = 64 * 1024 * 1024;
 
   public:
    rbtree_impl(ThreadedRBTreeRep *rep) : data_(256), rep_(rep) {}
    ~rbtree_impl() { assert(iter_cnt_ == 0); }

    void Destory() {
      if (mem_used_ < lowCapicity) {
        auto &list = rep_->rbt_list_;
        auto iter = list.begin();
        while ((iter != list.end()) && (((iter->MemUsed() + mem_used_) / 4 - UINT32_MAX >= 0) || (&*iter == this))) iter++;
        if (iter != list.end()) {
          iter->Merge(this);
          iter = list.begin();
          while (&*iter != this) iter++;
          list.erase(iter);
        }
      }
    }

    ThreadedRBTreeRep *Rep() { return rep_; }

    class InnerIterator {
     private:
      rbtree_impl *impl_;
      size_t pos_ = 0;

     public:
      explicit InnerIterator(rbtree_impl *impl) : impl_(impl) {
        impl_->iter_cnt_++;
      }

      ~InnerIterator() {
        --impl_->iter_cnt_;
        if ( impl_->NeedToDestory() && impl_->iter_cnt_ == 0) impl_->Destory();
      }

      void Next() {
        pos_ = threaded_rbtree_move_next<my_deref_node_t>(
            pos_, my_deref_node_t{&impl_->data_});
      }

      void Prev() {
        pos_ = threaded_rbtree_move_prev<my_deref_node_t>(
            pos_, my_deref_node_t{&impl_->data_});
      }

      void Seek(const Slice &key, const InternalKeyComparator *icmp) {
        auto deref_key = my_deref_key_t{&impl_->data_};
        auto cmp = my_cmp_t{deref_key, icmp};
        pos_ = threaded_rbtree_lower_bound(
            impl_->root_, my_deref_node_t{&impl_->data_}, key, deref_key, cmp);
      }

      void SeekForPrev(const Slice &key, const InternalKeyComparator *icmp) {
        auto deref_key = my_deref_key_t{&impl_->data_};
        auto cmp = my_cmp_t{deref_key, icmp};
        pos_ = threaded_rbtree_reverse_lower_bound(
            impl_->root_, my_deref_node_t{&impl_->data_}, key, deref_key, cmp);
      }

      void SeekToFirst() {
        pos_ = impl_->root_.get_most_left(my_deref_node_t{&impl_->data_});
      }

      void SeekToLast() {
        pos_ = impl_->root_.get_most_right(my_deref_node_t{&impl_->data_});
      }

      Slice Key() const { return my_deref_node_t{&impl_->data_}(pos_).key(); }

      Slice Value() const {
        return my_deref_node_t{&impl_->data_}(pos_).value();
      }

      bool Valid() const { return pos_ != my_node_t::nil_sentinel; }
    };

    bool InsertKeyValue(const Slice &iKey, const Slice &val) {
      if (readOnly_) return false;
      bool mag = iKey.size() > 39;
      uint32_t klnln = mag ? VarintLength(iKey.size() - 39) : 0;
      uint32_t vlnln = VarintLength(val.size());
      size_t c =
          terark::align_up(9 + klnln + iKey.size() + vlnln + val.size(), 4);
      size_t pos = data_.alloc(c);
      if (pos == size_t(-1)) return false;
      size_t idx = pos >> padding;
      mem_used_ += c;
      my_deref_node_t{&data_}(idx).set_key_value(iKey, val, klnln, vlnln, mag);
      my_stack_t stack;
      {
        ReadLock lock(&mutex_);
        bool ex = threaded_rbtree_find_path_for_unique(
            root_, stack, my_deref_node_t{&data_}, iKey, my_deref_key_t{&data_},
            my_cmp_t{my_deref_key_t{&data_}, rep_->icmp_});
        if (ex) return false;
      }
      WriteLock lock(&mutex_);
      threaded_rbtree_insert(root_, stack, my_deref_node_t{&data_}, idx);
      item_num_++;
      return true;
    }

    void Merge(rbtree_impl<padding> *ptr) {
      WriteLock lock(&mutex_);
      size_t c0 = ptr->MemUsed();
      size_t n = ptr->item_num_;
      size_t pos = data_.alloc(c0);
      my_stack_t stack;
      memcpy(data_.data() + pos, ptr->data_.data(), c0);
      for (size_t i = 0; i < n; ++i) {
        auto &node = my_deref_node_t{&data_}(pos >> padding);
        if (!threaded_rbtree_find_path_for_unique(
                root_, stack, my_deref_node_t{&data_}, node.key(),
                my_deref_key_t{&data_},
                my_cmp_t{my_deref_key_t{&data_}, rep_->icmp_}))
          threaded_rbtree_insert(root_, stack, my_deref_node_t{&data_},
                                 pos >> padding);
        else
          assert(false);
        pos += terark::align_up(node.capicity(), 4);
      }
    }

    bool Contains(const Slice &internal_key) const { return false; }

    InnerIterator NewInnerIterator() { return InnerIterator(this); }

    void MarkReadOnly() { readOnly_ = true; }

    void MarkDestory() { destory_ = true; }

    bool NeedToDestory() { return destory_; }

    bool IsReadOnly() { return readOnly_; }

    uint64_t MemUsed() { return mem_used_; }

    size_t Size() { return item_num_; }

    size_t IterNum() { return iter_cnt_; }

    uint64_t ApprxNumEntries(const Slice &start_ikey, const Slice &end_ikey) {
      auto ApprxRankRatio = [this](const Slice &key) {
        auto deref_key = my_deref_key_t{&data_};
        auto cmp = my_cmp_t{deref_key, rep_->icmp_};
        return uint64_t(
            threaded_rbtree_approximate_rank_ratio(
                root_, my_deref_node_t{&data_}, key, deref_key, cmp) *
            item_num_);
      };

      uint64_t st = ApprxRankRatio(start_ikey);
      uint64_t ed = ApprxRankRatio(end_ikey);

      return ed > st ? ed - st : 0;
    }
  };

  using my_rbtree_t = rbtree_impl<2>;
  struct TlsItem {
    TlsItem() = default;
    TlsItem(const TlsItem &) = delete;
    TlsItem &operator=(const TlsItem &) = delete;

    my_rbtree_t *ptr = nullptr;

    ~TlsItem() {
      if (ptr == nullptr) return;
      ptr->MarkReadOnly();
      ptr->MarkDestory();
    }

    void init(my_rbtree_t *p) { ptr = p; }
    my_rbtree_t *get() { return ptr; }
  };

  std::list<my_rbtree_t> rbt_list_;
  terark::instance_tls<TlsItem> rbtree_ref_;
  std::atomic_bool immutable_;
  const SliceTransform *transform_;
  const MemTableRep::KeyComparator &cmp_;
  const InternalKeyComparator *icmp_;
  port::RWMutex mutex_;
  size_t iter_cnt_ = 0;

 public:
  explicit ThreadedRBTreeRep(const MemTableRep::KeyComparator &compare,
                             Allocator *allocator,
                             const SliceTransform *transform)
      : MemTableRep(allocator),
        transform_(transform),
        immutable_(false),
        cmp_(compare) {
    icmp_ = cmp_.icomparator();
  }

  virtual ~ThreadedRBTreeRep() override { assert(iter_cnt_ > 0); }

  virtual KeyHandle Allocate(const size_t len, char **buf) override {
    assert(false);
    return nullptr;
  }

  virtual void Insert(KeyHandle handle) override { assert(false); }

  char *EncodeKey(const Slice &user_key) { return nullptr; }

  class HeapIterator : public MemTableRep::Iterator {
   private:
    std::vector<my_rbtree_t::InnerIterator> rbt_;
    ThreadedRBTreeRep *rep_;
    const InternalKeyComparator *icmp_;
    bool forward = true;
    std::atomic<size_t> iter_cnt_ = 0;

   public:
    explicit HeapIterator(ThreadedRBTreeRep *rep) : rep_(rep) {
      icmp_ = rep_->cmp_.icomparator();
      for (auto &rbt : rep_->rbt_list_) {
        rbt_.emplace_back(std::move(rbt.NewInnerIterator()));
      }
      ++iter_cnt_;
    }

    virtual ~HeapIterator() override { --iter_cnt_; }

    struct ForwardCmp {
      const InternalKeyComparator *icmp_;
      bool operator()(const my_rbtree_t::InnerIterator &l,
                      const my_rbtree_t::InnerIterator &r) const {
        if (!r.Valid()) return false;
        if (!l.Valid()) return true;
        return icmp_->Compare(l.Key(), r.Key()) > 0;
      }
    };

    struct BackwardCmp {
      const InternalKeyComparator *icmp_;
      bool operator()(const my_rbtree_t::InnerIterator &l,
                      const my_rbtree_t::InnerIterator &r) const {
        if (!r.Valid()) return false;
        if (!l.Valid()) return true;
        return icmp_->Compare(l.Key(), r.Key()) < 0;
      }
    };

    virtual bool Valid() const override {
      return !rbt_.empty() && rbt_.front().Valid();
    }

    virtual void Next() override {
      assert(Valid());
      if (!forward) Seek(GetKey(), nullptr);
      assert(Valid());
      rbt_.begin()->Next();
      terark::adjust_heap_top(rbt_.begin(), rbt_.size(), ForwardCmp{icmp_});
    }

    virtual void Prev() override {
      assert(Valid());
      if (forward) SeekForPrev(GetKey(), nullptr);
      assert(Valid());
      rbt_.begin()->Prev();
      terark::adjust_heap_top(rbt_.begin(), rbt_.size(), BackwardCmp{icmp_});
    }

    virtual void Seek(const Slice &user_key,
                      const char *memtable_key) override {
      for (auto &iter : rbt_)
        if (memtable_key != nullptr)
          iter.Seek(GetLengthPrefixedSlice(memtable_key), rep_->icmp_);
        else
          iter.Seek(user_key, rep_->icmp_);
      std::make_heap(rbt_.begin(), rbt_.end(), ForwardCmp{icmp_});
      forward = true;
    }

    virtual void SeekForPrev(const Slice &user_key,
                             const char *memtable_key) override {
      for (auto &iter : rbt_)
        if (memtable_key != nullptr)
          iter.SeekForPrev(GetLengthPrefixedSlice(memtable_key), rep_->icmp_);
        else
          iter.SeekForPrev(user_key, rep_->icmp_);
      std::make_heap(rbt_.begin(), rbt_.end(), BackwardCmp{icmp_});
      forward = false;
    }

    virtual void SeekToFirst() override {
      for (auto &iter : rbt_) iter.SeekToFirst();
      std::make_heap(rbt_.begin(), rbt_.end(), ForwardCmp{icmp_});
      forward = true;
    }

    virtual void SeekToLast() override {
      for (auto &iter : rbt_) iter.SeekToLast();
      std::make_heap(rbt_.begin(), rbt_.end(), BackwardCmp{icmp_});
      forward = false;
    }

    virtual const char *key() const override {
      assert(false);
      return nullptr;
    }

    virtual Slice GetKey() const override { return rbt_.front().Key(); }

    virtual Slice GetValue() const override { return rbt_.front().Value(); }

    virtual std::pair<Slice, Slice> GetKeyValue() const override {
      return std::pair<Slice, Slice>(rbt_.front().Key(), rbt_.front().Value());
    }
  };

  virtual void Get(const LookupKey &k, void *callback_args,
                   bool (*callback_func)(void *arg,
                                         const KeyValuePair *kv)) override {
    ThreadedRBTreeRep::HeapIterator iter(this);
    EncodedKeyValuePair pair;
    Slice dummy;
    for (iter.Seek(dummy, k.memtable_key().data());
         iter.Valid() && callback_func(callback_args, &iter); iter.Next()) {
    }
  }

  virtual MemTableRep::Iterator *GetIterator(Arena *arena = nullptr) override {
    typedef ThreadedRBTreeRep::HeapIterator i_t;
    return arena ? new (arena->AllocateAligned(sizeof(i_t))) i_t(this)
                 : new i_t(this);
  }

  virtual bool InsertKeyValue(const Slice &internal_key,
                              const Slice &value) override {
    assert(!immutable_);
    auto &ref = rbtree_ref_.get();
    if (ref.get() == nullptr) {
      WriteLock l(&mutex_);
      rbt_list_.emplace_back(this);
      ref.init(&*std::prev(rbt_list_.end()));
    }
    return ref.get()->InsertKeyValue(internal_key, value);
  }

  virtual void MarkReadOnly() override {
    for (auto &rbt : rbt_list_) rbt.MarkReadOnly();
    immutable_ = true;
  }

  virtual size_t ApproximateMemoryUsage() override {
    size_t ret = 0;
    for (auto &rbt : rbt_list_) ret += rbt.MemUsed();
    return ret;
  }

  virtual uint64_t ApproximateNumEntries(const Slice &start_ikey,
                                         const Slice &end_ikey) override {
    size_t n = 0;
    for (auto &rbt : rbt_list_) n += rbt.ApprxNumEntries(start_ikey, end_ikey);
    return n;
  }

  virtual bool Contains(const Slice &internal_key) const override {
    for (auto &rbt : rbt_list_)
      if (rbt.Contains(internal_key)) return true;
    return false;
  }
};
}  // namespace

class TRBTreeRepFactory : public MemTableRepFactory {
 public:
  explicit TRBTreeRepFactory() {}

  using MemTableRepFactory::CreateMemTableRep;
  virtual MemTableRep *CreateMemTableRep(
      const MemTableRep::KeyComparator &comparator, bool needs_dup_key_check,
      Allocator *allocator, const SliceTransform *transform,
      Logger *logger) override {
    return new ThreadedRBTreeRep(comparator, allocator, transform);
  };

  virtual const char *Name() const override { return "TRBTreeRepFactory"; }

  bool IsInsertConcurrentlySupported() const override { return true; }
};

MemTableRepFactory *NewTRBTreeRepFactory() { return new TRBTreeRepFactory(); }

}  // namespace rocksdb
