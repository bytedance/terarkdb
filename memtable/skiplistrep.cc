//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "db/memtable.h"
#include "memtable/inlineskiplist.h"
#include "rocksdb/memtablerep.h"
#include "util/arena.h"
#include "util/string_util.h"

#ifdef WITH_TERARK_ZIP
#include "table/terark_zip_internal.h"
#endif

namespace rocksdb {
namespace {
template <class ComparatorType>
class SkipListRep : public MemTableRep {
  InlineSkipList<const ComparatorType&> skip_list_;
  const ComparatorType& cmp_;
  const SliceTransform* transform_;
  const size_t lookahead_;

  friend class LookaheadIterator;

 public:
  explicit SkipListRep(const ComparatorType& compare, Allocator* allocator,
                       const SliceTransform* transform, const size_t lookahead)
      : MemTableRep(allocator),
        skip_list_(compare, allocator),
        cmp_(compare),
        transform_(transform),
        lookahead_(lookahead) {}

  virtual KeyHandle Allocate(const size_t len, char** buf) override {
    *buf = skip_list_.AllocateKey(len);
    return static_cast<KeyHandle>(*buf);
  }

  virtual bool InsertKeyValue(const Slice& internal_key,
                              const SliceParts& value) override {
    size_t buf_size = EncodeKeyValueSize(internal_key, value);
    char* buf;
    KeyHandle handle = Allocate(buf_size, &buf);
    EncodeKeyValue(internal_key, value, buf);
    return skip_list_.Insert(static_cast<char*>(handle));
  }

  virtual bool InsertKeyValueWithHint(const Slice& internal_key,
                                      const SliceParts& value,
                                      void** hint) override {
    size_t buf_size = EncodeKeyValueSize(internal_key, value);
    char* buf;
    KeyHandle handle = Allocate(buf_size, &buf);
    EncodeKeyValue(internal_key, value, buf);
    return skip_list_.InsertWithHint(static_cast<char*>(handle), hint);
  }

  bool InsertKeyValueConcurrently(const Slice& internal_key,
                                  const SliceParts& value) override {
    size_t buf_size = EncodeKeyValueSize(internal_key, value);
    char* buf;
    KeyHandle handle = Allocate(buf_size, &buf);
    EncodeKeyValue(internal_key, value, buf);
    return skip_list_.InsertConcurrently(static_cast<char*>(handle));
  }

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  virtual void Insert(KeyHandle handle) override {
    skip_list_.Insert(static_cast<char*>(handle));
  }

  virtual void InsertWithHint(KeyHandle handle, void** hint) override {
    skip_list_.InsertWithHint(static_cast<char*>(handle), hint);
  }

  virtual void InsertConcurrently(KeyHandle handle) override {
    skip_list_.InsertConcurrently(static_cast<char*>(handle));
  }

  // Returns true iff an entry that compares equal to key is in the list.
  virtual bool Contains(const Slice& internal_key) const override {
    std::string memtable_key;
    return skip_list_.Contains(EncodeKey(&memtable_key, internal_key));
  }

  virtual size_t ApproximateMemoryUsage() override {
    // All memory is allocated through allocator; nothing to report here
    return 0;
  }

  virtual void Get(const LookupKey& k, void* callback_args,
                   bool (*callback_func)(void* arg, const Slice& key,
                                         const char* value)) override {
    SkipListRep::Iterator iter(&skip_list_);
    Slice dummy_slice;
    for (iter.Seek(dummy_slice, k.memtable_key().data());
         iter.Valid() && callback_func(callback_args, iter.key(), iter.value());
         iter.Next()) {
    }
  }

  uint64_t ApproximateNumEntries(const Slice& start_ikey,
                                 const Slice& end_ikey) override {
    std::string tmp;
    uint64_t start_count =
        skip_list_.EstimateCount(EncodeKey(&tmp, start_ikey));
    uint64_t end_count = skip_list_.EstimateCount(EncodeKey(&tmp, end_ikey));
    return (end_count >= start_count) ? (end_count - start_count) : 0;
  }

  virtual ~SkipListRep() override {}

  // Iteration over the contents of a skip list
  class Iterator : public MemTableRep::Iterator {
    typename InlineSkipList<const ComparatorType&>::Iterator iter_;

   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const InlineSkipList<const ComparatorType&>* list)
        : iter_(list) {}

    virtual ~Iterator() override {}

    // Returns true iff the iterator is positioned at a valid node.
    virtual bool Valid() const override { return iter_.Valid(); }

    // Returns the key at the current position.
    // REQUIRES: Valid()
    virtual const char* EncodedKey() const override { return iter_.key(); }

    // Advances to the next position.
    // REQUIRES: Valid()
    virtual void Next() override { iter_.Next(); }

    // Advances to the previous position.
    // REQUIRES: Valid()
    virtual void Prev() override { iter_.Prev(); }

    // Advance to the first entry with a key >= target
    virtual void Seek(const Slice& user_key,
                      const char* memtable_key) override {
      if (memtable_key != nullptr) {
        iter_.Seek(memtable_key);
      } else {
        iter_.Seek(EncodeKey(&tmp_, user_key));
      }
    }

    // Retreat to the last entry with a key <= target
    virtual void SeekForPrev(const Slice& user_key,
                             const char* memtable_key) override {
      if (memtable_key != nullptr) {
        iter_.SeekForPrev(memtable_key);
      } else {
        iter_.SeekForPrev(EncodeKey(&tmp_, user_key));
      }
    }

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToFirst() override { iter_.SeekToFirst(); }

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    virtual void SeekToLast() override { iter_.SeekToLast(); }

    virtual bool IsSeekForPrevSupported() const override { return true; }

   protected:
    std::string tmp_;  // For passing to EncodeKey
  };

  // Iterator over the contents of a skip list which also keeps track of the
  // previously visited node. In Seek(), it examines a few nodes after it
  // first, falling back to O(log n) search from the head of the list only if
  // the target key hasn't been found.
  class LookaheadIterator : public MemTableRep::Iterator {
   public:
    explicit LookaheadIterator(const SkipListRep& rep)
        : rep_(rep), iter_(&rep_.skip_list_), prev_(iter_) {}

    virtual ~LookaheadIterator() override {}

    virtual bool Valid() const override { return iter_.Valid(); }

    virtual const char* EncodedKey() const override {
      assert(Valid());
      return iter_.key();
    }

    virtual void Next() override {
      assert(Valid());

      bool advance_prev = true;
      if (prev_.Valid()) {
        auto k1 = rep_.UserKey(prev_.key());
        auto k2 = rep_.UserKey(iter_.key());

        if (k1.compare(k2) == 0) {
          // same user key, don't move prev_
          advance_prev = false;
        } else if (rep_.transform_) {
          // only advance prev_ if it has the same prefix as iter_
          auto t1 = rep_.transform_->Transform(k1);
          auto t2 = rep_.transform_->Transform(k2);
          advance_prev = t1.compare(t2) == 0;
        }
      }

      if (advance_prev) {
        prev_ = iter_;
      }
      iter_.Next();
    }

    virtual void Prev() override {
      assert(Valid());
      iter_.Prev();
      prev_ = iter_;
    }

    virtual void Seek(const Slice& internal_key,
                      const char* memtable_key) override {
      const char* encoded_key = (memtable_key != nullptr)
                                    ? memtable_key
                                    : EncodeKey(&tmp_, internal_key);

      if (prev_.Valid() && rep_.cmp_(encoded_key, prev_.key()) >= 0) {
        // prev_.key() is smaller or equal to our target key; do a quick
        // linear search (at most lookahead_ steps) starting from prev_
        iter_ = prev_;

        size_t cur = 0;
        while (cur++ <= rep_.lookahead_ && iter_.Valid()) {
          if (rep_.cmp_(encoded_key, iter_.key()) <= 0) {
            return;
          }
          Next();
        }
      }

      iter_.Seek(encoded_key);
      prev_ = iter_;
    }

    virtual void SeekForPrev(const Slice& internal_key,
                             const char* memtable_key) override {
      const char* encoded_key = (memtable_key != nullptr)
                                    ? memtable_key
                                    : EncodeKey(&tmp_, internal_key);
      iter_.SeekForPrev(encoded_key);
      prev_ = iter_;
    }

    virtual void SeekToFirst() override {
      iter_.SeekToFirst();
      prev_ = iter_;
    }

    virtual void SeekToLast() override {
      iter_.SeekToLast();
      prev_ = iter_;
    }

    virtual bool IsSeekForPrevSupported() const override { return true; }

   protected:
    std::string tmp_;  // For passing to EncodeKey

   private:
    const SkipListRep& rep_;
    typename InlineSkipList<const ComparatorType&>::Iterator iter_;
    typename InlineSkipList<const ComparatorType&>::Iterator prev_;
  };

  virtual MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override {
    if (lookahead_ > 0) {
      void* mem =
          arena ? arena->AllocateAligned(sizeof(SkipListRep::LookaheadIterator))
                :
                operator new(sizeof(SkipListRep::LookaheadIterator));
      return new (mem) SkipListRep::LookaheadIterator(*this);
    } else {
      void* mem = arena ? arena->AllocateAligned(sizeof(SkipListRep::Iterator))
                        :
                        operator new(sizeof(SkipListRep::Iterator));
      return new (mem) SkipListRep::Iterator(&skip_list_);
    }
  }
};

struct BytewiseKeyComparator {
  typedef rocksdb::Slice DecodedType;
  const InternalKeyComparator comparator;
  DecodedType decode_key(const char* key) const {
    return GetLengthPrefixedSlice(key);
  }
  explicit BytewiseKeyComparator(const InternalKeyComparator& c)
      : comparator(c) {}

  int compare_impl(const Slice& k1, const Slice& k2) const {
    int ret = memcmp(k1.data_, k2.data_, std::min(k1.size_, k2.size_) - 8);
    if (ret != 0) {
      return ret;
    } else if (k1.size_ != k2.size_) {
      return k1.size_ > k2.size_ ? +1 : -1;
    }
    const uint64_t k1_seq = DecodeFixed64(k1.data_ + k1.size_ - 8) >> 8;
    const uint64_t k2_seq = DecodeFixed64(k2.data_ + k2.size_ - 8) >> 8;
    if (k1_seq > k2_seq) {
      ret = -1;
    } else if (k1_seq < k2_seq) {
      ret = +1;
    }
    return ret;
  }

  int operator()(const char* prefix_len_key1,
                 const char* prefix_len_key2) const {
    return compare_impl(GetLengthPrefixedSlice(prefix_len_key1),
                        GetLengthPrefixedSlice(prefix_len_key2));
  }
  int operator()(const char* prefix_len_key, const DecodedType& key) const {
    return compare_impl(GetLengthPrefixedSlice(prefix_len_key), key);
  }
  const InternalKeyComparator* icomparator() const { return &comparator; }
};

struct ReverseBytewiseKeyComparator {
  typedef rocksdb::Slice DecodedType;
  const InternalKeyComparator comparator;
  DecodedType decode_key(const char* key) const {
    return GetLengthPrefixedSlice(key);
  }
  explicit ReverseBytewiseKeyComparator(const InternalKeyComparator& c)
      : comparator(c) {}

  int compare_impl(const Slice& k1, const Slice& k2) const {
    int ret = memcmp(k1.data_, k2.data_, std::min(k1.size_, k2.size_) - 8);
    if (ret != 0) {
      return -ret;
    } else if (k1.size_ != k2.size_) {
      return k1.size_ > k2.size_ ? -1 : +1;
    }
    const uint64_t k1_seq = DecodeFixed64(k1.data_ + k1.size_ - 8) >> 8;
    const uint64_t k2_seq = DecodeFixed64(k2.data_ + k2.size_ - 8) >> 8;
    if (k1_seq > k2_seq) {
      ret = -1;
    } else if (k1_seq < k2_seq) {
      ret = +1;
    }
    return ret;
  }

  int operator()(const char* prefix_len_key1,
                 const char* prefix_len_key2) const {
    return compare_impl(GetLengthPrefixedSlice(prefix_len_key1),
                        GetLengthPrefixedSlice(prefix_len_key2));
  }
  int operator()(const char* prefix_len_key, const DecodedType& key) const {
    return compare_impl(GetLengthPrefixedSlice(prefix_len_key), key);
  }
  const InternalKeyComparator* icomparator() const { return &comparator; }
};
}  // namespace

#ifndef WITH_TERARK_ZIP
// These functions are defined inside terarkzip, we should remove them in the
// future.
bool IsForwardBytewiseComparator(const Slice name) {
  if (name.ToString().find("RocksDB_SE_") == 0) {
    return true;
  }
  return name == "leveldb.BytewiseComparator";
}
inline bool IsForwardBytewiseComparator(const Comparator* cmp) {
  return IsForwardBytewiseComparator(cmp->Name());
}
bool IsBackwardBytewiseComparator(const Slice name) {
  if (name.ToString().find("rev:RocksDB_SE_") == 0) {
    return true;
  }
  return name == "rocksdb.ReverseBytewiseComparator";
}
inline bool IsBackwardBytewiseComparator(const Comparator* cmp) {
  return IsBackwardBytewiseComparator(cmp->Name());
}
#endif

MemTableRep* SkipListFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& compare, bool /*needs_dup_key_check*/,
    Allocator* allocator, const SliceTransform* transform, Logger* /*logger*/) {
  auto uc = compare.icomparator()->user_comparator();
  if (IsForwardBytewiseComparator(uc)) {
    static BytewiseKeyComparator bytewise_compare =
        BytewiseKeyComparator(InternalKeyComparator(BytewiseComparator()));
    return new SkipListRep<BytewiseKeyComparator>(bytewise_compare, allocator,
                                                  transform, lookahead_);
  } else if (IsBackwardBytewiseComparator(uc)) {
    static ReverseBytewiseKeyComparator reverse_bytewise_compare =
        ReverseBytewiseKeyComparator(
            InternalKeyComparator(ReverseBytewiseComparator()));
    return new SkipListRep<ReverseBytewiseKeyComparator>(
        reverse_bytewise_compare, allocator, transform, lookahead_);
  }
  return new SkipListRep<MemTableRep::KeyComparator>(compare, allocator,
                                                     transform, lookahead_);
}

static MemTableRepFactory* NewSkipListFactory(
    const std::unordered_map<std::string, std::string>& options, Status*) {
  auto f = options.find("lookahead");
  size_t lookahead = 0;
  if (options.end() != f) lookahead = ParseSizeT(f->second);
  return new SkipListFactory(lookahead);
}

ROCKSDB_REGISTER_MEM_TABLE("skip_list", SkipListFactory);

}  // namespace rocksdb
