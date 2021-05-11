#ifndef ROCKSDB_LITE

#include <atomic>

#include "db/memtable.h"
#include "memtable/skiplist.h"
#include "rocksdb/terark_namespace.h"

namespace TERARKDB_NAMESPACE {
namespace {

using Key = const char*;
using Pointer = std::atomic<void*>;
using MemtableSkipList = SkipList<Key, const MemTableRep::KeyComparator&>;

class FullListIterator : public MemTableRep::Iterator {
 public:
  explicit FullListIterator(MemtableSkipList* list, Allocator* allocator)
      : iter_(list), full_list_(list), allocator_(allocator) {}

  ~FullListIterator() {}

  // Returns true iff the iterator is positioned at a valid node.
  bool Valid() const override { return iter_.Valid(); }

  // Returns the key at the current position.
  // REQUIRES: Valid()
  const char* EncodedKey() const override {
    assert(Valid());
    return iter_.key();
  }

  // Advances to the next position.
  // REQUIRES: Valid()
  void Next() override {
    assert(Valid());
    iter_.Next();
  }

  // Advances to the previous position.
  // REQUIRES: Valid()
  void Prev() override {
    assert(Valid());
    iter_.Prev();
  }

  // Advance to the first entry with a key >= target
  void Seek(const Slice& internal_key, const char* memtable_key) override {
    const char* encoded_key = (memtable_key != nullptr)
                                  ? memtable_key
                                  : EncodeKey(&tmp_, internal_key);
    iter_.Seek(encoded_key);
  }

  // Advance to the first entry with a key <= target
  void SeekForPrev(const Slice& internal_key,
                   const char* memtable_key) override {
    const char* encoded_key = (memtable_key != nullptr)
                                  ? memtable_key
                                  : EncodeKey(&tmp_, internal_key);
    iter_.SeekForPrev(encoded_key);
  }

  // Position at the first entry in collection.
  // Final state of iterator is Valid() iff collection is not empty.
  void SeekToFirst() override { iter_.SeekToFirst(); }

  // Position at the last entry in collection.
  // Final state of iterator is Valid() iff collection is not empty.
  void SeekToLast() override { iter_.SeekToLast(); }

  bool IsSeekForPrevSupported() const override { return true; }

 private:
  MemtableSkipList::Iterator iter_;
  // To destruct with the iterator.
  std::unique_ptr<MemtableSkipList> full_list_;
  std::unique_ptr<Allocator> allocator_;
  std::string tmp_;  // For passing to EncodeKey
};

}  // namespace
}  // namespace TERARKDB_NAMESPACE
#endif
