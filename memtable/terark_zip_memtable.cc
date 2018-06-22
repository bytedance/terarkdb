//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <utility>
#include <iterator>
#include <memory>
#include <cstdlib>
#include <vector>
#include <algorithm>
#include <atomic>

#include "db/memtable.h"
#include "rocksdb/memtablerep.h"
#include "util/arena.h"
#include "util/mutexlock.h"
#include "util/threaded_rbtree.h"
#include <terark/fsa/dynamic_patricia_trie.hpp>

namespace rocksdb {
namespace {

    class PTrieRep : public MemTableRep {
        typedef size_t size_type;
        static size_type constexpr max_stack_depth = 2 * (sizeof(uintptr_t) * 8 - 1);

        typedef threaded_rbtree_node_t<uintptr_t, std::false_type> node_t;
        typedef threaded_rbtree_stack_t<node_t, max_stack_depth> stack_t;
        typedef threaded_rbtree_root_t<node_t, std::false_type, std::false_type> root_t;

        struct rep_node_t {
            node_t node;
            uint64_t tag;
            char prefixed_value[1];
        };
        static size_type constexpr rep_node_size = sizeof(node_t) + sizeof(uint64_t);

        struct deref_node_t {
            node_t &operator()(size_type index) {
                return *(node_t*)index;
            }
        };
        struct deref_key_t {
            uint64_t operator()(size_type index) const {
                return ((rep_node_t*)index)->tag;
            }
        };
        typedef std::greater<uint64_t> key_compare_t;

        static const char* build_key(terark::fstring user_key, uintptr_t index, std::string* buffer) {
            rep_node_t* node = (rep_node_t*)index;
            uint32_t value_size;
            const char* value_ptr = GetVarint32Ptr(node->prefixed_value, node->prefixed_value + 5, &value_size);
            buffer->resize(0);
            buffer->reserve(user_key.size() + value_size + 18);
            PutVarint32(buffer, user_key.size() + 8);
            buffer->append(user_key.data(), user_key.size());
            PutFixed64(buffer, node->tag);
            buffer->append(node->prefixed_value, value_ptr - node->prefixed_value + value_size);
            return buffer->data();
        }

    private:
        std::atomic_uintptr_t memory_size_;
        mutable terark::PatriciaTrie trie_;
        mutable port::RWMutex mutex_;
        std::atomic_bool immutable_;
        std::atomic_size_t num_entries_;

    public:
        explicit PTrieRep(const MemTableRep::KeyComparator &compare, Allocator *allocator,
                          const SliceTransform *) : MemTableRep(allocator)
                                                  , memory_size_(0)
                                                  , trie_(sizeof(root_t), allocator->BlockSize())
                                                  , immutable_(false)
                                                  , num_entries_(0) {
        }

        virtual KeyHandle Allocate(const size_t len, char **buf) override {
            char *mem = (char*)malloc(len + 4);
            *buf = mem;
            return static_cast<KeyHandle>(mem);
        }

        // Insert key into the list.
        // REQUIRES: nothing that compares equal to key is currently in the list.
        virtual void Insert(KeyHandle handle) override {
            char *entry = (char*)handle;
            uint32_t key_length;
            const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
            const char* key_end = key_ptr + key_length;
            terark::fstring key(key_ptr, key_end - 8);
            uint64_t tag = DecodeFixed64(key.end());
            uint32_t value_size;
            const char* value_ptr = GetVarint32Ptr(key_end, key_end + 5, &value_size);
            value_size += value_ptr - key_end;
            rep_node_t* node = (rep_node_t*)allocator_->AllocateAligned(rep_node_size + value_size);
            node->tag = tag;
            memcpy(node->prefixed_value, key_end, value_size);
            
            terark::PatriciaTrie::AccessToken token(&trie_);
            root_t new_root;
            stack_t stack;
            stack.height = 0;
            threaded_rbtree_insert(new_root, stack, deref_node_t(), (uintptr_t)node);

            WriteLock _lock(&mutex_);
            if (!trie_.insert(key, &new_root, &token)) {
                root_t* root = (root_t*)token.value();
                threaded_rbtree_find_path_for_multi(*root, stack, deref_node_t(), tag,
                                                    deref_key_t(), key_compare_t());
                threaded_rbtree_insert(*root, stack, deref_node_t(), (uintptr_t)node);
            }
            free(handle);
            ++num_entries_;
        }

        // Returns true iff an entry that compares equal to key is in the list.
        virtual bool Contains(const char *key) const override {
            Slice internal_key = GetLengthPrefixedSlice(key);
            terark::fstring find_key(internal_key.data(), internal_key.size() - 8);
            uint64_t tag = DecodeFixed64(find_key.end());
            terark::PatriciaTrie::AccessToken token(&trie_);
            if (immutable_) {
                if (!trie_.lookup(find_key, &token)) {
                    return false;
                }
                root_t* root = (root_t*)token.value();
                auto index = threaded_rbtree_equal_unique(*root, deref_node_t(), tag,
                                                          deref_key_t(), key_compare_t());
                return index != node_t::nil_sentinel;
            } else {
                ReadLock _lock(&mutex_);
                if (!trie_.lookup(find_key, &token)) {
                    return false;
                }
                root_t* root = (root_t*)token.value();
                auto index = threaded_rbtree_equal_unique(*root, deref_node_t(), tag,
                                                          deref_key_t(), key_compare_t());
                return index != node_t::nil_sentinel;
            }
        }

        virtual void MarkReadOnly() override {
            immutable_ = true;
        }

        virtual size_t ApproximateMemoryUsage() override {
            return memory_size_.load();
        }

        virtual uint64_t ApproximateNumEntries(const Slice& start_ikey,
                                               const Slice& end_ikey) override {
            return 0;
        }

        virtual void
        Get(const LookupKey &k, void *callback_args,
            bool (*callback_func)(void *arg, const char *entry)) override {
            Slice internal_key = k.internal_key();
            terark::fstring find_key(internal_key.data(), internal_key.size() - 8);
            uint64_t tag = DecodeFixed64(find_key.end());
            terark::PatriciaTrie::AccessToken token(&trie_);
            std::string buffer;

            auto get_impl = [&] {
                if (!trie_.lookup(find_key, &token)) {
                    return;
                }
                root_t* root = (root_t*)token.value();
                auto index = threaded_rbtree_lower_bound(*root, deref_node_t(), tag,
                                                         deref_key_t(), key_compare_t());
                while (index != node_t::nil_sentinel &&
                       callback_func(callback_args, build_key(find_key, index, &buffer))) {
                    index = threaded_rbtree_move_next(index, deref_node_t());
                }
            };
            if (immutable_) {
                get_impl();
            } else {
                ReadLock _lock(&mutex_);
                get_impl();
            }
        }

        virtual ~PTrieRep() override {}
        
        // used for immutable
        struct DummyLock {
            template<class T> DummyLock(T const &) {}
        };

        template<class Lock>
        class Iterator : public MemTableRep::Iterator {
            mutable std::string buffer_;
            PTrieRep* rep_;
            std::unique_ptr<terark::ADFA_LexIterator> iter_;
            uintptr_t where_;

            friend class PTrieRep;

            Iterator(PTrieRep* rep)
                : rep_(rep)
                , iter_(rep_->trie_.adfa_make_iter())
                , where_(node_t::nil_sentinel) {
            }

        public:
            virtual ~Iterator() override {}

            // Returns true iff the iterator is positioned at a valid node.
            virtual bool Valid() const override {
                return where_ != node_t::nil_sentinel;
            }

            // Returns the key at the current position.
            // REQUIRES: Valid()
            virtual const char *key() const override {
                Lock _lock(&rep_->mutex_);
                return build_key(iter_->word(), where_, &buffer_);
            }

            // Advances to the next position.
            // REQUIRES: Valid()
            virtual void Next() override {
                Lock _lock(&rep_->mutex_);
                where_ = threaded_rbtree_move_next(where_, deref_node_t());
                if (where_ == node_t::nil_sentinel && iter_->incr()) {
                    root_t* root = (root_t*)rep_->trie_.get_valptr(iter_->word_state());
                    where_ = root->get_most_left(deref_node_t());
                }
            }

            // Advances to the previous position.
            // REQUIRES: Valid()
            virtual void Prev() override {
                Lock _lock(&rep_->mutex_);
                where_ = threaded_rbtree_move_prev(where_, deref_node_t());
                if (where_ == node_t::nil_sentinel && iter_->decr()) {
                    root_t* root = (root_t*)rep_->trie_.get_valptr(iter_->word_state());
                    where_ = root->get_most_right(deref_node_t());
                }
            }

            // Advance to the first entry with a key >= target
            virtual void Seek(const Slice &user_key, const char *memtable_key)
            override {
                if (rep_->num_entries_.load() == 0) {
                    where_ = node_t::nil_sentinel;
                    return;
                }
                terark::fstring find_key;
                if(memtable_key != nullptr) {
                    Slice internal_key = GetLengthPrefixedSlice(memtable_key);
                    find_key = terark::fstring(internal_key.data(), internal_key.size() - 8);
                } else {
                    find_key = terark::fstring(user_key.data(), user_key.size() - 8);
                }
                uint64_t tag = DecodeFixed64(find_key.end());

                Lock _lock(&rep_->mutex_);
                if (!iter_->seek_lower_bound(find_key)) {
                    where_ = node_t::nil_sentinel;
                    return;
                }
                root_t* root = (root_t*)rep_->trie_.get_valptr(iter_->word_state());
                where_ = threaded_rbtree_lower_bound(*root, deref_node_t(), tag,
                                                     deref_key_t(), key_compare_t());
                if (where_ == node_t::nil_sentinel) {
                    if (!iter_->incr()) {
                        where_ = node_t::nil_sentinel;
                        return;
                    }
                    root = (root_t*)rep_->trie_.get_valptr(iter_->word_state());
                    where_ = root->get_most_left(deref_node_t());
                }
            }

            // retreat to the first entry with a key <= target
            virtual void SeekForPrev(const Slice& user_key, const char* memtable_key)
            override {
                if (rep_->num_entries_.load() == 0) {
                    where_ = node_t::nil_sentinel;
                    return;
                }
                terark::fstring find_key;
                if(memtable_key != nullptr) {
                    Slice internal_key = GetLengthPrefixedSlice(memtable_key);
                    find_key = terark::fstring(internal_key.data(), internal_key.size() - 8);
                } else {
                    find_key = terark::fstring(user_key.data(), user_key.size() - 8);
                }
                uint64_t tag = DecodeFixed64(find_key.end());

                Lock _lock(&rep_->mutex_);
                if (!iter_->seek_rev_lower_bound(find_key)) {
                    where_ = node_t::nil_sentinel;
                    return;
                }
                root_t* root = (root_t*)rep_->trie_.get_valptr(iter_->word_state());
                where_ = threaded_rbtree_reverse_lower_bound(*root, deref_node_t(), tag,
                                                             deref_key_t(), key_compare_t());
                if (where_ == node_t::nil_sentinel) {
                    if (!iter_->decr()) {
                        where_ = node_t::nil_sentinel;
                        return;
                    }
                    root = (root_t*)rep_->trie_.get_valptr(iter_->word_state());
                    where_ = root->get_most_right(deref_node_t());
                }
            }

            // Position at the first entry in list.
            // Final state of iterator is Valid() iff list is not empty.
            virtual void SeekToFirst() override {
                Lock _lock(&rep_->mutex_);
                if (iter_->seek_begin()) {
                    root_t* root = (root_t*)rep_->trie_.get_valptr(iter_->word_state());
                    where_ = root->get_most_left(deref_node_t());
                } else {
                    where_ = node_t::nil_sentinel;
                }
            }

            // Position at the last entry in list.
            // Final state of iterator is Valid() iff list is not empty.
            virtual void SeekToLast() override {
                Lock _lock(&rep_->mutex_);
                if (iter_->seek_end() && iter_->decr()) {
                    root_t* root = (root_t*)rep_->trie_.get_valptr(iter_->word_state());
                    where_ = root->get_most_right(deref_node_t());
                } else {
                    where_ = node_t::nil_sentinel;
                }
            }
        };
        virtual MemTableRep::Iterator *GetIterator(Arena *arena = nullptr) override {
            if(immutable_) {
                void *mem =
                        arena ? arena->AllocateAligned(sizeof(PTrieRep::Iterator<DummyLock>))
                              : operator new(sizeof(PTrieRep::Iterator<DummyLock>));
                return new(mem) PTrieRep::Iterator<DummyLock>(this);
            } else {
                void *mem =
                        arena ? arena->AllocateAligned(sizeof(PTrieRep::Iterator<ReadLock>))
                              : operator new(sizeof(PTrieRep::Iterator<ReadLock>));
                return new(mem) PTrieRep::Iterator<ReadLock>(this);
            }
        }
    };

    class PTrieMemtableRepFactory : public MemTableRepFactory {
    public:
        virtual ~PTrieMemtableRepFactory(){}

        using MemTableRepFactory::CreateMemTableRep;
        virtual MemTableRep *CreateMemTableRep(
                const MemTableRep::KeyComparator &compare, Allocator *allocator,
                const SliceTransform *transform, Logger *logger) override {
          return new PTrieRep(compare, allocator, transform);
        }

        virtual const char *Name() const override {
          return "PatriciaTrieRepFactory";
        }

        virtual bool IsInsertConcurrentlySupported() const override {
            return false;
        }
    };
}

MemTableRepFactory *NewPatriciaTrieRepFactory()
{
  return new PTrieMemtableRepFactory();
}

} // namespace rocksdb
