// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <string>
#include <terark/util/factory.hpp>
#include <terark/util/function.hpp>

namespace rocksdb {

class Slice;

// A Comparator object provides a total order across slices that are
// used as keys in an sstable or a database.  A Comparator implementation
// must be thread-safe since rocksdb may invoke its methods concurrently
// from multiple threads.
class Comparator : public terark::Factoryable<const Comparator*> {
 public:
  virtual ~Comparator() {}

  // Three-way comparison.  Returns value:
  //   < 0 iff "a" < "b",
  //   == 0 iff "a" == "b",
  //   > 0 iff "a" > "b"
  virtual int Compare(const Slice& a, const Slice& b) const = 0;

  // Compares two slices for equality. The following invariant should always
  // hold (and is the default implementation):
  //   Equal(a, b) iff Compare(a, b) == 0
  // Overwrite only if equality comparisons can be done more efficiently than
  // three-way comparisons.
  virtual bool Equal(const Slice& a, const Slice& b) const {
    return Compare(a, b) == 0;
  }

  // The name of the comparator.  Used to check for comparator
  // mismatches (i.e., a DB created with one comparator is
  // accessed using a different comparator.
  //
  // The client of this package should switch to a new name whenever
  // the comparator implementation changes in a way that will cause
  // the relative ordering of any two keys to change.
  //
  // Names starting with "rocksdb." are reserved and should not be used
  // by any clients of this package.
  virtual const char* Name() const = 0;

  // Advanced functions: these are used to reduce the space requirements
  // for internal data structures like index blocks.

  // If *start < limit, changes *start to a short string in [start,limit).
  // Simple comparator implementations may return with *start unchanged,
  // i.e., an implementation of this method that does nothing is correct.
  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const = 0;

  // Changes *key to a short string >= *key.
  // Simple comparator implementations may return with *key unchanged,
  // i.e., an implementation of this method that does nothing is correct.
  virtual void FindShortSuccessor(std::string* key) const = 0;

  // if it is a wrapped comparator, may return the root one.
  // return itself it is not wrapped.
  virtual const Comparator* GetRootComparator() const { return this; }

  // given two keys, determine if t is the successor of s
  virtual bool IsSameLengthImmediateSuccessor(const Slice& /*s*/,
                                              const Slice& /*t*/) const {
    return false;
  }

  // return true if two keys with different byte sequences can be regarded
  // as equal by this comparator.
  // The major use case is to determine if DataBlockHashIndex is compatible
  // with the customized comparator.
  virtual bool CanKeysWithDifferentByteContentsBeEqual() const { return true; }
};

// Return a builtin comparator that uses lexicographic byte-wise
// ordering.  The result remains the property of this module and
// must not be deleted.
extern const Comparator* BytewiseComparator();

// Return a builtin comparator that uses reverse lexicographic byte-wise
// ordering.
extern const Comparator* ReverseBytewiseComparator();

template<class Comp>
struct StdComparareLessType {
  const Comp* cmp;
  template<class T>
  bool operator()(const T& x, const T& y) const {
    return cmp->Compare(x, y) < 0;
  }
};
template<class Comp>
struct StdComparareGreaterType {
  const Comp* cmp;
  template<class T>
  bool operator()(const T& x, const T& y) const {
    return cmp->Compare(x, y) > 0;
  }
};
template<class Comp>
struct StdComparareEqualType {
  const Comp* cmp;
  template<class T>
  bool operator()(const T& x, const T& y) const {
    return cmp->Compare(x, y) == 0;
  }
};
template<class Comp>
StdComparareLessType<Comp> StdCompareLess(const Comp* cmp) {
  return StdComparareLessType<Comp>{cmp};
}
template<class Comp>
StdComparareGreaterType<Comp> StdCompareGreater(const Comp* cmp) {
  return StdComparareGreaterType<Comp>{cmp};
}
template<class Comp>
StdComparareEqualType<Comp> StdCompareEqual(const Comp* cmp) {
  return StdComparareEqualType<Comp>{cmp};
}

// gcc fails with
//   sorry, unimplemented: string literal in function template signature
// if we write cmp.Compare("",""), so use SFINAE_STR
static const char SFINAE_STR[1] = "";

template<class KeyExtractor, class KeyComparator>
auto operator<(const KeyExtractor& ex, const KeyComparator& cmp) ->
decltype(cmp.Compare(SFINAE_STR,SFINAE_STR),
         terark::ExtractorComparator(ex, StdCompareLess(&cmp)))
{
  return terark::ExtractorComparator(ex, StdCompareLess(&cmp));
}

template<class KeyExtractor, class KeyComparator>
auto operator>(const KeyExtractor& ex, const KeyComparator& cmp) ->
decltype(cmp.Compare(SFINAE_STR,SFINAE_STR),
         terark::ExtractorComparator(ex, StdCompareGreater(&cmp)))
{
  return terark::ExtractorComparator(ex, StdCompareGreater(&cmp));
}

template<class KeyExtractor, class KeyComparator>
auto operator==(const KeyExtractor& ex, const KeyComparator& cmp) ->
decltype(cmp.Compare(SFINAE_STR,SFINAE_STR),
         terark::ExtractorComparator(ex, StdCompareEqual(&cmp)))
{
  return terark::ExtractorComparator(ex, StdCompareEqual(&cmp));
}

template<class KeyComparator>
auto operator<(const char[1], const KeyComparator& cmp) ->
decltype(cmp.Compare(SFINAE_STR,SFINAE_STR), StdCompareLess(&cmp))
{
  return StdCompareLess(&cmp);
}
template<class KeyComparator>
auto operator>(const char[1], const KeyComparator& cmp) ->
decltype(cmp.Compare(SFINAE_STR,SFINAE_STR), StdCompareGreater(&cmp))
{
  return StdCompareGreater(&cmp);
}
template<class KeyComparator>
auto operator==(const char[1], const KeyComparator& cmp) ->
decltype(cmp.Compare(SFINAE_STR,SFINAE_STR), StdCompareEqual(&cmp))
{
  return StdCompareEqual(&cmp);
}

template<class Cmp3>
struct Cmp3_to_Less {
  template<class T>
  bool operator()(const T& x, const T& y) const {
    return (*p_cmp3)(x, y) < 0;
  }
  const Cmp3* p_cmp3;
};
template<class Cmp3>
struct Cmp3_to_Greater {
  template<class T>
  bool operator()(const T& x, const T& y) const {
    return (*p_cmp3)(x, y) > 0;
  }
  const Cmp3* p_cmp3;
};
template<class Cmp3>
struct Cmp3_to_Equal {
  template<class T>
  bool operator()(const T& x, const T& y) const {
    return (*p_cmp3)(x, y) == 0;
  }
  const Cmp3* p_cmp3;
};

template<class Cmp3>
auto operator<(const char[1], const Cmp3& cmp) ->
decltype(cmp(SFINAE_STR,SFINAE_STR), Cmp3_to_Less<Cmp3>{&cmp})
{
  return Cmp3_to_Less<Cmp3>{&cmp};
}
template<class Cmp3>
auto operator>(const char[1], const Cmp3& cmp) ->
decltype(cmp(SFINAE_STR,SFINAE_STR), Cmp3_to_Greater<Cmp3>{&cmp})
{
  return Cmp3_to_Greater<Cmp3>{&cmp};
}
template<class Cmp3>
auto operator>(const char[1], const Cmp3& cmp) ->
decltype(cmp(SFINAE_STR,SFINAE_STR), Cmp3_to_Equal<Cmp3>{&cmp})
{
  return Cmp3_to_Equal<Cmp3>{&cmp};
}

}  // namespace rocksdb
