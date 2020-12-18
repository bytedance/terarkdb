#ifdef WITH_TERARK_ZIP
#include <terark/valvec.hpp>
#else

#include <stdlib.h>

#include "utilities/util/function.hpp"

namespace terark {

/// STL like algorithm with array/RanIt and size_t param

template <class RanIt, class Key, class KeyExtractor>
size_t lower_bound_ex_n(RanIt a, size_t low, size_t upp, const Key& key,
                        KeyExtractor keyEx) {
  assert(low <= upp);
  size_t i = low, j = upp;
  while (i < j) {
    size_t mid = (i + j) / 2;
    if (keyEx(a[mid]) < key)
      i = mid + 1;
    else
      j = mid;
  }
  return i;
}
template <class RanIt, class Key, class KeyExtractor, class Comp>
size_t lower_bound_ex_n(RanIt a, size_t low, size_t upp, const Key& key,
                        KeyExtractor keyEx, Comp comp) {
  assert(low <= upp);
  size_t i = low, j = upp;
  while (i < j) {
    size_t mid = (i + j) / 2;
    if (comp(keyEx(a[mid]), key))
      i = mid + 1;
    else
      j = mid;
  }
  return i;
}

template <class RanIt, class Key>
size_t lower_bound_n(RanIt a, size_t low, size_t upp, const Key& key) {
  size_t i = low, j = upp;
  while (i < j) {
    size_t mid = (i + j) / 2;
    if (a[mid] < key)
      i = mid + 1;
    else
      j = mid;
  }
  return i;
}
template <class RanIt, class Key, class Comp>
size_t lower_bound_n(RanIt a, size_t low, size_t upp, const Key& key,
                     Comp comp) {
  size_t i = low, j = upp;
  while (i < j) {
    size_t mid = (i + j) / 2;
    if (comp(a[mid], key))
      i = mid + 1;
    else
      j = mid;
  }
  return i;
}

template <class RanIt, class Key>
size_t upper_bound_n(RanIt a, size_t low, size_t upp, const Key& key) {
  size_t i = low, j = upp;
  while (i < j) {
    size_t mid = (i + j) / 2;
    if (key < a[mid])
      j = mid;
    else
      i = mid + 1;
  }
  return i;
}

template <class RanIt, class Key, class Comp>
size_t upper_bound_n(RanIt a, size_t low, size_t upp, const Key& key,
                     Comp comp) {
  size_t i = low, j = upp;
  while (i < j) {
    size_t mid = (i + j) / 2;
    if (comp(key, a[mid]))
      j = mid;
    else
      i = mid + 1;
  }
  return i;
}

template <class RanIt, class Key>
std::pair<size_t, size_t> equal_range_n(RanIt a, size_t low, size_t upp,
                                        const Key& key) {
  size_t i = low, j = upp;
  while (i < j) {
    size_t mid = (i + j) / 2;
    if (a[mid] < key)
      i = mid + 1;
    else if (key < a[mid])
      j = mid;
    else
      return std::pair<size_t, size_t>(
          lower_bound_n<RanIt, Key>(a, i, mid, key),
          upper_bound_n<RanIt, Key>(a, mid + 1, upp, key));
  }
  return std::pair<size_t, size_t>(i, i);
}

template <class RanIt, class Key, class Comp>
std::pair<size_t, size_t> equal_range_n(RanIt a, size_t low, size_t upp,
                                        const Key& key, Comp comp) {
  size_t i = low, j = upp;
  while (i < j) {
    size_t mid = (i + j) / 2;
    if (comp(a[mid], key))
      i = mid + 1;
    else if (comp(key, a[mid]))
      j = mid;
    else
      return std::pair<size_t, size_t>(
          lower_bound_n<RanIt, Key, Comp>(a, i, mid, key, comp),
          upper_bound_n<RanIt, Key, Comp>(a, mid + 1, j, key, comp));
  }
  return std::pair<size_t, size_t>(i, i);
}

template <class RanIt, class Key>
bool binary_search_n(RanIt a, size_t low, size_t upp, const Key& key) {
  size_t f = lower_bound_n<RanIt, Key>(a, low, upp, key);
  return f < upp && !(key < a[f]);
}
template <class RanIt, class Key, class Comp>
bool binary_search_n(RanIt a, size_t low, size_t upp, const Key& key,
                     Comp comp) {
  size_t f = lower_bound_n<RanIt, Key, Comp>(a, low, upp, key, comp);
  return f < upp && !comp(key, a[f]);
}

template <class RanIt>
void sort_n(RanIt a, size_t low, size_t upp) {
  std::sort<RanIt>(a + low, a + upp);
}
template <class RanIt, class Comp>
void sort_n(RanIt a, size_t low, size_t upp, Comp comp) {
  std::sort<RanIt, Comp>(a + low, a + upp, comp);
}

template <class RanIt, class Key>
size_t lower_bound_0(RanIt a, size_t n, const Key& key) {
  return lower_bound_n<RanIt, Key>(a, 0, n, key);
}
template <class RanIt, class Key, class Comp>
size_t lower_bound_0(RanIt a, size_t n, const Key& key, Comp comp) {
  return lower_bound_n<RanIt, Key, Comp>(a, 0, n, key, comp);
}
template <class Container, class Key>
size_t lower_bound_a(const Container& a, const Key& key) {
  typedef typename Container::const_iterator RanIt;
  return lower_bound_n<RanIt, Key>(a.begin(), 0, a.size(), key);
}
template <class Container, class Key, class Comp>
size_t lower_bound_a(const Container& a, const Key& key, Comp comp) {
  typedef typename Container::const_iterator RanIt;
  return lower_bound_n<RanIt, Key, Comp>(a.begin(), 0, a.size(), key, comp);
}

template <class RanIt, class Key>
size_t upper_bound_0(RanIt a, size_t n, const Key& key) {
  return upper_bound_n<RanIt, Key>(a, 0, n, key);
}
template <class RanIt, class Key, class Comp>
size_t upper_bound_0(RanIt a, size_t n, const Key& key, Comp comp) {
  return upper_bound_n<RanIt, Key, Comp>(a, 0, n, key, comp);
}
template <class Container, class Key>
size_t upper_bound_a(const Container& a, const Key& key) {
  typedef typename Container::const_iterator RanIt;
  return upper_bound_n<RanIt, Key>(a.begin(), 0, a.size(), key);
}
template <class Container, class Key, class Comp>
size_t upper_bound_a(const Container& a, const Key& key, Comp comp) {
  typedef typename Container::const_iterator RanIt;
  return upper_bound_n<RanIt, Key, Comp>(a.begin(), 0, a.size(), key, comp);
}

template <class RanIt, class Key>
std::pair<size_t, size_t> equal_range_0(RanIt a, size_t n, const Key& key) {
  return equal_range_n<RanIt, Key>(a, 0, n, key);
}
template <class RanIt, class Key, class Comp>
std::pair<size_t, size_t> equal_range_0(RanIt a, size_t n, const Key& key,
                                        Comp comp) {
  return equal_range_n<RanIt, Key, Comp>(a, 0, n, key, comp);
}
template <class Container, class Key>
std::pair<size_t, size_t> equal_range_a(const Container& a, const Key& key) {
  typedef typename Container::const_iterator RanIt;
  return equal_range_n<RanIt, Key>(a.begin(), 0, a.size(), key);
}
template <class Container, class Key, class Comp>
std::pair<size_t, size_t> equal_range_a(const Container& a, const Key& key,
                                        Comp comp) {
  typedef typename Container::const_iterator RanIt;
  return equal_range_n<RanIt, Key, Comp>(a.begin(), 0, a.size(), key, comp);
}

template <class RanIt, class Key>
bool binary_search_0(RanIt a, size_t n, const Key& key) {
  return binary_search_n<RanIt, Key>(a, 0, n, key);
}
template <class RanIt, class Key, class Comp>
bool binary_search_0(RanIt a, size_t n, const Key& key, Comp comp) {
  return binary_search_n<RanIt, Key, Comp>(a, 0, n, key, comp);
}

template <class Range, class Key>
bool binary_search_a(const Range& a, const Key& key) {
  return binary_search_n(a.begin(), 0, a.size(), key);
}
template <class Range, class Key, class Comp>
bool binary_search_a(const Range& a, const Key& key, Comp comp) {
  return binary_search_n(a.begin(), 0, a.size(), key, comp);
}

template <class RanIt>
void sort_0(RanIt a, size_t n) {
  sort_n<RanIt>(a, 0, n);
}
template <class RanIt, class Comp>
void sort_0(RanIt a, size_t n, Comp comp) {
  sort_n<RanIt, Comp>(a, 0, n, comp);
}

template <class Container>
void sort_a(Container& a) {
  std::sort(std::begin(a), std::end(a));
}
template <class Container, class Comp>
void sort_a(Container& a, Comp comp) {
  std::sort(std::begin(a), std::end(a), comp);
}

template <class RanIt>
void reverse_n(RanIt a, size_t low, size_t upp) {
  std::reverse<RanIt>(a + low, a + upp);
}
template <class RanIt>
void reverse_0(RanIt a, size_t n) {
  std::reverse<RanIt>(a + 0, a + n);
}
template <class Container>
void reverse_a(Container& a) {
  std::reverse(a.begin(), a.end());
}
template <class Container>
void reverse_a(Container& a, size_t low, size_t upp) {
  assert(low <= upp);
  assert(upp <= a.size());
  std::reverse(a.begin() + low, a.begin() + upp);
}

template <class RanIt>
size_t unique_n(RanIt a, size_t low, size_t upp) {
  return std::unique<RanIt>(a + low, a + upp) - a;
}
template <class RanIt>
size_t unique_0(RanIt a, size_t n) {
  return std::unique<RanIt>(a + 0, a + n) - a;
}

template <class Container>
size_t unique_a(Container& a) {
  return std::unique(a.begin(), a.end()) - a.begin();
}
template <class Container, class Equal>
size_t unique_a(Container& a, Equal eq) {
  return std::unique(a.begin(), a.end(), eq) - a.begin();
}
template <class Container>
size_t unique_a(Container& a, size_t low, size_t upp) {
  assert(low <= upp);
  assert(upp <= a.size());
  return std::unique(a.begin() + low, a.begin() + upp) - low - a.begin();
}
template <class Container, class Equal>
size_t unique_a(Container& a, Equal eq, size_t low, size_t upp) {
  assert(low <= upp);
  assert(upp <= a.size());
  return std::unique(a.begin() + low, a.begin() + upp, eq) - low - a.begin();
}
template <class RanIt, class KeyExtractor>
void sort_ex_n(RanIt a, size_t low, size_t upp, KeyExtractor keyEx) {
  assert(low <= upp);
  std::sort(a + low, a + upp, ExtractorLess(keyEx));
}
template <class RanIt, class KeyExtractor, class Comp>
void sort_ex_n(RanIt a, size_t low, size_t upp, KeyExtractor keyEx, Comp cmp) {
  assert(low <= upp);
  std::sort(a + low, a + upp, ExtractorComparator(keyEx, cmp));
}

template <class Container, class KeyExtractor>
void sort_ex_a(Container& a, KeyExtractor keyEx) {
  std::sort(std::begin(a), std::end(a), ExtractorLess(keyEx));
}
template <class Container, class KeyExtractor, class Comp>
void sort_ex_a(Container& a, KeyExtractor keyEx, Comp cmp) {
  std::sort(std::begin(a), std::end(a), ExtractorComparator(keyEx, cmp));
}

}  // namespace terark
#endif