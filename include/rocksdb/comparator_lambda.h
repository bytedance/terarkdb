#pragma once

namespace rocksdb {

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

}