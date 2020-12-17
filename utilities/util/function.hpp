#pragma once
#ifdef WITH_TERARK_ZIP
#include "terark/util/function.hpp"
#include "terark/valvec.hpp"
#else

#include <functional>
#include "utilities/util/config.hpp"
#include "utilities/util/preproc.hpp"
#include "utilities/util/terark_boost.hpp"
namespace terark {

using std::bind;
using std::cref;
using std::function;
using std::ref;
using std::reference_wrapper;
using std::remove_reference;

template <class FuncProto>
class tfunc : public function<FuncProto> {
  typedef function<FuncProto> super;

 public:
  using super::super;
  template <class Functor>
  tfunc(const Functor* f) : super(ref(*f)) {}
};

template <class Functor, class... ArgList>
auto bind(Functor* f, ArgList&&... args)
    -> decltype(bind(ref(*f), std::forward<ArgList>(args)...)) {
  return bind(ref(*f), std::forward<ArgList>(args)...);
}

template <class Func>
class OnScopeExit {
  const Func& on_exit;

 public:
  OnScopeExit(const Func& f) : on_exit(f) {}
  ~OnScopeExit() { on_exit(); }
};
#define TERARK_SCOPE_EXIT(...)                                           \
  auto TERARK_PP_CAT2(func_on_exit_, __LINE__) = [&]() { __VA_ARGS__; }; \
  terark::OnScopeExit<decltype(TERARK_PP_CAT2(func_on_exit_, __LINE__))> \
      TERARK_PP_CAT2(call_on_exit_,                                      \
                     __LINE__)(TERARK_PP_CAT2(func_on_exit_, __LINE__))

template <class R, class... Args>
using c_callback_fun_t = R (*)(void*, Args...);

template <class Lambda>
struct c_callback_t {
  template <class R, class... Args>
  static R invoke(void* vlamb, Args... args) {
    return (*(Lambda*)vlamb)(std::forward<Args>(args)...);
  }

  template <class R, class... Args>
  operator c_callback_fun_t<R, Args...>() const {
    return &c_callback_t::invoke<R, Args...>;
  }
};
template <class Lambda>
c_callback_t<Lambda> c_callback(Lambda&) {
  return c_callback_t<Lambda>();
}

template <class MemFuncType, MemFuncType MemFunc>
struct mf_callback_t {
  template <class R, class Obj, class... Args>
  static R invoke(void* self, Args... args) {
    return (((Obj*)self)->*MemFunc)(std::forward<Args>(args)...);
  }
  template <class R, class... Args>
  operator c_callback_fun_t<R, Args...>() const {
    return &mf_callback_t::invoke<R, Args...>;
  }
};

// do not need to pack <This, MemFunc> as a struct
#define TERARK_MEM_FUNC(MemFunc) mf_callback_t<decltype(MemFunc), MemFunc>()

///@param lambda lambda obj
///@note  this yield two args
#define TERARK_C_CALLBACK(lambda) terark::c_callback(lambda), &lambda

//--------------------------------------------------------------------
// User/Application defined MemPool
class TERARK_DLL_EXPORT UserMemPool : boost::noncopyable {
  UserMemPool();

 public:
  virtual ~UserMemPool();
  virtual void* alloc(size_t);
  virtual void* realloc(void*, size_t);
  virtual void sfree(void*, size_t);
  static UserMemPool* SysMemPool();
};

#define TERARK_COMPARATOR_OP(Name, expr)            \
  struct Name {                                     \
    template <class T>                              \
    bool operator()(const T& x, const T& y) const { \
      return expr;                                  \
    }                                               \
  }
TERARK_COMPARATOR_OP(CmpLT, x < y);
TERARK_COMPARATOR_OP(CmpGT, y < x);
TERARK_COMPARATOR_OP(CmpLE, !(y < x));
TERARK_COMPARATOR_OP(CmpGE, !(x < y));
TERARK_COMPARATOR_OP(CmpEQ, x == y);
TERARK_COMPARATOR_OP(CmpNE, !(x == y));

struct cmp_placeholder {};
constexpr cmp_placeholder cmp;  // gcc warns for unused static var

template <class Pred>
struct NotPredT {
  template <class T>
  bool operator()(const T& x) const {
    return !pred(x);
  }
  Pred pred;
};

///{@
///@arg x is the free  arg
///@arg y is the bound arg
#define TERARK_BINDER_CMP_OP(BinderName, expr)         \
  template <class T>                                   \
  struct BinderName {                                  \
    const T y;                                         \
    template <class U>                                 \
    BinderName(U&& y1) : y(std::forward<U>(y1)) {}     \
    bool operator()(const T& x) const { return expr; } \
  };                                                   \
  template <class T>                                   \
  struct BinderName<T*> {                              \
    const T* y;                                        \
    BinderName(const T* y1) : y(y1) {}                 \
    bool operator()(const T* x) const { return expr; } \
  };                                                   \
  template <class T>                                   \
  struct BinderName<reference_wrapper<const T> > {     \
    const T& y;                                        \
    BinderName(const T& y1) : y(y1) {}                 \
    bool operator()(const T& x) const { return expr; } \
  }
TERARK_BINDER_CMP_OP(BinderLT, x < y);
TERARK_BINDER_CMP_OP(BinderGT, y < x);
TERARK_BINDER_CMP_OP(BinderLE, !(y < x));
TERARK_BINDER_CMP_OP(BinderGE, !(x < y));
TERARK_BINDER_CMP_OP(BinderEQ, x == y);
TERARK_BINDER_CMP_OP(BinderNE, !(x == y));
///@}

template <class KeyExtractor>
struct ExtractorLessT {
  KeyExtractor ex;
  template <class T>
  bool operator()(const T& x, const T& y) const {
    return ex(x) < ex(y);
  }
};
template <class KeyExtractor>
ExtractorLessT<KeyExtractor> ExtractorLess(KeyExtractor ex) {
  return ExtractorLessT<KeyExtractor>{ex};
}

template <class KeyExtractor>
struct ExtractorGreaterT {
  KeyExtractor ex;
  template <class T>
  bool operator()(const T& x, const T& y) const {
    return ex(y) < ex(x);
  }
};
template <class KeyExtractor>
ExtractorGreaterT<KeyExtractor> ExtractorGreater(KeyExtractor ex) {
  return ExtractorGreaterT<KeyExtractor>{ex};
}

template <class KeyExtractor>
struct ExtractorEqualT {
  KeyExtractor ex;
  template <class T>
  bool operator()(const T& x, const T& y) const {
    return ex(x) == ex(y);
  }
};
template <class KeyExtractor>
ExtractorEqualT<KeyExtractor> ExtractorEqual(KeyExtractor ex) {
  return ExtractorEqualT<KeyExtractor>{ex};
}

template <class KeyExtractor, class KeyComparator>
struct ExtractorComparatorT {
  template <class T>
  bool operator()(const T& x, const T& y) const {
    return keyCmp(keyEx(x), keyEx(y));
  }
  KeyExtractor keyEx;
  KeyComparator keyCmp;
};
template <class KeyExtractor, class Comparator>
ExtractorComparatorT<KeyExtractor, Comparator> ExtractorComparator(
    KeyExtractor ex, Comparator cmp) {
  return ExtractorComparatorT<KeyExtractor, Comparator>{ex, cmp};
}

template <class Extractor1, class Extractor2>
struct CombineExtractor {
  Extractor1 ex1;
  Extractor2 ex2;
  template <class T>
  auto operator()(const T& x) const -> decltype(ex2(ex1(x))) {
    return ex2(ex1(x));
  }
};
template <class Extractor1>
struct CombinableExtractorT {
  Extractor1 ex1;

  NotPredT<CombinableExtractorT> operator!() const {
    return NotPredT<CombinableExtractorT>{*this};
  }

  ///@{
  /// operator+ as combine operator
  template <class Extractor2>
  CombineExtractor<Extractor1, Extractor2> operator+(Extractor2&& ex2) const {
    return CombineExtractor<Extractor1, Extractor2>{
        ex1, std::forward<Extractor2>(ex2)};
  }
  ///@}

  ///@{
  /// operator| combine a comparator: less, greator, equal...
  template <class Comparator>
  ExtractorComparatorT<Extractor1, Comparator> operator|(
      Comparator&& cmp) const {
    return ExtractorComparator(ex1, std::forward<Comparator>(cmp));
  }
  ///@}

#define TERARK_CMP_OP(Name, op)                                               \
  ExtractorComparatorT<Extractor1, Name> operator op(cmp_placeholder) const { \
    return ExtractorComparator(ex1, Name());                                  \
  }

  TERARK_CMP_OP(CmpLT, <)
  TERARK_CMP_OP(CmpGT, >)
  TERARK_CMP_OP(CmpLE, <=)
  TERARK_CMP_OP(CmpGE, >=)
  TERARK_CMP_OP(CmpEQ, ==)
  TERARK_CMP_OP(CmpNE, !=)

#define TERARK_COMBINE_BIND_OP(Name, op)                                     \
  template <class T>                                                         \
  CombineExtractor<Extractor1, Name<typename remove_reference<T>::type> >    \
  operator op(T&& y) const {                                                 \
    return CombineExtractor<Extractor1,                                      \
                            Name<typename remove_reference<T>::type> >{      \
        ex1, {std::forward<T>(y)}};                                          \
  }                                                                          \
                                                                             \
  template <class T>                                                         \
  CombineExtractor<Extractor1, Name<reference_wrapper<const T> > >           \
  operator op(reference_wrapper<const T> y) const {                          \
    return CombineExtractor<Extractor1, Name<reference_wrapper<const T> > >{ \
        ex1, {y.get()}};                                                     \
  }

  ///@{
  /// operators: <, >, <=, >=, ==, !=
  /// use bound operator as Extractor, Extractor is a transformer in this case
  TERARK_COMBINE_BIND_OP(BinderLT, <)
  TERARK_COMBINE_BIND_OP(BinderGT, >)
  TERARK_COMBINE_BIND_OP(BinderLE, <=)
  TERARK_COMBINE_BIND_OP(BinderGE, >=)
  TERARK_COMBINE_BIND_OP(BinderEQ, ==)
  TERARK_COMBINE_BIND_OP(BinderNE, !=)
  ///@}

  /// forward the extractor
  template <class T>
  auto operator()(const T& x) const -> decltype(ex1(x)) {
    return ex1(x);
  }
};
template <class Extractor1>
CombinableExtractorT<Extractor1> CombinableExtractor(Extractor1&& ex1) {
  return CombinableExtractorT<Extractor1>{std::forward<Extractor1>(ex1)};
}

///@param __VA_ARGS__ can be ' .template some_member_func<1,2,3>()'
///                       or '->template some_member_func<1,2,3>()'
///@note '.' or '->' before field is required
///@note TERARK_GET() is identity operator
#define TERARK_GET(...)                  \
  terark::CombinableExtractor( \
      [](const auto& x) -> decltype(auto) { return (x __VA_ARGS__); })

#define TERARK_FIELD_O_0() x
#define TERARK_FIELD_P_0() x
#define TERARK_FIELD_O_1(field) x.field
#define TERARK_FIELD_P_1(field) x->field

///@param __VA_ARGS__ can NOT be 'template some_member_func<1,2,3>()'
///@note decltype(auto) is required, () on return is required
///@note TERARK_FIELD() is identity operator
///@note '.' or '->' can not before field name
#define TERARK_FIELD(...)                                                  \
  [](const auto& x) -> decltype(auto) {                                    \
    return (TERARK_PP_VA_NAME(TERARK_FIELD_O_, __VA_ARGS__)(__VA_ARGS__)); \
  }

#define TERARK_FIELD_P(...)                                                \
  [](const auto& x) -> decltype(auto) {                                    \
    return (TERARK_PP_VA_NAME(TERARK_FIELD_P_, __VA_ARGS__)(__VA_ARGS__)); \
  }

///@{
///@param d '.' or '->'
///@param f field
///@param o order/operator, '<' or '>'
#define TERARK_CMP1(d, f, o)      \
  if (x d f o y d f) return true; \
  if (y d f o x d f) return false;

#define TERARK_CMP_O_2(f, o) return x.f o y.f;
#define TERARK_CMP_O_4(f, o, ...) \
  TERARK_CMP1(., f, o) TERARK_CMP_O_2(__VA_ARGS__)
#define TERARK_CMP_O_6(f, o, ...) \
  TERARK_CMP1(., f, o) TERARK_CMP_O_4(__VA_ARGS__)
#define TERARK_CMP_O_8(f, o, ...) \
  TERARK_CMP1(., f, o) TERARK_CMP_O_6(__VA_ARGS__)
#define TERARK_CMP_O_a(f, o, ...) \
  TERARK_CMP1(., f, o) TERARK_CMP_O_8(__VA_ARGS__)
#define TERARK_CMP_O_c(f, o, ...) \
  TERARK_CMP1(., f, o) TERARK_CMP_O_a(__VA_ARGS__)
#define TERARK_CMP_O_e(f, o, ...) \
  TERARK_CMP1(., f, o) TERARK_CMP_O_c(__VA_ARGS__)
#define TERARK_CMP_O_g(f, o, ...) \
  TERARK_CMP1(., f, o) TERARK_CMP_O_e(__VA_ARGS__)
#define TERARK_CMP_O_i(f, o, ...) \
  TERARK_CMP1(., f, o) TERARK_CMP_O_g(__VA_ARGS__)
#define TERARK_CMP_O_k(f, o, ...) \
  TERARK_CMP1(., f, o) TERARK_CMP_O_i(__VA_ARGS__)
#define TERARK_CMP_O_m(f, o, ...) \
  TERARK_CMP1(., f, o) TERARK_CMP_O_k(__VA_ARGS__)
#define TERARK_CMP_O_o(f, o, ...) \
  TERARK_CMP1(., f, o) TERARK_CMP_O_m(__VA_ARGS__)
#define TERARK_CMP_O_q(f, o, ...) \
  TERARK_CMP1(., f, o) TERARK_CMP_O_o(__VA_ARGS__)
#define TERARK_CMP_O_s(f, o, ...) \
  TERARK_CMP1(., f, o) TERARK_CMP_O_q(__VA_ARGS__)
#define TERARK_CMP_O_u(f, o, ...) \
  TERARK_CMP1(., f, o) TERARK_CMP_O_s(__VA_ARGS__)
#define TERARK_CMP_O_w(f, o, ...) \
  TERARK_CMP1(., f, o) TERARK_CMP_O_u(__VA_ARGS__)
#define TERARK_CMP_O_y(f, o, ...) \
  TERARK_CMP1(., f, o) TERARK_CMP_O_w(__VA_ARGS__)
#define TERARK_CMP_O_A(f, o, ...) \
  TERARK_CMP1(., f, o) TERARK_CMP_O_y(__VA_ARGS__)
#define TERARK_CMP_O_C(f, o, ...) \
  TERARK_CMP1(., f, o) TERARK_CMP_O_A(__VA_ARGS__)
#define TERARK_CMP_O_E(f, o, ...) \
  TERARK_CMP1(., f, o) TERARK_CMP_O_C(__VA_ARGS__)

#define TERARK_CMP_P_2(f, o) return x->f o y->f;
#define TERARK_CMP_P_4(f, o, ...) \
  TERARK_CMP1(->, f, o) TERARK_CMP_P_2(__VA_ARGS__)
#define TERARK_CMP_P_6(f, o, ...) \
  TERARK_CMP1(->, f, o) TERARK_CMP_P_4(__VA_ARGS__)
#define TERARK_CMP_P_8(f, o, ...) \
  TERARK_CMP1(->, f, o) TERARK_CMP_P_6(__VA_ARGS__)
#define TERARK_CMP_P_a(f, o, ...) \
  TERARK_CMP1(->, f, o) TERARK_CMP_P_8(__VA_ARGS__)
#define TERARK_CMP_P_c(f, o, ...) \
  TERARK_CMP1(->, f, o) TERARK_CMP_P_a(__VA_ARGS__)
#define TERARK_CMP_P_e(f, o, ...) \
  TERARK_CMP1(->, f, o) TERARK_CMP_P_c(__VA_ARGS__)
#define TERARK_CMP_P_g(f, o, ...) \
  TERARK_CMP1(->, f, o) TERARK_CMP_P_e(__VA_ARGS__)
#define TERARK_CMP_P_i(f, o, ...) \
  TERARK_CMP1(->, f, o) TERARK_CMP_P_g(__VA_ARGS__)
#define TERARK_CMP_P_k(f, o, ...) \
  TERARK_CMP1(->, f, o) TERARK_CMP_P_i(__VA_ARGS__)
#define TERARK_CMP_P_m(f, o, ...) \
  TERARK_CMP1(->, f, o) TERARK_CMP_P_k(__VA_ARGS__)
#define TERARK_CMP_P_o(f, o, ...) \
  TERARK_CMP1(->, f, o) TERARK_CMP_P_m(__VA_ARGS__)
#define TERARK_CMP_P_q(f, o, ...) \
  TERARK_CMP1(->, f, o) TERARK_CMP_P_o(__VA_ARGS__)
#define TERARK_CMP_P_s(f, o, ...) \
  TERARK_CMP1(->, f, o) TERARK_CMP_P_q(__VA_ARGS__)
#define TERARK_CMP_P_u(f, o, ...) \
  TERARK_CMP1(->, f, o) TERARK_CMP_P_s(__VA_ARGS__)
#define TERARK_CMP_P_w(f, o, ...) \
  TERARK_CMP1(->, f, o) TERARK_CMP_P_u(__VA_ARGS__)
#define TERARK_CMP_P_y(f, o, ...) \
  TERARK_CMP1(->, f, o) TERARK_CMP_P_w(__VA_ARGS__)
#define TERARK_CMP_P_A(f, o, ...) \
  TERARK_CMP1(->, f, o) TERARK_CMP_P_y(__VA_ARGS__)
#define TERARK_CMP_P_C(f, o, ...) \
  TERARK_CMP1(->, f, o) TERARK_CMP_P_A(__VA_ARGS__)
#define TERARK_CMP_P_E(f, o, ...) \
  TERARK_CMP1(->, f, o) TERARK_CMP_P_C(__VA_ARGS__)
///@}

///@param __VA_ARGS__ at least 1 field
///@note max support 20 fields, sample usage: TERARK_CMP(f1,>,f2,<,f3,<)
#define TERARK_CMP(...)                                        \
  [](const auto& x, const auto& y) -> bool {                   \
    TERARK_PP_VA_NAME(TERARK_CMP_O_, __VA_ARGS__)(__VA_ARGS__) \
  }

#define TERARK_CMP_P(...)                                      \
  [](const auto& x, const auto& y) -> bool {                   \
    TERARK_PP_VA_NAME(TERARK_CMP_P_, __VA_ARGS__)(__VA_ARGS__) \
  }

#define TERARK_EQUAL_MAP(c, f) \
  if (!(x f == y f)) return false;
#define TERARK_EQUAL_IMP(...)                        \
  [](const auto& x, const auto& y) {                 \
    TERARK_PP_MAP(TERARK_EQUAL_MAP, ~, __VA_ARGS__); \
    return true;                                     \
  }

///@param __VA_ARGS__ can not be empty
#define TERARK_EQUAL(...) \
  TERARK_EQUAL_IMP(TERARK_PP_MAP(TERARK_PP_PREPEND, ., __VA_ARGS__))

///@param __VA_ARGS__ can not be empty
#define TERARK_EQUAL_P(...) \
  TERARK_EQUAL_IMP(TERARK_PP_MAP(TERARK_PP_PREPEND, ->, __VA_ARGS__))

}  // namespace terark

// using terark::cmp;
#endif