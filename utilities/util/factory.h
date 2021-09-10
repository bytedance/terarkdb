#pragma once

#include "rocksdb/terark_namespace.h"

#ifdef WITH_TERARK_ZIP
#include "terark/util/factory.ipp"
#else

#include <rocksdb/slice.h>

#ifdef WITH_BOOSTLIB
#include <boost/current_function.hpp>
#include <boost/noncopyable.hpp>
#endif
#ifndef BOOST_CURRENT_FUNCTION
#define BOOST_CURRENT_FUNCTION "(unknown)"
#endif

#include <mutex>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>

#include "util/hash.h"
#include "utilities/util/function.hpp"
#include "utilities/util/terark_boost.hpp"

#define BYTEDANCE_TERARK_DLL_EXPORT

namespace terark {
///@note on principle, the factory itself is stateless, but its product
/// can has states, sometimes we need factory of factory, in this case,
/// just let the factory being factoryable:
/// class SomeFactory : public Factoyable<SomeFactory> {
///    ...
/// };
template <class ProductPtr, class... CreatorArgs>
class Factoryable {
 public:
  virtual ~Factoryable();
  static ProductPtr create(TERARKDB_NAMESPACE::Slice name, CreatorArgs...);

  TERARKDB_NAMESPACE::Slice reg_name() const;

#ifdef WITH_BOOSTLIB
  struct AutoReg : boost::noncopyable {
#else
  struct AutoReg : boost::noncopyable {
#endif
    typedef function<ProductPtr(CreatorArgs...)> CreatorFunc;
    AutoReg(TERARKDB_NAMESPACE::Slice name, CreatorFunc creator,
            const std::type_info&);
    ~AutoReg();
    std::string m_name;
    std::type_index m_type_idx;
    struct Impl;
  };
};

///@param VarID var identifier
///@param Name string of factory name
///@param Creator
///@param Class ... can be qulified template such as:
///                     SomeNameSpace::SomeProductPtr<T1, T2, T3>
#define TERARK_FACTORY_REGISTER_IMPL(VarID, Name, Creator, Class, ...) \
  TERARK_PP_IDENTITY(Class, ##__VA_ARGS__)::AutoReg TERARK_PP_CAT(     \
      g_reg_factory_, VarID, __LINE__)(                                \
      Name, Creator, typeid(TERARK_PP_IDENTITY(Class, ##__VA_ARGS__)))

///@param Class can not be template such as SomeProductPtr<T1, T2, T3>,
///             can not be qulified class name such as SomeNameSpace::SomeClass
///@note if Class has some non-var char, such as "-,." ...
///         must use TERARK_FACTORY_REGISTER_IMPL to set an VarID
#define TERARK_FACTORY_REGISTER_EX(Class, Name, Creator) \
  TERARK_FACTORY_REGISTER_IMPL(Class, Name, Creator, Class)

#define TERARK_FACTORY_REGISTER(Class, Creator) \
  TERARK_FACTORY_REGISTER_EX(Class, TERARK_PP_STR(Class), Creator)

}  // namespace terark
namespace terark {
BYTEDANCE_TERARK_DLL_EXPORT bool getEnvBool(const char* envName,
                                            bool Default = false);
BYTEDANCE_TERARK_DLL_EXPORT long getEnvLong(const char* envName,
                                            long Default = false);
BYTEDANCE_TERARK_DLL_EXPORT double getEnvDouble(const char* envName,
                                                double Default);
}  // namespace terark
// #endif  // __UTILITIES_UTIL_FACTORY_HPP__

namespace terark {

template <class ProductPtr, class... CreatorArgs>
struct Factoryable<ProductPtr, CreatorArgs...>::AutoReg::Impl {
#if 1
  struct FakeMutex {
    void lock() {}
    void unlock() {}
  };
  using MaybeMutex = FakeMutex;
#else
  using MaybeMutex = std::mutex;
#endif
  using NameToFuncMap = std::unordered_map<std::string, CreatorFunc>;
  using TypeToNameMap = std::unordered_map<std::type_index, std::string>;

  NameToFuncMap func_map;
  TypeToNameMap type_map;
  MaybeMutex mtx;

  static Impl& s_singleton() {
    static Impl imp;
    return imp;
  }
};

//#define TERARK_FACTORY_WARN_ON_DUP_NAME

template <class ProductPtr, class... CreatorArgs>
Factoryable<ProductPtr, CreatorArgs...>::AutoReg::AutoReg(
    TERARKDB_NAMESPACE::Slice name, CreatorFunc creator,
    const std::type_info& ti)
    : m_name(name.ToString()), m_type_idx(ti) {
  auto& imp = Impl::s_singleton();
  auto& func_map = imp.func_map;
  imp.mtx.lock();
  if (!func_map.emplace(name.ToString(), creator).second) {
    fprintf(stderr, "FATAL: %s: duplicate name = %.*s\n", __FUNCTION__,
            int(name.size()), name.data());
    abort();
  }
  if (!imp.type_map.emplace(ti, name.ToString()).second) {
#if defined(TERARK_FACTORY_WARN_ON_DUP_NAME)
    TERARKDB_NAMESPACE::Slice oldname = imp.type_map.val(ib.first);
    fprintf(stderr,
            "WARN: %s: dup name: {old=\"%.*s\", new=\"%.*s\"} "
            "for type: %s, new name ignored\n",
            __FUNCTION__, oldname.size(), oldname.data(), name.size(),
            name.data(), ti.name());
#endif
  }
  imp.mtx.unlock();
}

template <class ProductPtr, class... CreatorArgs>
Factoryable<ProductPtr, CreatorArgs...>::AutoReg::~AutoReg() {
  auto& imp = Impl::s_singleton();
  imp.mtx.lock();
  size_t cnt1 = imp.func_map.erase(m_name);
  size_t cnt2 = imp.type_map.erase(m_type_idx);
  if (0 == cnt1) {
    fprintf(stderr, "FATAL: %s: name = %.*s to creator not found\n",
            __FUNCTION__, int(m_name.size()), m_name.data());
    abort();
  }
  if (0 == cnt2) {
#if defined(TERARK_FACTORY_WARN_ON_DUP_NAME)
    fprintf(stderr, "WARN: %s: type = %s to name not found, ignored\n",
            __FUNCTION__, m_type_idx.name());
#endif
  }
  imp.mtx.unlock();
}

template <class ProductPtr, class... CreatorArgs>
ProductPtr Factoryable<ProductPtr, CreatorArgs...>::create(
    TERARKDB_NAMESPACE::Slice name, CreatorArgs... args) {
  auto& imp = AutoReg::Impl::s_singleton();
  auto& func_map = imp.func_map;
  imp.mtx.lock();
  auto i = func_map.find(name.ToString());
  if (func_map.end() != i) {
    auto& creator = i->second;
    imp.mtx.unlock();
    return (creator)(args...);
  } else {
    imp.mtx.unlock();
    return nullptr;
  }
}

template <class ProductPtr, class... CreatorArgs>
TERARKDB_NAMESPACE::Slice Factoryable<ProductPtr, CreatorArgs...>::reg_name()
    const {
  auto& imp = AutoReg::Impl::s_singleton();
  TERARKDB_NAMESPACE::Slice name;
  imp.mtx.lock();
  auto i = imp.type_map.find(std::type_index(typeid(*this)));
  if (imp.type_map.end() != i) {
    name = i->second;
  }
  imp.mtx.unlock();
  return name;
}

template <class ProductPtr, class... CreatorArgs>
Factoryable<ProductPtr, CreatorArgs...>::~Factoryable() {}

}  // namespace terark

/// ---- user land ----

///@param ProductPtr allowing template product, such as
/// TERARK_FACTORY_INSTANTIATE(SomeProduct<T1, T2, T3>, CreatorArg1...)
///@note this macro must be called in namespace terark
#define TERARK_FACTORY_INSTANTIATE(ProductPtr, ...) \
  template class Factoryable<ProductPtr, ##__VA_ARGS__>

///@note this macro must be called in global namespace
#define TERARK_FACTORY_INSTANTIATE_GNS(ProductPtr, ...)  \
  namespace terark {                                     \
  TERARK_FACTORY_INSTANTIATE(ProductPtr, ##__VA_ARGS__); \
  }

#endif