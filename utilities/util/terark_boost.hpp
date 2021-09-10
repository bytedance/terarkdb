#pragma once
#ifdef WITH_BOOSTLIB
#include <boost/noncopyable.hpp>
#elif __has_include("boost/noncopyable.hpp")
#include <boost/noncopyable.hpp>
#else
namespace boost {

struct base_token {};
class noncopyable : base_token {
 protected:
  noncopyable() = default;
  ~noncopyable() = default;

 private:
  noncopyable(const noncopyable&);
  const noncopyable& operator=(const noncopyable&);
};

}  // namespace boost
#endif
