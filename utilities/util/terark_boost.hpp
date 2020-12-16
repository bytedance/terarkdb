#pragma once
namespace terark_boost {

struct base_token {};
class noncopyable : base_token {
 protected:
  noncopyable() = default;
  ~noncopyable() = default;

 private:
  noncopyable(const noncopyable&);
  const noncopyable& operator=(const noncopyable&);
};

}  // namespace terark_boost