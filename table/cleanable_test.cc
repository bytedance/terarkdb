//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <functional>

#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/terark_namespace.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace TERARKDB_NAMESPACE {

class CleanableTest : public testing::Test {};

// Use this to keep track of the cleanups that were actually performed
void Multiplier(void* arg1, void* arg2) {
  int* res = reinterpret_cast<int*>(arg1);
  int* num = reinterpret_cast<int*>(arg2);
  *res *= *num;
}

// the first Cleanup is on stack and the rest on heap, so test with both cases
TEST_F(CleanableTest, Register) {
  int n2 = 2, n3 = 3;
  int res = 1;
  { Cleanable c1; }
  // ~Cleanable
  ASSERT_EQ(1, res);

  res = 1;
  {
    Cleanable c1;
    c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
  }
  // ~Cleanable
  ASSERT_EQ(2, res);

  res = 1;
  {
    Cleanable c1;
    c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
    c1.RegisterCleanup(Multiplier, &res, &n3);  // res = 2 * 3;
  }
  // ~Cleanable
  ASSERT_EQ(6, res);

  // Test the Reset does cleanup
  res = 1;
  {
    Cleanable c1;
    c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
    c1.RegisterCleanup(Multiplier, &res, &n3);  // res = 2 * 3;
    c1.Reset();
    ASSERT_EQ(6, res);
  }
  // ~Cleanable
  ASSERT_EQ(6, res);

  // Test Clenable is usable after Reset
  res = 1;
  {
    Cleanable c1;
    c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
    c1.Reset();
    ASSERT_EQ(2, res);
    c1.RegisterCleanup(Multiplier, &res, &n3);  // res = 2 * 3;
  }
  // ~Cleanable
  ASSERT_EQ(6, res);
}

// the first Cleanup is on stack and the rest on heap,
// so test all the combinations of them
TEST_F(CleanableTest, Delegation) {
  int n2 = 2, n3 = 3, n5 = 5, n7 = 7;
  int res = 1;
  {
    Cleanable c2;
    {
      Cleanable c1;
      c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
      c1.DelegateCleanupsTo(&c2);
    }
    // ~Cleanable
    ASSERT_EQ(1, res);
  }
  // ~Cleanable
  ASSERT_EQ(2, res);

  res = 1;
  {
    Cleanable c2;
    {
      Cleanable c1;
      c1.DelegateCleanupsTo(&c2);
    }
    // ~Cleanable
    ASSERT_EQ(1, res);
  }
  // ~Cleanable
  ASSERT_EQ(1, res);

  res = 1;
  {
    Cleanable c2;
    {
      Cleanable c1;
      c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
      c1.RegisterCleanup(Multiplier, &res, &n3);  // res = 2 * 3;
      c1.DelegateCleanupsTo(&c2);
    }
    // ~Cleanable
    ASSERT_EQ(1, res);
  }
  // ~Cleanable
  ASSERT_EQ(6, res);

  res = 1;
  {
    Cleanable c2;
    c2.RegisterCleanup(Multiplier, &res, &n5);  // res = 5;
    {
      Cleanable c1;
      c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
      c1.RegisterCleanup(Multiplier, &res, &n3);  // res = 2 * 3;
      c1.DelegateCleanupsTo(&c2);                 // res = 2 * 3 * 5;
    }
    // ~Cleanable
    ASSERT_EQ(1, res);
  }
  // ~Cleanable
  ASSERT_EQ(30, res);

  res = 1;
  {
    Cleanable c2;
    c2.RegisterCleanup(Multiplier, &res, &n5);  // res = 5;
    c2.RegisterCleanup(Multiplier, &res, &n7);  // res = 5 * 7;
    {
      Cleanable c1;
      c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
      c1.RegisterCleanup(Multiplier, &res, &n3);  // res = 2 * 3;
      c1.DelegateCleanupsTo(&c2);                 // res = 2 * 3 * 5 * 7;
    }
    // ~Cleanable
    ASSERT_EQ(1, res);
  }
  // ~Cleanable
  ASSERT_EQ(210, res);

  res = 1;
  {
    Cleanable c2;
    c2.RegisterCleanup(Multiplier, &res, &n5);  // res = 5;
    c2.RegisterCleanup(Multiplier, &res, &n7);  // res = 5 * 7;
    {
      Cleanable c1;
      c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
      c1.DelegateCleanupsTo(&c2);                 // res = 2 * 5 * 7;
    }
    // ~Cleanable
    ASSERT_EQ(1, res);
  }
  // ~Cleanable
  ASSERT_EQ(70, res);

  res = 1;
  {
    Cleanable c2;
    c2.RegisterCleanup(Multiplier, &res, &n5);  // res = 5;
    c2.RegisterCleanup(Multiplier, &res, &n7);  // res = 5 * 7;
    {
      Cleanable c1;
      c1.DelegateCleanupsTo(&c2);  // res = 5 * 7;
    }
    // ~Cleanable
    ASSERT_EQ(1, res);
  }
  // ~Cleanable
  ASSERT_EQ(35, res);

  res = 1;
  {
    Cleanable c2;
    c2.RegisterCleanup(Multiplier, &res, &n5);  // res = 5;
    {
      Cleanable c1;
      c1.DelegateCleanupsTo(&c2);  // res = 5;
    }
    // ~Cleanable
    ASSERT_EQ(1, res);
  }
  // ~Cleanable
  ASSERT_EQ(5, res);
}

// Putting the LazyBuffer tests here due to similarity to Cleanable tests
TEST_F(CleanableTest, LazyBuffer) {
  int n2 = 2;
  int res = 1;
  const std::string const_str = "123";

  {
    res = 1;
    LazyBuffer value;
    Slice slice(const_str);
    value.reset(slice, Cleanable(Multiplier, &res, &n2));
    std::string str;
    str.assign(value.data(), value.size());
    ASSERT_EQ(const_str, str);
  }
  // ~Cleanable
  ASSERT_EQ(2, res);

  {
    res = 1;
    LazyBuffer value;
    Slice slice(const_str);
    {
      Cleanable c1;
      c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
      value.reset(slice, std::move(c1));
    }
    // ~Cleanable
    ASSERT_EQ(1, res);  // cleanups must have be delegated to value
    std::string str;
    str.assign(value.data(), value.size());
    ASSERT_EQ(const_str, str);
  }
  // ~Cleanable
  ASSERT_EQ(2, res);

  {
    LazyBuffer value;
    Slice slice(const_str);
    value.reset(slice, true);
    std::string str;
    str.assign(value.data(), value.size());
    ASSERT_EQ(const_str, str);
  }

  {
    LazyBuffer value;
    std::string* self_str_ptr = value.trans_to_string();
    self_str_ptr->assign(const_str);
    ASSERT_OK(value.fetch());
    std::string str;
    str.assign(value.data(), value.size());
    ASSERT_EQ(const_str, str);
  }
}

}  // namespace TERARKDB_NAMESPACE

int main(int argc, char** argv) {
  TERARKDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
