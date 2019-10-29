//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/lazy_buffer.h"
#include <string>
#include "util/random.h"
#include "util/testharness.h"

namespace rocksdb {

class LazyBufferTest : public testing::Test {};

TEST_F(LazyBufferTest, Basic) {

  LazyBuffer empty;
  ASSERT_OK(empty.fetch());
  ASSERT_EQ(empty.slice(), "");

  LazyBuffer abc("abc");
  ASSERT_OK(abc.fetch());
  ASSERT_EQ(abc.slice(), "abc");

  LazyBuffer abc2(std::move(abc));
  ASSERT_OK(abc2.fetch());
  ASSERT_EQ(abc2.slice(), "abc");

  LazyBuffer abc3 = std::move(abc2);
  ASSERT_OK(abc3.fetch());
  ASSERT_EQ(abc3.slice(), "abc");

}

TEST_F(LazyBufferTest, LightColtroller) {

  LazyBuffer buffer;
  ASSERT_EQ(buffer.TEST_state(), LazyBufferState::light_state());

  std::string string('a', 33);
  buffer.reset(string, false);
  ASSERT_EQ(buffer.TEST_state(), LazyBufferState::light_state());

  for (auto &c : string) { c = 'b'; };
  ASSERT_OK(buffer.fetch());
  ASSERT_EQ(buffer.slice(), string);

  buffer.reset(Slice(string.data(), 32), true);
  ASSERT_EQ(buffer.TEST_state(), LazyBufferState::light_state());

  buffer.reset(string, true);
  ASSERT_EQ(buffer.TEST_state(), LazyBufferState::buffer_state());

  string = "abc";
  buffer.reset(string);
  for (auto &c : string) { c = 'a'; };
  ASSERT_OK(buffer.fetch());
  ASSERT_EQ(buffer.slice(), "aaa");

  buffer.reset(string, true);
  for (auto &c : string) { c = 'b'; };
  ASSERT_OK(buffer.fetch());
  ASSERT_EQ(buffer.slice(), "aaa");

}


TEST_F(LazyBufferTest, BufferColtroller) {

  auto test = [](LazyBuffer& b) {
    auto builder = b.get_builder();
    ASSERT_FALSE(builder->resize(size_t(-1)));
    ASSERT_NOK(builder->fetch());
    ASSERT_TRUE(builder->resize(4));
    ASSERT_EQ(b.slice(), Slice("\0\0\0\0", 4));
    ::memcpy(builder->data(), "abcd", 4);
    ASSERT_TRUE(builder->resize(3));
    ASSERT_EQ(b.slice(), "abc");
  };
  LazyBuffer buffer(size_t(-1));
  ASSERT_EQ(buffer.TEST_state(), LazyBufferState::buffer_state());
  ASSERT_NOK(buffer.fetch());
  test(buffer);

  buffer.clear();
  ASSERT_EQ(buffer.TEST_state(),
      LazyBufferState::buffer_state());
  test(buffer);

  buffer = LazyBuffer("123");
  test(buffer);

  std::string string;
  LazyBufferCustomizeBuffer cb = {
      &string,
      [](void* ptr, size_t size)->void* {
        auto string_ptr = (std::string*)ptr;
        try {
          string_ptr->resize(size);
        } catch (...) {
          return nullptr;
        }
        return (void*)string_ptr->data();
      }
  };
  buffer.reset(cb);
  test(buffer);

}

TEST_F(LazyBufferTest, StringColtroller) {

  std::string string = "abc";
  LazyBuffer buffer(&string);
  ASSERT_EQ(buffer.TEST_state(), LazyBufferState::string_state());
  ASSERT_EQ(buffer.slice(), "abc");

  buffer.reset("aaa", true);
  buffer.trans_to_string()->append("bbb");
  ASSERT_OK(buffer.fetch());
  ASSERT_EQ(buffer.slice(), "aaabbb");
  ASSERT_EQ(buffer.TEST_context()->data[1], 0);

  buffer.get_builder()->resize(buffer.size() - 1);
  ASSERT_EQ(buffer.TEST_state(), LazyBufferState::string_state());
  ASSERT_OK(std::move(buffer).dump(&string));
  ASSERT_EQ(string, "aaabb");

  buffer.reset(string);
  buffer.trans_to_string()->resize(string.size() - 1);
  ASSERT_OK(std::move(buffer).dump(&string));
  ASSERT_EQ(buffer.TEST_context()->data[1], 1);
  ASSERT_EQ(string, "aaab");

  buffer.reset(string);
  buffer.get_builder()->resize(string.size() - 1);
  ASSERT_OK(std::move(buffer).dump(&string));
  ASSERT_EQ(string, "aaa");

}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
