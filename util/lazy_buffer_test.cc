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

TEST_F(LazyBufferTest, LightState) {

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


TEST_F(LazyBufferTest, BufferState) {

  auto test = [](LazyBuffer& b) {

    auto builder = b.get_builder();
    ASSERT_TRUE(b.valid());
    ASSERT_FALSE(builder->resize(size_t(-1)));
    ASSERT_NOK(builder->fetch());
    ASSERT_FALSE(builder->resize(3));

    ASSERT_TRUE(builder->resize(0));
    ASSERT_TRUE(builder->resize(3));
    ::memcpy(builder->data(), "LOL", 3);
    ASSERT_EQ(b.slice(), "LOL");

    ASSERT_TRUE(builder->resize(4));
    ASSERT_EQ(b.slice(), Slice("LOL\0", 4));

    b.reset(Status::BadAlloc());
    builder = b.get_builder();
    ASSERT_FALSE(builder->resize(1));
    ASSERT_TRUE(builder->resize(0));
    ASSERT_TRUE(b.empty());

    b.reset(Status::BadAlloc());
    b.reset("", true);
    ASSERT_TRUE(b.empty());

  };
  LazyBuffer buffer(size_t(-1));
  ASSERT_EQ(buffer.TEST_state(), LazyBufferState::buffer_state());
  ASSERT_NOK(buffer.fetch());
  ASSERT_NOK(buffer.fetch()); // test fetch twice
  buffer.clear();
  test(buffer);

  buffer.clear();
  ASSERT_EQ(buffer.TEST_state(), LazyBufferState::buffer_state());
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
  ASSERT_TRUE(string.empty());

  buffer.reset("abc", true);
  ASSERT_EQ(string, "abc");

  buffer = LazyBuffer();
  ASSERT_TRUE(string.empty());

}

TEST_F(LazyBufferTest, StringState) {

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

TEST_F(LazyBufferTest, ReferenceState) {

  std::string string = "abc";
  LazyBuffer buffer(&string);
  ASSERT_EQ(buffer.slice(), "abc");

  LazyBuffer reference = LazyBufferReference(buffer);
  ASSERT_EQ(reference.TEST_state(), LazyBufferState::light_state());
  ASSERT_TRUE(reference.valid());
  ASSERT_EQ(reference.slice(), "abc");

  buffer.trans_to_string()->assign("LOL");
  reference = LazyBufferReference(buffer);
  ASSERT_EQ(reference.TEST_state(), LazyBufferState::reference_state());
  ASSERT_FALSE(reference.valid());
  ASSERT_OK(reference.fetch());
  ASSERT_EQ(reference.slice(), "LOL");

  buffer.trans_to_string()->assign("LOLLLL");
  reference = LazyBufferReference(buffer);
  LazyBuffer remote_suffix = LazyBufferRemoveSuffix(&reference, 3);
  ASSERT_OK(remote_suffix.fetch());
  ASSERT_TRUE(buffer.valid());
  ASSERT_EQ(remote_suffix.slice(), "LOL");

}

TEST_F(LazyBufferTest, CleanableState) {

  std::string string = "abc";
  Cleanable clean([](void* arg1, void* /*arg2*/) {
    reinterpret_cast<std::string*>(arg1)->assign("empty");
  }, &string, nullptr);

  LazyBuffer buffer(string, std::move(clean));
  ASSERT_EQ(buffer.slice(), "abc");

  buffer.clear();
  ASSERT_EQ(string, "empty");

}


TEST_F(LazyBufferTest, Constructor) {

  {   // empty
    LazyBuffer buffer;
    ASSERT_EQ(buffer.TEST_state(), LazyBufferState::light_state());
    ASSERT_TRUE(buffer.valid());
    ASSERT_EQ(buffer.slice(), "");
  }{  // uninitialized resize
    LazyBuffer buffer(size_t(0));
    ASSERT_EQ(buffer.TEST_state(), LazyBufferState::light_state());
    ASSERT_TRUE(buffer.valid());
    ASSERT_TRUE(buffer.empty());
  }{  // uninitialized resize
    LazyBuffer buffer(10);
    ASSERT_EQ(buffer.TEST_state(), LazyBufferState::light_state());
    ASSERT_TRUE(buffer.valid());
    ASSERT_EQ(buffer.size(), 10);
  }{  // uninitialized resize
    LazyBuffer buffer(100);
    ASSERT_EQ(buffer.TEST_state(), LazyBufferState::buffer_state());
    ASSERT_TRUE(buffer.valid());
    ASSERT_EQ(buffer.size(), 100);
  }{  // move
    LazyBuffer buffer(LazyBuffer("abc", true));
    ASSERT_EQ(buffer.TEST_state(), LazyBufferState::light_state());
    ASSERT_TRUE(buffer.valid());
    ASSERT_EQ(buffer.slice(), "abc");
  }{  // move
    std::string string = "abc";
    LazyBuffer buffer(LazyBuffer(((void)0, &string)));
    ASSERT_EQ(buffer.TEST_state(), LazyBufferState::string_state());
    ASSERT_TRUE(buffer.valid());
    ASSERT_EQ(buffer.slice(), string);
  }{  // move
    LazyBuffer buffer_move;
    buffer_move.get_builder()->resize(100);
    LazyBuffer buffer(std::move(buffer_move));
    ASSERT_EQ(buffer.TEST_state(), LazyBufferState::buffer_state());
    ASSERT_TRUE(buffer.valid());
    ASSERT_EQ(buffer.size(), 100);
  }

}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
