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
  ASSERT_EQ(empty.get_slice(), "");

  LazyBuffer abc("abc");
  ASSERT_OK(abc.fetch());
  ASSERT_EQ(abc.get_slice(), "abc");

  LazyBuffer abc2(std::move(abc));
  ASSERT_OK(abc2.fetch());
  ASSERT_EQ(abc2.get_slice(), "abc");

  LazyBuffer abc3 = std::move(abc2);
  ASSERT_OK(abc3.fetch());
  ASSERT_EQ(abc3.get_slice(), "abc");

}

TEST_F(LazyBufferTest, DefaultColtroller) {

  LazyBuffer buffer;
  ASSERT_EQ(buffer.TEST_controller(),
            LazyBufferController::default_controller());

  std::string string('a', 33);
  buffer.reset(string, false);
  ASSERT_EQ(buffer.TEST_controller(),
            LazyBufferController::default_controller());

  for (auto &c : string) { c = 'b'; };
  ASSERT_OK(buffer.fetch());
  ASSERT_EQ(buffer.get_slice(), string);

  buffer.reset(Slice(string.data(), 32), true);
  ASSERT_EQ(buffer.TEST_controller(),
            LazyBufferController::default_controller());

  buffer.reset(string, true);
  ASSERT_EQ(buffer.TEST_controller(),
            LazyBufferController::buffer_controller());

  string = "abc";
  buffer.reset(string);
  for (auto &c : string) { c = 'a'; };
  ASSERT_OK(buffer.fetch());
  ASSERT_EQ(buffer.get_slice(), "aaa");

  buffer.reset(string, true);
  for (auto &c : string) { c = 'b'; };
  ASSERT_OK(buffer.fetch());
  ASSERT_EQ(buffer.get_slice(), "aaa");

}


TEST_F(LazyBufferTest, BufferColtroller) {

  LazyBuffer buffer(size_t(-1));
  ASSERT_EQ(buffer.TEST_controller(),
            LazyBufferController::buffer_controller());
  ASSERT_NOK(buffer.fetch());

  buffer.clear();
  ASSERT_EQ(buffer.TEST_controller(),
            LazyBufferController::buffer_controller());

}

TEST_F(LazyBufferTest, StringColtroller) {

  std::string string = "abc";
  LazyBuffer buffer(&string);
  ASSERT_EQ(buffer.TEST_controller(),
      LazyBufferController::string_controller());
  ASSERT_EQ(buffer.get_slice(), "abc");

  buffer.reset("aaa", true);
  buffer.trans_to_string()->append("bbb");
  ASSERT_OK(buffer.fetch());
  ASSERT_EQ(buffer.get_slice(), "aaabbb");
  ASSERT_EQ(buffer.TEST_rep()->data[1], 0);

  buffer.get_editor()->resize(buffer.size() - 1);
  ASSERT_EQ(buffer.TEST_controller(),
      LazyBufferController::string_controller());
  ASSERT_OK(std::move(buffer).dump(&string));
  ASSERT_EQ(string, "aaabb");

  buffer.reset(string);
  buffer.trans_to_string()->resize(string.size() - 1);
  ASSERT_OK(std::move(buffer).dump(&string));
  ASSERT_EQ(buffer.TEST_rep()->data[1], 1);
  ASSERT_EQ(string, "aaab");

  buffer.reset(string);
  buffer.get_editor()->resize(string.size() - 1);
  ASSERT_OK(std::move(buffer).dump(&string));
  ASSERT_EQ(string, "aaa");

}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
