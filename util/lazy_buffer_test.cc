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

class LazyBufferTest : public testing::Test {
 protected:
  struct MallocCustomizeBuffer {
    struct Handle {
      void* ptr;
      size_t size;
      Handle() : ptr(nullptr), size(0) {}
      ~Handle() {
        if (ptr != nullptr) {
          ::free(ptr);
        }
      }
    };
    static void* uninitialized_resize(void* handle, size_t size) {
      auto mem = reinterpret_cast<Handle*>(handle);
      if (size > 0) {
        auto new_ptr = realloc(mem->ptr, size);
        if (new_ptr == nullptr) {
          return nullptr;
        }
        mem->ptr = new_ptr;
      }
      mem->size = size;
      return mem->ptr;
    }
    Handle handle;
    LazyBufferCustomizeBuffer get() { return {&handle, &uninitialized_resize}; }
  };

  struct StringCustomizeBuffer {
    using Handle = std::string;
    static void* uninitialized_resize(void* handle, size_t size) {
      auto string_ptr = reinterpret_cast<Handle*>(handle);
      try {
        string_ptr->resize(size);
      } catch (...) {
        return nullptr;
      }
      return (void*)string_ptr->data();
    };
    Handle handle;
    LazyBufferCustomizeBuffer get() { return {&handle, &uninitialized_resize}; }
  };
};

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
  ASSERT_EQ(buffer.state(), LazyBufferState::light_state());

  std::string string('a', 33);
  buffer.reset(string, false);
  ASSERT_EQ(buffer.state(), LazyBufferState::light_state());

  for (auto& c : string) {
    c = 'b';
  };
  ASSERT_OK(buffer.fetch());
  ASSERT_EQ(buffer.slice(), string);

  buffer.reset(Slice(string.data(), 32), true);
  ASSERT_EQ(buffer.state(), LazyBufferState::light_state());

  buffer.reset(string, true);
  ASSERT_EQ(buffer.state(), LazyBufferState::buffer_state());

  string = "abc";
  buffer.reset(string);
  for (auto& c : string) {
    c = 'a';
  };
  ASSERT_OK(buffer.fetch());
  ASSERT_EQ(buffer.slice(), "aaa");

  buffer.reset(string, true);
  for (auto& c : string) {
    c = 'b';
  };
  ASSERT_OK(buffer.fetch());
  ASSERT_EQ(buffer.slice(), "aaa");
}

TEST_F(LazyBufferTest, BufferState) {
  auto test = [](LazyBuffer& b) {
    auto builder = b.get_builder();
    ASSERT_TRUE(b.valid());
    // builder->resize() has no reload function
    // ASSERT_FALSE(builder->resize(size_t(-1)));
    // ASSERT_NOK(builder->fetch());
    // ASSERT_FALSE(builder->resize(3));

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
    b.reset("abc", true);

    b.reset(LazyBuffer());
  };

  // LazyBuffer buffer(size_t(-1));
  LazyBuffer buffer(Status::Aborted());
  ASSERT_EQ(buffer.state(), LazyBufferState::buffer_state());
  ASSERT_NOK(buffer.fetch());
  ASSERT_NOK(buffer.fetch());  // test fetch twice
  buffer.clear();
  test(buffer);
  ASSERT_EQ(buffer.state(), LazyBufferState::light_state());

  test(buffer);
  ASSERT_EQ(buffer.state(), LazyBufferState::light_state());

  buffer = LazyBuffer("123");
  test(buffer);
  ASSERT_EQ(buffer.state(), LazyBufferState::light_state());

  StringCustomizeBuffer customize_buffer;
  buffer.reset(customize_buffer.get());
  test(buffer);
  ASSERT_EQ(customize_buffer.handle, "abc");
}

TEST_F(LazyBufferTest, StringState) {
  std::string string = "abc";
  LazyBuffer buffer(&string);
  ASSERT_EQ(buffer.state(), LazyBufferState::string_state());
  ASSERT_EQ(buffer.slice(), "abc");

  buffer.reset("aaa", true);
  buffer.trans_to_string()->append("bbb");
  ASSERT_OK(buffer.fetch());
  ASSERT_EQ(buffer.slice(), "aaabbb");
  ASSERT_EQ(buffer.TEST_context()->data[1], 0);

  buffer.get_builder()->resize(buffer.size() - 1);
  ASSERT_EQ(buffer.state(), LazyBufferState::string_state());
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

TEST_F(LazyBufferTest, LazyBufferReference) {
  std::string string = "abc";
  LazyBuffer buffer(&string);
  ASSERT_EQ(buffer.slice(), "abc");

  LazyBuffer reference = LazyBufferReference(buffer);
  ASSERT_EQ(reference.state(), LazyBufferState::light_state());
  ASSERT_TRUE(reference.valid());
  ASSERT_EQ(reference.slice(), "abc");

  buffer.trans_to_string()->assign("LOL");
  reference = LazyBufferReference(buffer);
  ASSERT_EQ(reference.state(), LazyBufferState::reference_state());
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

TEST_F(LazyBufferTest, ConstructorEmpty) {
  LazyBuffer buffer;
  ASSERT_EQ(buffer.state(), LazyBufferState::light_state());
  ASSERT_TRUE(buffer.valid());
  ASSERT_EQ(buffer.slice(), "");
}

TEST_F(LazyBufferTest, ConstructorUninitializedResize) {
  LazyBuffer buffer_0(size_t(0));
  ASSERT_EQ(buffer_0.state(), LazyBufferState::light_state());
  ASSERT_TRUE(buffer_0.valid());
  ASSERT_TRUE(buffer_0.empty());

  LazyBuffer buffer_1(10);
  ASSERT_EQ(buffer_1.state(), LazyBufferState::light_state());
  ASSERT_TRUE(buffer_1.valid());
  ASSERT_EQ(buffer_1.size(), 10);

  LazyBuffer buffer_2(100);
  ASSERT_EQ(buffer_2.state(), LazyBufferState::buffer_state());
  ASSERT_TRUE(buffer_2.valid());
  ASSERT_EQ(buffer_2.size(), 100);

  LazyBuffer buffer_3(Status::Aborted());
  ASSERT_EQ(buffer_3.state(), LazyBufferState::buffer_state());
  ASSERT_FALSE(buffer_3.valid());
  ASSERT_NOK(buffer_3.fetch());
}

TEST_F(LazyBufferTest, ConstructorMove) {
  LazyBuffer buffer_0(LazyBuffer("abc", true, 2));
  ASSERT_EQ(buffer_0.state(), LazyBufferState::light_state());
  ASSERT_TRUE(buffer_0.valid());
  ASSERT_EQ(buffer_0.slice(), "abc");
  ASSERT_EQ(buffer_0.file_number(), 2);

  std::string string = "abc";
  LazyBuffer buffer_1(LazyBuffer(((void)0, &string)));
  ASSERT_EQ(buffer_1.state(), LazyBufferState::string_state());
  ASSERT_TRUE(buffer_1.valid());
  ASSERT_EQ(buffer_1.slice(), string);

  LazyBuffer buffer_move;
  buffer_move.get_builder()->resize(100);
  LazyBuffer buffer_2(std::move(buffer_move));
  ASSERT_EQ(buffer_2.state(), LazyBufferState::buffer_state());
  ASSERT_TRUE(buffer_2.valid());
  ASSERT_EQ(buffer_2.size(), 100);

  LazyBuffer buffer_3(LazyBuffer(((void)0, Status::Corruption())));
  ASSERT_EQ(buffer_3.state(), LazyBufferState::buffer_state());
  ASSERT_FALSE(buffer_3.valid());
  ASSERT_TRUE(buffer_3.fetch().IsCorruption());
}

TEST_F(LazyBufferTest, ConstructorReference) {
  std::string string = "abc";
  LazyBuffer buffer(string, false, 2);
  ASSERT_EQ(buffer.state(), LazyBufferState::light_state());
  ASSERT_TRUE(buffer.valid());
  ASSERT_EQ(buffer.slice(), "abc");
  ASSERT_EQ(buffer.file_number(), 2);
  string = "LOL";
  ASSERT_EQ(buffer.slice(), "LOL");
}

TEST_F(LazyBufferTest, ConstructorCopying) {
  std::string string = "abc";
  LazyBuffer buffer_0(string, true, 2);
  ASSERT_EQ(buffer_0.state(), LazyBufferState::light_state());
  ASSERT_TRUE(buffer_0.valid());
  ASSERT_EQ(buffer_0.slice(), "abc");
  ASSERT_EQ(buffer_0.file_number(), 2);
  string = "LOL";
  ASSERT_EQ(buffer_0.slice(), "abc");

  string = std::string('a', 100);
  LazyBuffer buffer_1(string, true, 2);
  ASSERT_EQ(buffer_1.state(), LazyBufferState::buffer_state());
  ASSERT_TRUE(buffer_1.valid());
  ASSERT_EQ(buffer_1.slice(), string);
  ASSERT_EQ(buffer_1.file_number(), 2);
  string = "LOL";
  ASSERT_EQ(buffer_1.slice(), std::string('a', 100));
}

TEST_F(LazyBufferTest, ConstructorStatus) {
  LazyBuffer buffer_0(Status::Corruption());
  ASSERT_EQ(buffer_0.state(), LazyBufferState::buffer_state());
  ASSERT_FALSE(buffer_0.valid());
  ASSERT_TRUE(buffer_0.fetch().IsCorruption());

  LazyBuffer buffer_1(Status::OK());
  ASSERT_EQ(buffer_1.state(), LazyBufferState::light_state());
  ASSERT_TRUE(buffer_1.valid());
}

TEST_F(LazyBufferTest, ConstructorCustomizeBuffer) {
  MallocCustomizeBuffer customize_buffer_0;
  LazyBuffer buffer_0(customize_buffer_0.get());
  ASSERT_EQ(buffer_0.state(), LazyBufferState::buffer_state());
  buffer_0.reset("abc", true, 2);
  ASSERT_TRUE(buffer_0.valid());
  ASSERT_EQ(buffer_0.slice(), "abc");
  ASSERT_EQ(::memcmp(customize_buffer_0.handle.ptr, "abc", 3), 0);
  buffer_0.clear();
  ASSERT_EQ(customize_buffer_0.handle.size, 0);

  StringCustomizeBuffer customize_buffer_1;
  LazyBuffer buffer_1(customize_buffer_1.get());
  ASSERT_EQ(buffer_1.state(), LazyBufferState::buffer_state());
  buffer_1.reset("abc", true, 2);
  ASSERT_TRUE(buffer_1.valid());
  ASSERT_EQ(buffer_1.slice(), "abc");
  ASSERT_EQ(customize_buffer_1.handle, "abc");
  buffer_1.clear();
  ASSERT_TRUE(customize_buffer_1.handle.empty());
}

TEST_F(LazyBufferTest, ConstructorString) {
  std::string string = "abc";
  LazyBuffer buffer(&string);
  ASSERT_EQ(buffer.state(), LazyBufferState::string_state());
  ASSERT_TRUE(buffer.valid());
  ASSERT_EQ(buffer.slice(), string);
  buffer.reset("LOL", true, 2);
  ASSERT_TRUE(buffer.valid());
  ASSERT_EQ(buffer.slice(), "LOL");
  ASSERT_EQ(buffer.file_number(), 2);
  ASSERT_EQ(string, "LOL");
}

TEST_F(LazyBufferTest, ConstructorCleanable) {
  std::string string = "abc";
  Cleanable clean(
      [](void* arg1, void* /*arg2*/) {
        reinterpret_cast<std::string*>(arg1)->assign("empty");
      },
      &string, nullptr);

  LazyBuffer buffer(string, std::move(clean), 2);
  ASSERT_EQ(buffer.state(), LazyBufferState::cleanable_state());
  ASSERT_EQ(buffer.slice(), "abc");
  ASSERT_EQ(buffer.file_number(), 2);

  buffer.clear();
  ASSERT_EQ(string, "empty");
}

TEST_F(LazyBufferTest, ConstructorCustomizeState) {
  class CustomizeLazyBufferState : public LazyBufferState {
   public:
    std::string* string = nullptr;
    void destroy(LazyBuffer* /*buffer*/) const override {
      string->assign("empty");
    }
    Status fetch_buffer(LazyBuffer* buffer) const override {
      buffer->reset("123", false, buffer->file_number());
      return Status::OK();
    }
  } state;
  std::string string;
  state.string = &string;

  LazyBuffer buffer(&state, {}, Slice::Invalid(), 2);
  ASSERT_EQ(buffer.state(), &state);
  ASSERT_FALSE(buffer.valid());
  ASSERT_EQ(buffer.file_number(), 2);
  ASSERT_OK(buffer.fetch());
  ASSERT_EQ(buffer.slice(), "123");
  buffer.reset("LOL", true);
  ASSERT_EQ(string, "empty");
}

TEST_F(LazyBufferTest, DumpToString) {
  std::string string;
  LazyBuffer buffer(&string);
  buffer.trans_to_string()->assign("abc");
  std::move(buffer).dump(&string);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
