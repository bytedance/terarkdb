#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iterator>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

template <class index_t, class flags_in_front_t = std::true_type>
struct threaded_rbtree_node_t {
  typedef index_t index_type;

  index_type children[2];

  threaded_rbtree_node_t(index_type left, index_type right) {
    children[0] = left;
    children[1] = right;
  }
  threaded_rbtree_node_t() {}

  static std::size_t constexpr flag_bit_mask =
      index_type(1) << (flags_in_front_t::value ? sizeof(index_type) * 8 - 1
                                                : 0);
  static std::size_t constexpr type_bit_mask =
      index_type(1) << (flags_in_front_t::value ? sizeof(index_type) * 8 - 2
                                                : 1);
  static std::size_t constexpr full_bit_mask = flag_bit_mask | type_bit_mask;

  static std::size_t constexpr nil_sentinel = ~index_type(0) & ~full_bit_mask;

  bool left_is_child() const { return (children[0] & type_bit_mask) == 0; }

  bool right_is_child() const { return (children[1] & type_bit_mask) == 0; }

  bool left_is_thread() const { return (children[0] & type_bit_mask) != 0; }

  bool right_is_thread() const { return (children[1] & type_bit_mask) != 0; }

  void left_set_child() { children[0] &= ~type_bit_mask; }

  void right_set_child() { children[1] &= ~type_bit_mask; }

  void left_set_thread() { children[0] |= type_bit_mask; }

  void right_set_thread() { children[1] |= type_bit_mask; }

  void left_set_link(std::size_t link) {
    children[0] = index_type((children[0] & full_bit_mask) | link);
  }

  void right_set_link(std::size_t link) {
    children[1] = index_type((children[1] & full_bit_mask) | link);
  }

  std::size_t left_get_link() const { return children[0] & ~full_bit_mask; }

  std::size_t right_get_link() const { return children[1] & ~full_bit_mask; }

  bool is_used() const { return (children[1] & flag_bit_mask) == 0; }

  void set_used() { children[1] &= ~flag_bit_mask; }

  bool is_empty() const { return (children[1] & flag_bit_mask) != 0; }

  void set_empty() { children[1] |= flag_bit_mask; }

  bool is_black() const { return (children[0] & flag_bit_mask) == 0; }

  void set_black() { children[0] &= ~flag_bit_mask; }

  bool is_red() const { return (children[0] & flag_bit_mask) != 0; }

  void set_red() { children[0] |= flag_bit_mask; }
};

template <class dereference_t>
std::size_t threaded_rbtree_move_next(std::size_t node, dereference_t deref) {
  if (deref(node).right_is_thread()) {
    return deref(node).right_get_link();
  } else {
    node = deref(node).right_get_link();
    while (deref(node).left_is_child()) {
      node = deref(node).left_get_link();
    }
    return node;
  }
}

template <class dereference_t>
std::size_t threaded_rbtree_move_prev(std::size_t node, dereference_t deref) {
  if (deref(node).left_is_thread()) {
    return deref(node).left_get_link();
  } else {
    node = deref(node).left_get_link();
    while (deref(node).right_is_child()) {
      node = deref(node).right_get_link();
    }
    return node;
  }
}

template <class node_t, class enable_count_t>
struct threaded_rbtree_count_t {
  typedef node_t count_node_type;
  typedef typename count_node_type::index_type count_index_type;

  count_index_type count;

  threaded_rbtree_count_t() : count(0) {}

  void increase_count() { ++count; }

  void decrease_count() { --count; }

  void reset() { count = 0; }

  std::size_t get_count() const { return count; }
};

template <class node_t>
struct threaded_rbtree_count_t<node_t, std::false_type> {
  typedef node_t count_node_type;

  void increase_count() {}

  void decrease_count() {}

  void reset() {}

  std::size_t get_count() const { return 0; }
};

template <class node_t, class enable_most_t>
struct threaded_rbtree_most_t {
  typedef node_t most_node_type;
  typedef typename most_node_type::index_type most_index_type;

  most_index_type left, right;

  threaded_rbtree_most_t()
      : left(most_node_type::nil_sentinel),
        right(most_node_type::nil_sentinel) {}

  template <class dereference_t>
  std::size_t get_left(std::size_t, dereference_t) const {
    return left;
  }

  template <class dereference_t>
  std::size_t get_right(std::size_t, dereference_t) const {
    return right;
  }

  void set_left(std::size_t value) { left = most_index_type(value); }

  void set_right(std::size_t value) { right = most_index_type(value); }

  void update_left(std::size_t check, std::size_t value) {
    if (check == left) {
      left = most_index_type(value);
    }
  }

  void update_right(std::size_t check, std::size_t value) {
    if (check == right) {
      right = most_index_type(value);
    }
  }

  template <class dereference_t>
  void detach_left(std::size_t value, dereference_t deref) {
    if (value == left) {
      left = most_index_type(threaded_rbtree_move_next(left, deref));
    }
  }

  template <class dereference_t>
  void detach_right(std::size_t value, dereference_t deref) {
    if (value == right) {
      right = most_index_type(threaded_rbtree_move_prev(right, deref));
    }
  }
};

template <class node_t>
struct threaded_rbtree_most_t<node_t, std::false_type> {
  typedef node_t most_node_type;

  template <class dereference_t>
  std::size_t get_left(std::size_t root, dereference_t deref) const {
    std::size_t node = root;
    if (root != most_node_type::nil_sentinel) {
      while (deref(node).left_is_child()) {
        node = deref(node).left_get_link();
      }
    }
    return node;
  }

  template <class dereference_t>
  std::size_t get_right(std::size_t root, dereference_t deref) const {
    std::size_t node = root;
    if (root != most_node_type::nil_sentinel) {
      while (deref(node).right_is_child()) {
        node = deref(node).right_get_link();
      }
    }
    return node;
  }

  void set_left(std::size_t) {}

  void set_right(std::size_t) {}

  void update_left(std::size_t, std::size_t) {}

  void update_right(std::size_t, std::size_t) {}

  template <class dereference_t>
  void detach_left(std::size_t, dereference_t) {}

  template <class dereference_t>
  void detach_right(std::size_t, dereference_t) {}
};

template <class node_t, class enable_count_t, class enable_most_t>
struct threaded_rbtree_root_t {
  typedef node_t node_type;
  typedef typename node_type::index_type index_type;

  threaded_rbtree_root_t() { root.root = node_type::nil_sentinel; }

  struct
#ifdef _MSC_VER
      __declspec(empty_bases)
#endif
          root_type : public threaded_rbtree_count_t<node_t, enable_count_t>,
                      public threaded_rbtree_most_t<node_t, enable_most_t> {
    index_type root;
  } root;

  std::size_t get_count() const { return root.get_count(); }

  template <class dereference_t>
  std::size_t get_most_left(dereference_t deref) const {
    return root.get_left(root.root, deref);
  }

  template <class dereference_t>
  std::size_t get_most_right(dereference_t deref) const {
    return root.get_right(root.root, deref);
  }
};

template <class node_t, size_t max_depth>
struct threaded_rbtree_stack_t {
  typedef node_t node_type;
  typedef typename node_type::index_type index_type;
  typedef decltype(node_type::flag_bit_mask) mask_type;

  static mask_type constexpr dir_bit_mask = node_type::flag_bit_mask;

  std::size_t height;
  index_type stack[max_depth];

  bool is_left(std::size_t k) const {
    assert(k < max_depth);
    return (mask_type(stack[k]) & dir_bit_mask) == 0;
  }

  bool is_right(std::size_t k) const {
    assert(k < max_depth);
    return (mask_type(stack[k]) & dir_bit_mask) != 0;
  }

  std::size_t get_index(std::size_t k) const {
    assert(k < max_depth);
    return index_type(mask_type(stack[k]) & ~dir_bit_mask);
  }

  void push_index(std::size_t index, bool left) {
    if (height == max_depth) {
      throw std::length_error("thread_tb_tree_stack overflow");
    }
    stack[height++] = index_type(mask_type(index) | (left ? 0 : dir_bit_mask));
  }

  void update_index(std::size_t k, std::size_t index, bool left) {
    assert(k < max_depth);
    stack[k] = index_type(mask_type(index) | (left ? 0 : dir_bit_mask));
  }

  void update_index(std::size_t k, std::size_t index) {
    assert(k < max_depth);
    stack[k] =
        index_type(mask_type(index) | (mask_type(stack[k]) & dir_bit_mask));
  }
};

namespace threded_rb_tree_tools {
template <typename U, typename C, int (U::*)(C const &, C const &) const>
struct check_comparator_1;
template <typename U, typename C>
std::true_type check_comparator(check_comparator_1<U, C, &U::compare> *) {
  return std::true_type();
}
template <typename U, typename C, int (U::*)(C, C) const>
struct check_comparator_2;
template <typename U, typename C>
std::true_type check_comparator(check_comparator_2<U, C, &U::compare> *) {
  return std::true_type();
}
template <typename U, typename C>
std::false_type check_comparator(...) {
  return std::false_type();
}
template <typename U, typename C>
struct has_compare : public decltype(check_comparator<U, C>(nullptr)) {};
}  // namespace threded_rb_tree_tools

template <class root_t, template <class, size_t> class stack_t,
          class comparator_t, class deref_node_t, size_t max_depth>
void threaded_rbtree_find_path_for_multi(
    root_t &root, stack_t<typename root_t::node_type, max_depth> &stack,
    deref_node_t deref, std::size_t index, comparator_t comparator) {
  typedef typename root_t::node_type node_type;

  stack.height = 0;
  std::size_t p = root.root.root;
  while (p != node_type::nil_sentinel) {
    if (comparator(index, p)) {
      stack.push_index(p, true);
      if (deref(p).left_is_thread()) {
        return;
      }
      p = deref(p).left_get_link();
    } else {
      stack.push_index(p, false);
      if (deref(p).right_is_thread()) {
        return;
      }
      p = deref(p).right_get_link();
    }
  }
}

template <class root_t, template <class, size_t> class stack_t,
          class comparator_t, class key_t, class deref_node_t,
          class deref_key_t, size_t max_depth>
void threaded_rbtree_find_path_for_multi(
    root_t &root, stack_t<typename root_t::node_type, max_depth> &stack,
    deref_node_t deref, key_t const &key, deref_key_t deref_key,
    comparator_t comparator) {
  typedef typename root_t::node_type node_type;

  stack.height = 0;
  std::size_t p = root.root.root;
  while (p != node_type::nil_sentinel) {
    if (comparator(key, deref_key(p))) {
      stack.push_index(p, true);
      if (deref(p).left_is_thread()) {
        return;
      }
      p = deref(p).left_get_link();
    } else {
      stack.push_index(p, false);
      if (deref(p).right_is_thread()) {
        return;
      }
      p = deref(p).right_get_link();
    }
  }
}

template <class root_t, template <class, size_t> class stack_t,
          class comparator_t, class key_t, class deref_node_t,
          class deref_key_t, size_t max_depth>
bool threaded_rbtree_find_path_for_unique(
    root_t &root, stack_t<typename root_t::node_type, max_depth> &stack,
    deref_node_t deref, key_t const &key, deref_key_t deref_key,
    comparator_t comparator) {
  typedef typename threded_rb_tree_tools::has_compare<comparator_t, key_t>::type
      comparator_3way;
  return threaded_rbtree_find_path_for_unique(
      root, stack, deref, key, deref_key, comparator, comparator_3way());
}

template <class root_t, template <class, size_t> class stack_t,
          class comparator_t, class key_t, class deref_node_t,
          class deref_key_t, size_t max_depth>
bool threaded_rbtree_find_path_for_unique(
    root_t &root, stack_t<typename root_t::node_type, max_depth> &stack,
    deref_node_t deref, key_t const &key, deref_key_t deref_key,
    comparator_t comparator, std::false_type) {
  typedef typename root_t::node_type node_type;

  stack.height = 0;
  std::size_t p = root.root.root;
  while (p != node_type::nil_sentinel) {
    if (comparator(key, deref_key(p))) {
      stack.push_index(p, true);
      if (deref(p).left_is_thread()) {
        return false;
      }
      p = deref(p).left_get_link();
    } else {
      stack.push_index(p, false);
      if (!comparator(deref_key(p), key)) {
        return true;
      }
      if (deref(p).right_is_thread()) {
        return false;
      }
      p = deref(p).right_get_link();
    }
  }
  return false;
}

template <class root_t, template <class, size_t> class stack_t,
          class comparator_t, class key_t, class deref_node_t,
          class deref_key_t, size_t max_depth>
bool threaded_rbtree_find_path_for_unique(
    root_t &root, stack_t<typename root_t::node_type, max_depth> &stack,
    deref_node_t deref, key_t const &key, deref_key_t deref_key,
    comparator_t comparator, std::true_type) {
  typedef typename root_t::node_type node_type;

  stack.height = 0;
  std::size_t p = root.root.root;
  while (p != node_type::nil_sentinel) {
    int c = comparator.compare(key, deref_key(p));
    if (c < 0) {
      stack.push_index(p, true);
      if (deref(p).left_is_thread()) {
        return false;
      }
      p = deref(p).left_get_link();
    } else {
      stack.push_index(p, false);
      if (c == 0) {
        return true;
      }
      if (deref(p).right_is_thread()) {
        return false;
      }
      p = deref(p).right_get_link();
    }
  }
  return false;
}

template <class root_t, template <class, size_t> class stack_t,
          class comparator_t, class deref_node_t, size_t max_depth>
bool threaded_rbtree_find_path_for_remove(
    root_t &root, stack_t<typename root_t::node_type, max_depth> &stack,
    deref_node_t deref, std::size_t index, comparator_t comparator) {
  typedef typename threded_rb_tree_tools::has_compare<
      comparator_t, std::size_t>::type comparator_3way;
  return threaded_rbtree_find_path_for_remove(root, stack, deref, index,
                                              comparator, comparator_3way());
}

template <class root_t, template <class, size_t> class stack_t,
          class comparator_t, class deref_node_t, size_t max_depth>
bool threaded_rbtree_find_path_for_remove(
    root_t &root, stack_t<typename root_t::node_type, max_depth> &stack,
    deref_node_t deref, std::size_t index, comparator_t comparator,
    std::false_type) {
  typedef typename root_t::node_type node_type;

  stack.height = 0;
  std::size_t p = root.root.root;
  while (p != node_type::nil_sentinel) {
    if (comparator(index, p)) {
      stack.push_index(p, true);
      if (deref(p).left_is_thread()) {
        return false;
      }
      p = deref(p).left_get_link();
    } else {
      stack.push_index(p, false);
      if (!comparator(p, index)) {
        return true;
      }
      if (deref(p).right_is_thread()) {
        return false;
      }
      p = deref(p).right_get_link();
    }
  }
  return false;
}

template <class root_t, template <class, size_t> class stack_t,
          class comparator_t, class deref_node_t, size_t max_depth>
bool threaded_rbtree_find_path_for_remove(
    root_t &root, stack_t<typename root_t::node_type, max_depth> &stack,
    deref_node_t deref, std::size_t index, comparator_t comparator,
    std::true_type) {
  typedef typename root_t::node_type node_type;

  stack.height = 0;
  std::size_t p = root.root.root;
  while (p != node_type::nil_sentinel) {
    int c = comparator.compare(index, p);
    if (c < 0) {
      stack.push_index(p, true);
      if (deref(p).left_is_thread()) {
        return false;
      }
      p = deref(p).left_get_link();
    } else {
      stack.push_index(p, false);
      if (c == 0) {
        return true;
      }
      if (deref(p).right_is_thread()) {
        return false;
      }
      p = deref(p).right_get_link();
    }
  }
  return false;
}

template <class root_t, class comparator_t, class key_t, class deref_node_t,
          class deref_key_t>
double threaded_rbtree_approximate_rank_ratio(root_t &root, deref_node_t deref,
                                              key_t const &key,
                                              deref_key_t deref_key,
                                              comparator_t comparator) {
  typedef typename root_t::node_type node_type;
  double rank_ratio = 0.5;
  double step = rank_ratio / 2;

  std::size_t p = root.root.root;
  while (p != node_type::nil_sentinel) {
    if (comparator(deref_key(p), key)) {
      rank_ratio += step;
      if (deref(p).right_is_thread()) {
        break;
      }
      p = deref(p).right_get_link();
    } else {
      rank_ratio -= step;
      if (deref(p).left_is_thread()) {
        break;
      }
      p = deref(p).left_get_link();
    }
    step /= 2;
  }
  return rank_ratio;
}

template <class root_t, class comparator_t, class key_t, class deref_node_t,
          class deref_key_t>
std::size_t threaded_rbtree_lower_bound(root_t &root, deref_node_t deref,
                                        key_t const &key, deref_key_t deref_key,
                                        comparator_t comparator) {
  typedef typename root_t::node_type node_type;

  std::size_t p = root.root.root, w = node_type::nil_sentinel;
  while (p != node_type::nil_sentinel) {
    if (comparator(deref_key(p), key)) {
      if (deref(p).right_is_thread()) {
        break;
      }
      p = deref(p).right_get_link();
    } else {
      w = p;
      if (deref(p).left_is_thread()) {
        break;
      }
      p = deref(p).left_get_link();
    }
  }
  return w;
}
template <class root_t, class comparator_t, class key_t, class deref_node_t,
          class deref_key_t>
std::size_t threaded_rbtree_equal_unique(root_t &root, deref_node_t deref,
                                         key_t const &key,
                                         deref_key_t deref_key,
                                         comparator_t comparator) {
  typedef typename threded_rb_tree_tools::has_compare<
      comparator_t, std::size_t>::type comparator_3way;
  return threaded_rbtree_equal_unique(root, deref, key, deref_key, comparator,
                                      comparator_3way());
}
template <class root_t, class comparator_t, class key_t, class deref_node_t,
          class deref_key_t>
std::size_t threaded_rbtree_equal_unique(root_t &root, deref_node_t deref,
                                         key_t const &key,
                                         deref_key_t deref_key,
                                         comparator_t comparator,
                                         std::false_type) {
  typedef typename root_t::node_type node_type;

  std::size_t p = root.root.root;
  while (p != node_type::nil_sentinel) {
    if (comparator(deref_key(p), key)) {
      if (deref(p).right_is_thread()) {
        return node_type::nil_sentinel;
      }
      p = deref(p).right_get_link();
    } else {
      if (!comparator(key, deref_key(p))) {
        return p;
      }
      if (deref(p).left_is_thread()) {
        return node_type::nil_sentinel;
      }
      p = deref(p).left_get_link();
    }
  }
  return node_type::nil_sentinel;
}
template <class root_t, class comparator_t, class key_t, class deref_node_t,
          class deref_key_t>
std::size_t threaded_rbtree_equal_unique(root_t &root, deref_node_t deref,
                                         key_t const &key,
                                         deref_key_t deref_key,
                                         comparator_t comparator,
                                         std::true_type) {
  typedef typename root_t::node_type node_type;

  std::size_t p = root.root.root;
  while (p != node_type::nil_sentinel) {
    int c = comparator.compare(deref_key(p), key);
    if (c < 0) {
      if (deref(p).right_is_thread()) {
        return node_type::nil_sentinel;
      }
      p = deref(p).right_get_link();
    } else {
      if (c == 0) {
        return p;
      }
      if (deref(p).left_is_thread()) {
        return node_type::nil_sentinel;
      }
      p = deref(p).left_get_link();
    }
  }
  return node_type::nil_sentinel;
}
template <class root_t, class comparator_t, class key_t, class deref_node_t,
          class deref_key_t>
std::size_t threaded_rbtree_reverse_lower_bound(root_t &root,
                                                deref_node_t deref,
                                                key_t const &key,
                                                deref_key_t deref_key,
                                                comparator_t comparator) {
  typedef typename root_t::node_type node_type;

  std::size_t p = root.root.root, w = node_type::nil_sentinel;
  while (p != node_type::nil_sentinel) {
    if (comparator(key, deref_key(p))) {
      if (deref(p).left_is_thread()) {
        break;
      }
      p = deref(p).left_get_link();
    } else {
      w = p;
      if (deref(p).right_is_thread()) {
        break;
      }
      p = deref(p).right_get_link();
    }
  }
  return w;
}

template <class root_t, class comparator_t, class key_t, class deref_node_t,
          class deref_key_t>
std::size_t threaded_rbtree_upper_bound(root_t &root, deref_node_t deref,
                                        key_t const &key, deref_key_t deref_key,
                                        comparator_t comparator) {
  typedef typename root_t::node_type node_type;

  std::size_t p = root.root.root, w = node_type::nil_sentinel;
  while (p != node_type::nil_sentinel) {
    if (comparator(key, deref_key(p))) {
      w = p;
      if (deref(p).left_is_thread()) {
        break;
      }
      p = deref(p).left_get_link();
    } else {
      if (deref(p).right_is_thread()) {
        break;
      }
      p = deref(p).right_get_link();
    }
  }
  return w;
}
template <class root_t, class comparator_t, class key_t, class deref_node_t,
          class deref_key_t>
std::size_t threaded_rbtree_reverse_upper_bound(root_t &root,
                                                deref_node_t deref,
                                                key_t const &key,
                                                deref_key_t deref_key,
                                                comparator_t comparator) {
  typedef typename root_t::node_type node_type;

  std::size_t p = root.root.root, w = node_type::nil_sentinel;
  while (p != node_type::nil_sentinel) {
    if (comparator(deref_key(p), key)) {
      w = p;
      if (deref(p).right_is_thread()) {
        break;
      }
      p = deref(p).right_get_link();
    } else {
      if (deref(p).left_is_thread()) {
        break;
      }
      p = deref(p).left_get_link();
    }
  }
  return w;
}

template <class root_t, class comparator_t, class key_t, class deref_node_t,
          class deref_key_t>
void threaded_rbtree_equal_range(root_t &root, deref_node_t deref,
                                 key_t const &key, deref_key_t deref_key,
                                 comparator_t comparator, std::size_t &lower,
                                 std::size_t &upper) {
  typedef typename root_t::node_type node_type;

  std::size_t p = root.root.root;
  lower = node_type::nil_sentinel;
  upper = node_type::nil_sentinel;
  while (p != node_type::nil_sentinel) {
    if (comparator(deref_key(p), key)) {
      if (deref(p).right_is_thread()) {
        break;
      }
      p = deref(p).right_get_link();
    } else {
      if (upper == node_type::nil_sentinel && comparator(key, deref_key(p))) {
        upper = p;
      }
      lower = p;
      if (deref(p).left_is_thread()) {
        break;
      }
      p = deref(p).left_get_link();
    }
  }
  p = upper == node_type::nil_sentinel
          ? root.root.root
          : deref(upper).left_is_child() ? deref(upper).left_get_link()
                                         : node_type::nil_sentinel;
  while (p != node_type::nil_sentinel) {
    if (comparator(key, deref_key(p))) {
      upper = p;
      if (deref(p).left_is_thread()) {
        break;
      }
      p = deref(p).left_get_link();
    } else {
      if (deref(p).right_is_thread()) {
        break;
      }
      p = deref(p).right_get_link();
    }
  }
}

template <class root_t, class comparator_t, class key_t, class deref_node_t,
          class deref_key_t>
void threaded_rbtree_reverse_equal_range(
    root_t &root, deref_node_t deref, key_t const &key, deref_key_t deref_key,
    comparator_t comparator, std::size_t &lower, std::size_t &upper) {
  typedef typename root_t::node_type node_type;

  std::size_t p = root.root.root;
  lower = node_type::nil_sentinel;
  upper = node_type::nil_sentinel;
  while (p != node_type::nil_sentinel) {
    if (comparator(key, deref_key(p))) {
      if (deref(p).left_is_thread()) {
        break;
      }
      p = deref(p).left_get_link();
    } else {
      if (upper == node_type::nil_sentinel && comparator(deref_key(p), key)) {
        upper = p;
      }
      lower = p;
      if (deref(p).right_is_thread()) {
        break;
      }
      p = deref(p).right_get_link();
    }
  }
  p = upper == node_type::nil_sentinel
          ? root.root.root
          : deref(upper).right_is_child() ? deref(upper).right_get_link()
                                          : node_type::nil_sentinel;
  while (p != node_type::nil_sentinel) {
    if (comparator(deref_key(p), key)) {
      upper = p;
      if (deref(p).right_is_thread()) {
        break;
      }
      p = deref(p).right_get_link();
    } else {
      if (deref(p).left_is_thread()) {
        break;
      }
      p = deref(p).left_get_link();
    }
  }
}
template <class root_t, template <class, size_t> class stack_t,
          class dereference_t, size_t max_depth>
void threaded_rbtree_insert(
    root_t &root, stack_t<typename root_t::node_type, max_depth> &stack,
    dereference_t deref, std::size_t index) {
  typedef typename root_t::node_type node_type;
  typedef typename root_t::index_type index_type;

  root.root.increase_count();
  deref(index).set_used();
  deref(index).left_set_thread();
  deref(index).right_set_thread();

  if (stack.height == 0) {
    deref(index).left_set_link(index_type(node_type::nil_sentinel));
    deref(index).right_set_link(index_type(node_type::nil_sentinel));
    deref(index).set_black();
    root.root.root = index_type(index);
    root.root.set_left(index);
    root.root.set_right(index);
    return;
  }
  deref(index).set_red();
  std::size_t k = stack.height - 1;
  std::size_t where = stack.get_index(k);
  if (stack.is_left(k)) {
    deref(index).left_set_link(deref(where).left_get_link());
    deref(index).right_set_link(where);
    deref(where).left_set_child();
    deref(where).left_set_link(index);
    root.root.update_left(where, index);
  } else {
    deref(index).right_set_link(deref(where).right_get_link());
    deref(index).left_set_link(where);
    deref(where).right_set_child();
    deref(where).right_set_link(index);
    root.root.update_right(where, index);
  }
  if (k >= 1) {
    while (deref(stack.get_index(k)).is_red()) {
      std::size_t p2 = stack.get_index(k - 1);
      std::size_t p1 = stack.get_index(k);
      if (stack.is_left(k - 1)) {
        std::size_t u = deref(p2).right_get_link();
        if (deref(p2).right_is_child() && deref(u).is_red()) {
          deref(p1).set_black();
          deref(u).set_black();
          deref(p2).set_red();
          if (k < 2) {
            break;
          }
          k -= 2;
        } else {
          std::size_t y;
          if (stack.is_left(k)) {
            y = p1;
          } else {
            y = deref(p1).right_get_link();
            deref(p1).right_set_link(deref(y).left_get_link());
            deref(y).left_set_link(p1);
            deref(p2).left_set_link(y);
            if (deref(y).left_is_thread()) {
              deref(y).left_set_child();
              deref(p1).right_set_thread();
              deref(p1).right_set_link(y);
            }
          }
          deref(p2).set_red();
          deref(y).set_black();
          deref(p2).left_set_link(deref(y).right_get_link());
          deref(y).right_set_link(p2);
          if (k == 1) {
            root.root.root = index_type(y);
          } else if (stack.is_left(k - 2)) {
            deref(stack.get_index(k - 2)).left_set_link(y);
          } else {
            deref(stack.get_index(k - 2)).right_set_link(y);
          }
          if (deref(y).right_is_thread()) {
            deref(y).right_set_child();
            deref(p2).left_set_thread();
            deref(p2).left_set_link(y);
          }
          assert(deref(p2).right_get_link() == u);
          break;
        }
      } else {
        std::size_t u = deref(p2).left_get_link();
        if (deref(p2).left_is_child() && deref(u).is_red()) {
          deref(p1).set_black();
          deref(u).set_black();
          deref(p2).set_red();
          if (k < 2) {
            break;
          }
          k -= 2;
        } else {
          std::size_t y;
          if (stack.is_right(k)) {
            y = p1;
          } else {
            y = deref(p1).left_get_link();
            deref(p1).left_set_link(deref(y).right_get_link());
            deref(y).right_set_link(p1);
            deref(p2).right_set_link(y);
            if (deref(y).right_is_thread()) {
              deref(y).right_set_child();
              deref(p1).left_set_thread();
              deref(p1).left_set_link(y);
            }
          }
          deref(p2).set_red();
          deref(y).set_black();
          deref(p2).right_set_link(deref(y).left_get_link());
          deref(y).left_set_link(p2);
          if (k == 1) {
            root.root.root = index_type(y);
          } else if (stack.is_right(k - 2)) {
            deref(stack.get_index(k - 2)).right_set_link(y);
          } else {
            deref(stack.get_index(k - 2)).left_set_link(y);
          }
          if (deref(y).left_is_thread()) {
            deref(y).left_set_child();
            deref(p2).right_set_thread();
            deref(p2).right_set_link(y);
          }
          assert(deref(p2).left_get_link() == u);
          break;
        }
      }
    }
  }
  deref(root.root.root).set_black();
}

template <class root_t, template <class, size_t> class stack_t,
          class dereference_t, size_t max_depth>
void threaded_rbtree_remove(
    root_t &root, stack_t<typename root_t::node_type, max_depth> &stack,
    dereference_t deref) {
  typedef typename root_t::node_type node_type;
  typedef typename root_t::index_type index_type;

  assert(stack.height > 0);
  root.root.decrease_count();
  std::size_t k = stack.height - 1;
  std::size_t p = stack.get_index(k);
  deref(p).set_empty();
  root.root.detach_left(p, deref);
  root.root.detach_right(p, deref);
  if (deref(p).right_is_thread()) {
    if (deref(p).left_is_child()) {
      std::size_t t = deref(p).left_get_link();
      while (deref(t).right_is_child()) {
        t = deref(t).right_get_link();
      }
      deref(t).right_set_link(deref(p).right_get_link());
      if (k == 0) {
        root.root.root = index_type(deref(p).left_get_link());
      } else if (stack.is_left(k - 1)) {
        deref(stack.get_index(k - 1)).left_set_link(deref(p).left_get_link());
      } else {
        deref(stack.get_index(k - 1)).right_set_link(deref(p).left_get_link());
      }
    } else {
      if (k == 0) {
        root.root.root = index_type(deref(p).left_get_link());
      } else if (stack.is_left(k - 1)) {
        deref(stack.get_index(k - 1)).left_set_link(deref(p).left_get_link());
        deref(stack.get_index(k - 1)).left_set_thread();
      } else {
        deref(stack.get_index(k - 1)).right_set_link(deref(p).right_get_link());
        deref(stack.get_index(k - 1)).right_set_thread();
      }
    }
  } else {
    std::size_t r = deref(p).right_get_link();
    if (deref(r).left_is_thread()) {
      deref(r).left_set_link(deref(p).left_get_link());
      if (deref(p).left_is_child()) {
        deref(r).left_set_child();
        std::size_t t = deref(p).left_get_link();
        while (deref(t).right_is_child()) {
          t = deref(t).right_get_link();
        }
        deref(t).right_set_link(r);
      } else {
        deref(r).left_set_thread();
      }
      if (k == 0) {
        root.root.root = index_type(r);
      } else if (stack.is_left(k - 1)) {
        deref(stack.get_index(k - 1)).left_set_link(r);
      } else {
        deref(stack.get_index(k - 1)).right_set_link(r);
      }
      bool is_red = deref(r).is_red();
      if (deref(p).is_red()) {
        deref(r).set_red();
      } else {
        deref(r).set_black();
      }
      if (is_red) {
        deref(p).set_red();
      } else {
        deref(p).set_black();
      }
      stack.update_index(k, r, false);
      ++k;
    } else {
      std::size_t s;
      std::size_t const j = stack.height - 1;
      for (++k;;) {
        stack.update_index(k, r, true);
        ++k;
        s = deref(r).left_get_link();
        if (deref(s).left_is_thread()) {
          break;
        }
        r = s;
      }
      assert(stack.get_index(j) == p);
      assert(j == stack.height - 1);
      stack.update_index(j, s, false);
      if (deref(s).right_is_child()) {
        deref(r).left_set_link(deref(s).right_get_link());
      } else {
        assert(deref(r).left_get_link() == s);
        deref(r).left_set_thread();
      }
      deref(s).left_set_link(deref(p).left_get_link());
      if (deref(p).left_is_child()) {
        std::size_t t = deref(p).left_get_link();
        while (deref(t).right_is_child()) {
          t = deref(t).right_get_link();
        }
        deref(t).right_set_link(s);
        deref(s).left_set_child();
      }
      deref(s).right_set_link(deref(p).right_get_link());
      deref(s).right_set_child();
      bool is_red = deref(p).is_red();
      if (deref(s).is_red()) {
        deref(p).set_red();
      } else {
        deref(p).set_black();
      }
      if (is_red) {
        deref(s).set_red();
      } else {
        deref(s).set_black();
      }
      if (j == 0) {
        root.root.root = index_type(s);
      } else if (stack.is_left(j - 1)) {
        deref(stack.get_index(j - 1)).left_set_link(s);
      } else {
        deref(stack.get_index(j - 1)).right_set_link(s);
      }
    }
  }
  if (deref(p).is_black()) {
    for (; k > 0; --k) {
      if (stack.is_left(k - 1)) {
        if (deref(stack.get_index(k - 1)).left_is_child()) {
          std::size_t x = deref(stack.get_index(k - 1)).left_get_link();
          if (deref(x).is_red()) {
            deref(x).set_black();
            break;
          }
        }
        std::size_t w = deref(stack.get_index(k - 1)).right_get_link();
        if (deref(w).is_red()) {
          deref(w).set_black();
          deref(stack.get_index(k - 1)).set_red();
          deref(stack.get_index(k - 1))
              .right_set_link(deref(w).left_get_link());
          deref(w).left_set_link(stack.get_index(k - 1));
          if (k == 1) {
            root.root.root = index_type(w);
          } else if (stack.is_left(k - 2)) {
            deref(stack.get_index(k - 2)).left_set_link(w);
          } else {
            deref(stack.get_index(k - 2)).right_set_link(w);
          }
          stack.update_index(k, stack.get_index(k - 1), true);
          stack.update_index(k - 1, w);
          w = deref(stack.get_index(k)).right_get_link();
          ++k;
        }
        if ((deref(w).left_is_thread() ||
             deref(deref(w).left_get_link()).is_black()) &&
            (deref(w).right_is_thread() ||
             deref(deref(w).right_get_link()).is_black())) {
          deref(w).set_red();
        } else {
          if (deref(w).right_is_thread() ||
              deref(deref(w).right_get_link()).is_black()) {
            std::size_t y = deref(w).left_get_link();
            deref(y).set_black();
            deref(w).set_red();
            deref(w).left_set_link(deref(y).right_get_link());
            deref(y).right_set_link(w);
            deref(stack.get_index(k - 1)).right_set_link(y);
            if (deref(y).right_is_thread()) {
              std::size_t z = deref(y).right_get_link();
              deref(y).right_set_child();
              deref(z).left_set_thread();
              deref(z).left_set_link(y);
            }
            w = y;
          }
          if (deref(stack.get_index(k - 1)).is_red()) {
            deref(w).set_red();
          } else {
            deref(w).set_black();
          }
          deref(stack.get_index(k - 1)).set_black();
          deref(deref(w).right_get_link()).set_black();
          deref(stack.get_index(k - 1))
              .right_set_link(deref(w).left_get_link());
          deref(w).left_set_link(stack.get_index(k - 1));
          if (k == 1) {
            root.root.root = index_type(w);
          } else if (stack.is_left(k - 2)) {
            deref(stack.get_index(k - 2)).left_set_link(w);
          } else {
            deref(stack.get_index(k - 2)).right_set_link(w);
          }
          if (deref(w).left_is_thread()) {
            deref(w).left_set_child();
            deref(stack.get_index(k - 1)).right_set_thread();
            deref(stack.get_index(k - 1)).right_set_link(w);
          }
          break;
        }
      } else {
        if (deref(stack.get_index(k - 1)).right_is_child()) {
          std::size_t x = deref(stack.get_index(k - 1)).right_get_link();
          if (deref(x).is_red()) {
            deref(x).set_black();
            break;
          }
        }
        std::size_t w = deref(stack.get_index(k - 1)).left_get_link();
        if (deref(w).is_red()) {
          deref(w).set_black();
          deref(stack.get_index(k - 1)).set_red();
          deref(stack.get_index(k - 1))
              .left_set_link(deref(w).right_get_link());
          deref(w).right_set_link(stack.get_index(k - 1));
          if (k == 1) {
            root.root.root = index_type(w);
          } else if (stack.is_right(k - 2)) {
            deref(stack.get_index(k - 2)).right_set_link(w);
          } else {
            deref(stack.get_index(k - 2)).left_set_link(w);
          }
          stack.update_index(k, stack.get_index(k - 1), false);
          stack.update_index(k - 1, w);
          w = deref(stack.get_index(k)).left_get_link();
          ++k;
        }
        if ((deref(w).right_is_thread() ||
             deref(deref(w).right_get_link()).is_black()) &&
            (deref(w).left_is_thread() ||
             deref(deref(w).left_get_link()).is_black())) {
          deref(w).set_red();
        } else {
          if (deref(w).left_is_thread() ||
              deref(deref(w).left_get_link()).is_black()) {
            std::size_t y = deref(w).right_get_link();
            deref(y).set_black();
            deref(w).set_red();
            deref(w).right_set_link(deref(y).left_get_link());
            deref(y).left_set_link(w);
            deref(stack.get_index(k - 1)).left_set_link(y);
            if (deref(y).left_is_thread()) {
              std::size_t z = deref(y).left_get_link();
              deref(y).left_set_child();
              deref(z).right_set_thread();
              deref(z).right_set_link(y);
            }
            w = y;
          }
          if (deref(stack.get_index(k - 1)).is_red()) {
            deref(w).set_red();
          } else {
            deref(w).set_black();
          }
          deref(stack.get_index(k - 1)).set_black();
          deref(deref(w).left_get_link()).set_black();
          deref(stack.get_index(k - 1))
              .left_set_link(deref(w).right_get_link());
          deref(w).right_set_link(stack.get_index(k - 1));
          if (k == 1) {
            root.root.root = index_type(w);
          } else if (stack.is_right(k - 2)) {
            deref(stack.get_index(k - 2)).right_set_link(w);
          } else {
            deref(stack.get_index(k - 2)).left_set_link(w);
          }
          if (deref(w).right_is_thread()) {
            deref(w).right_set_child();
            deref(stack.get_index(k - 1)).left_set_thread();
            deref(stack.get_index(k - 1)).left_set_link(w);
          }
          break;
        }
      }
    }
    if (root.root.root != node_type::nil_sentinel) {
      deref(root.root.root).set_black();
    }
  }
}

template <class cast_t, class from_t>
cast_t threaded_rbtree_strict_aliasing_cast(from_t from) {
  static_assert(std::is_pointer<from_t>::value, "from type must be pointer");
  static_assert(std::is_pointer<cast_t>::value, "cast type must be pointer");
  union {
    from_t from;
    cast_t cast;
  } conv;
  conv.from = from;
  return conv.cast;
}

template <class config_t>
class threaded_rbtree_impl {
 public:
  typedef typename config_t::key_type key_type;
  typedef typename config_t::mapped_type mapped_type;
  typedef typename config_t::value_type value_type;
  typedef typename config_t::storage_type storage_type;
  typedef typename config_t::container_type container_type;
  typedef std::size_t size_type;
  typedef std::ptrdiff_t difference_type;
  typedef typename config_t::key_compare key_compare;
  typedef value_type &reference;
  typedef value_type const &const_reference;
  typedef value_type *pointer;
  typedef value_type const *const_pointer;

 protected:
  typedef typename config_t::node_type node_type;
  typedef typename node_type::index_type index_type;

  typedef threaded_rbtree_root_t<node_type, std::true_type, std::true_type>
      root_node_t;

  static size_type constexpr stack_max_depth = sizeof(index_type) * 16 - 3;

  struct root_t : public threaded_rbtree_root_t<node_type, std::true_type,
                                                std::true_type>,
                  public key_compare {
    template <class any_key_compare, class any_container_type>
    root_t(any_key_compare &&_comp, any_container_type &&_container)
        : key_compare(std::forward<any_key_compare>(_comp)),
          container(std::forward<any_container_type>(_container)) {
      free = node_type::nil_sentinel;
    }

    container_type container;
    size_type free;
  };

  struct deref_node_t {
    node_type &operator()(size_type index) const {
      return config_t::get_node(*container_ptr, index);
    }

    container_type *container_ptr;
  };

  struct const_deref_node_t {
    node_type const &operator()(size_type index) const {
      return config_t::get_node(*container_ptr, index);
    }

    container_type const *container_ptr;
  };

  struct deref_key_t {
    key_type const &operator()(size_type index) const {
      return config_t::get_key(config_t::get_value(*container_ptr, index));
    }

    container_type const *container_ptr;
  };

  template <class k_t, class v_t>
  struct get_key_select_t {
    key_type const &operator()(key_type const &value) const { return value; }

    key_type const &operator()(storage_type const &value) const {
      return config_t::get_key(value);
    }

    template <class first_t, class second_t>
    key_type operator()(std::pair<first_t, second_t> const &pair) {
      return key_type(pair.first);
    }

    template <class in_t, class... args_t>
    key_type operator()(in_t const &in, args_t const &... args) const {
      return key_type(in);
    }
  };

  template <class k_t>
  struct get_key_select_t<k_t, k_t> {
    key_type const &operator()(key_type const &value) const {
      return config_t::get_key(value);
    }

    template <class in_t, class... args_t>
    key_type operator()(in_t const &in, args_t const &... args) const {
      return key_type(in, args...);
    }
  };

  typedef get_key_select_t<key_type, storage_type> get_key_t;

  struct key_compare_ex {
    typedef
        typename threded_rb_tree_tools::has_compare<key_compare, key_type>::type
            comparator_3way;

    bool operator()(size_type left, size_type right) const {
      return (*this)(left, right, comparator_3way());
    }
    bool operator()(size_type left, size_type right, std::false_type) const {
      key_compare &compare = *root_ptr;
      auto &left_key =
          config_t::get_key(config_t::get_value(root_ptr->container, left));
      auto &right_key =
          config_t::get_key(config_t::get_value(root_ptr->container, right));
      if (compare(left_key, right_key)) {
        return true;
      } else if (compare(right_key, left_key)) {
        return false;
      } else {
        return left < right;
      }
    }
    bool operator()(size_type left, size_type right, std::true_type) const {
      key_compare &compare = *root_ptr;
      auto &left_key =
          config_t::get_key(config_t::get_value(root_ptr->container, left));
      auto &right_key =
          config_t::get_key(config_t::get_value(root_ptr->container, right));
      int c = compare.compare(left_key, right_key);
      if (c == 0) {
        return left < right;
      }
      return c < 0;
    }

    root_t *root_ptr;
  };

 public:
  class iterator {
   public:
    typedef std::bidirectional_iterator_tag iterator_category;
    typedef typename threaded_rbtree_impl::value_type value_type;
    typedef typename threaded_rbtree_impl::difference_type difference_type;
    typedef typename threaded_rbtree_impl::reference reference;
    typedef typename threaded_rbtree_impl::pointer pointer;

   public:
    explicit iterator(threaded_rbtree_impl *_tree, size_type _where)
        : tree(_tree), where(_where) {}

    iterator(iterator const &) = default;

    iterator &operator++() {
      where = tree->next_i(where);
      return *this;
    }

    iterator &operator--() {
      if (where == node_type::nil_sentinel) {
        where = tree->root_.rbeg_i();
      } else {
        where = tree->prev_i(where);
      }
      return *this;
    }

    iterator operator++(int) {
      iterator save(*this);
      ++*this;
      return save;
    }

    iterator operator--(int) {
      iterator save(*this);
      --*this;
      return save;
    }

    reference operator*() const {
      return *threaded_rbtree_strict_aliasing_cast<pointer>(
          &config_t::get_value(tree->root_.container, where));
    }

    pointer operator->() const {
      return threaded_rbtree_strict_aliasing_cast<pointer>(
          &config_t::get_value(tree->root_.container, where));
    }

    bool operator==(iterator const &other) const {
      assert(tree == other.tree);
      return where == other.where;
    }

    bool operator!=(iterator const &other) const {
      assert(tree == other.tree);
      return where != other.where;
    }

   private:
    friend class threaded_rbtree_impl;

    threaded_rbtree_impl *tree;
    size_type where;
  };

  class const_iterator {
   public:
    typedef std::bidirectional_iterator_tag iterator_category;
    typedef typename threaded_rbtree_impl::value_type value_type;
    typedef typename threaded_rbtree_impl::difference_type difference_type;
    typedef typename threaded_rbtree_impl::reference reference;
    typedef typename threaded_rbtree_impl::const_reference const_reference;
    typedef typename threaded_rbtree_impl::pointer pointer;
    typedef typename threaded_rbtree_impl::const_pointer const_pointer;

   public:
    explicit const_iterator(threaded_rbtree_impl const *_tree, size_type _where)
        : tree(_tree), where(_where) {}

    const_iterator(iterator const &other)
        : tree(other.tree), where(other.where) {}

    const_iterator(const_iterator const &) = default;

    const_iterator &operator++() {
      where = tree->next_i(where);
      return *this;
    }

    const_iterator &operator--() {
      if (where == node_type::nil_sentinel) {
        where = tree->rbeg_i();
      } else {
        where = tree->prev_i(where);
      }
      return *this;
    }

    const_iterator operator++(int) {
      const_iterator save(*this);
      ++*this;
      return save;
    }

    const_iterator operator--(int) {
      const_iterator save(*this);
      --*this;
      return save;
    }

    const_reference operator*() const {
      return *threaded_rbtree_strict_aliasing_cast<const_pointer>(
          &config_t::get_value(tree->root_.container, where));
    }

    const_pointer operator->() const {
      return threaded_rbtree_strict_aliasing_cast<const_pointer>(
          &config_t::get_value(tree->root_.container, where));
    }

    bool operator==(const_iterator const &other) const {
      assert(tree == other.tree);
      return where == other.where;
    }

    bool operator!=(const_iterator const &other) const {
      assert(tree == other.tree);
      return where != other.where;
    }

   private:
    friend class threaded_rbtree_impl;

    threaded_rbtree_impl const *tree;
    size_type where;
  };

  class reverse_iterator {
   public:
    typedef std::bidirectional_iterator_tag iterator_category;
    typedef typename threaded_rbtree_impl::value_type value_type;
    typedef typename threaded_rbtree_impl::difference_type difference_type;
    typedef typename threaded_rbtree_impl::reference reference;
    typedef typename threaded_rbtree_impl::pointer pointer;

   public:
    explicit reverse_iterator(threaded_rbtree_impl *_tree, size_type _where)
        : tree(_tree), where(_where) {}

    explicit reverse_iterator(iterator const &other) : tree(other.tree) {
      if (other.where == node_type::nil_sentinel) {
        where = other.tree->beg_i();
      } else {
        where = other.tree->prev_i(other.where);
      }
    }

    reverse_iterator(reverse_iterator const &) = default;

    reverse_iterator &operator++() {
      where = tree->prev_i(where);
      return *this;
    }

    reverse_iterator &operator--() {
      if (where == node_type::nil_sentinel) {
        where = tree->root_.beg_i();
      } else {
        where = tree->next_i(where);
      }
      return *this;
    }

    reverse_iterator operator++(int) {
      reverse_iterator save(*this);
      ++*this;
      return save;
    }

    reverse_iterator operator--(int) {
      reverse_iterator save(*this);
      --*this;
      return save;
    }

    reference operator*() const {
      return *threaded_rbtree_strict_aliasing_cast<pointer>(
          &config_t::get_value(tree->root_.container, where));
    }

    pointer operator->() const {
      return threaded_rbtree_strict_aliasing_cast<pointer>(
          &config_t::get_value(tree->root_.container, where));
    }

    bool operator==(reverse_iterator const &other) const {
      assert(tree == other.tree);
      return where == other.where;
    }

    bool operator!=(reverse_iterator const &other) const {
      assert(tree == other.tree);
      return where != other.where;
    }

    iterator base() const {
      if (node_type::nil_sentinel == where) {
        return tree->begin();
      }
      return ++iterator(tree, where);
    }

   private:
    friend class threaded_rbtree_impl;

    threaded_rbtree_impl *tree;
    size_type where;
  };

  class const_reverse_iterator {
   public:
    typedef std::bidirectional_iterator_tag iterator_category;
    typedef typename threaded_rbtree_impl::value_type value_type;
    typedef typename threaded_rbtree_impl::difference_type difference_type;
    typedef typename threaded_rbtree_impl::reference reference;
    typedef typename threaded_rbtree_impl::const_reference const_reference;
    typedef typename threaded_rbtree_impl::pointer pointer;
    typedef typename threaded_rbtree_impl::const_pointer const_pointer;

   public:
    explicit const_reverse_iterator(threaded_rbtree_impl const *_tree,
                                    size_type _where)
        : tree(_tree), where(_where) {}

    explicit const_reverse_iterator(const_iterator const &other)
        : tree(other.tree) {
      if (other.where == node_type::nil_sentinel) {
        where = other.tree->beg_i();
      } else {
        where = other.tree->prev_i(other.where);
      }
    }

    const_reverse_iterator(reverse_iterator const &other)
        : tree(other.tree), where(other.where) {}

    const_reverse_iterator(const_reverse_iterator const &) = default;

    const_reverse_iterator &operator++() {
      where = tree->prev_i(where);
      return *this;
    }

    const_reverse_iterator &operator--() {
      if (where == node_type::nil_sentinel) {
        where = tree->root_.beg_i();
      } else {
        where = tree->next_i(where);
      }
      return *this;
    }

    const_reverse_iterator operator++(int) {
      const_reverse_iterator save(*this);
      ++*this;
      return save;
    }

    const_reverse_iterator operator--(int) {
      const_reverse_iterator save(*this);
      --*this;
      return save;
    }

    const_reference operator*() const {
      return *threaded_rbtree_strict_aliasing_cast<const_pointer>(
          &config_t::get_value(tree->root_.container, where));
    }

    const_pointer operator->() const {
      return threaded_rbtree_strict_aliasing_cast<const_pointer>(
          &config_t::get_value(tree->root_.container, where));
    }

    bool operator==(const_reverse_iterator const &other) const {
      assert(tree == other.tree);
      return where == other.where;
    }

    bool operator!=(const_reverse_iterator const &other) const {
      assert(tree == other.tree);
      return where != other.where;
    }

    const_iterator base() const {
      if (node_type::nil_sentinel == where) {
        return tree->cbegin();
      }
      return ++const_iterator(tree, where);
    }

   private:
    friend class threaded_rbtree_impl;

    threaded_rbtree_impl const *tree;
    size_type where;
  };

 public:
  typedef typename std::conditional<config_t::unique_type::value,
                                    std::pair<iterator, bool>, iterator>::type
      insert_result_t;
  typedef std::pair<iterator, bool> pair_ib_t;

 protected:
  typedef std::pair<size_type, bool> pair_posi_t;

  template <class unique_type>
  typename std::enable_if<unique_type::value, insert_result_t>::type result_(
      pair_posi_t posi) {
    return std::make_pair(iterator(this, posi.first), posi.second);
  }

  template <class unique_type>
  typename std::enable_if<!unique_type::value, insert_result_t>::type result_(
      pair_posi_t posi) {
    return iterator(this, posi.first);
  }

 public:
  // empty
  threaded_rbtree_impl() : root_(key_compare(), container_type()) {}

  // empty
  explicit threaded_rbtree_impl(
      key_compare const &comp,
      container_type const &container = container_type())
      : root_(comp, container) {}

  // empty
  explicit threaded_rbtree_impl(container_type const &container)
      : root_(key_compare(), container) {}

  // range
  template <class iterator_t>
  threaded_rbtree_impl(iterator_t iter_begin, iterator_t iter_end,
                       key_compare const &comp = key_compare(),
                       container_type const &container = container_type())
      : root_(comp, container) {
    insert(iter_begin, iter_end);
  }

  // range
  template <class iterator_t>
  threaded_rbtree_impl(iterator_t iter_begin, iterator_t iter_end,
                       container_type const &container)
      : root_(key_compare(), container) {
    insert(iter_begin, iter_end);
  }

  // copy
  threaded_rbtree_impl(threaded_rbtree_impl const &other)
      : root_(other.get_comparator(), container_type()) {
    insert(other.begin(), other.end());
  }

  // copy
  threaded_rbtree_impl(threaded_rbtree_impl const &other,
                       container_type const &container)
      : root_(other.get_comparator(), container) {
    insert(other.begin(), other.end());
  }

  // move
  threaded_rbtree_impl(threaded_rbtree_impl &&other)
      : root_(key_compare(), container_type()) {
    swap(other);
  }

  // move
  threaded_rbtree_impl(threaded_rbtree_impl &&other,
                       container_type const &container)
      : root_(key_compare(), container) {
    insert(std::make_move_iterator(other.begin()),
           std::make_move_iterator(other.end()));
  }

  // initializer list
  threaded_rbtree_impl(std::initializer_list<value_type> il,
                       key_compare const &comp = key_compare(),
                       container_type const &container = container_type())
      : threaded_rbtree_impl(il.begin(), il.end(), comp, container) {}

  // initializer list
  threaded_rbtree_impl(std::initializer_list<value_type> il,
                       container_type const &container)
      : threaded_rbtree_impl(il.begin(), il.end(), key_compare(), container) {}

  // destructor
  ~threaded_rbtree_impl() { trb_clear_<true>(); }

  // copy
  threaded_rbtree_impl &operator=(threaded_rbtree_impl const &other) {
    if (this == &other) {
      return *this;
    }
    trb_clear_<false>();
    insert(other.begin(), other.end());
    return *this;
  }

  // move
  threaded_rbtree_impl &operator=(threaded_rbtree_impl &&other) {
    if (this == &other) {
      return *this;
    }
    std::swap(root_, other.root_);
    return *this;
  }

  // initializer list
  threaded_rbtree_impl &operator=(std::initializer_list<value_type> il) {
    clear();
    insert(il.begin(), il.end());
    return *this;
  }

  void swap(threaded_rbtree_impl &other) { std::swap(root_, other.root_); }

  typedef std::pair<iterator, iterator> pair_ii_t;
  typedef std::pair<const_iterator, const_iterator> pair_cici_t;

  // single element
  insert_result_t insert(value_type const &value) {
    check_max_size_();
    return result_<typename config_t::unique_type>(
        trb_insert_(typename config_t::unique_type(), value));
  }

  // single element
  template <class in_value_t>
  typename std::enable_if<std::is_convertible<in_value_t, value_type>::value,
                          insert_result_t>::type
  insert(in_value_t &&value) {
    check_max_size_();
    return result_<typename config_t::unique_type>(trb_insert_(
        typename config_t::unique_type(), std::forward<in_value_t>(value)));
  }

  // with hint
  iterator insert(const_iterator hint, value_type const &value) {
    check_max_size_();
    return iterator(this,
                    trb_insert_(typename config_t::unique_type(), value).first);
  }

  // with hint
  template <class in_value_t>
  typename std::enable_if<std::is_convertible<in_value_t, value_type>::value,
                          insert_result_t>::type
  insert(const_iterator hint, in_value_t &&value) {
    check_max_size_();
    return result_<typename config_t::unique_type>(trb_insert_(
        typename config_t::unique_type(), std::forward<in_value_t>(value)));
  }

  // range
  template <class iterator_t>
  void insert(iterator_t iter_begin, iterator_t iter_end) {
    for (; iter_begin != iter_end; ++iter_begin) {
      emplace(*iter_begin);
    }
  }

  // initializer list
  void insert(std::initializer_list<value_type> il) {
    insert(il.begin(), il.end());
  }

  // single element
  template <class... args_t>
  insert_result_t emplace(args_t &&... args) {
    check_max_size_();
    return result_<typename config_t::unique_type>(trb_insert_(
        typename config_t::unique_type(), std::forward<args_t>(args)...));
  }

  // with hint
  template <class... args_t>
  insert_result_t emplace_hint(const_iterator hint, args_t &&... args) {
    check_max_size_();
    return result_<typename config_t::unique_type>(trb_insert_(
        typename config_t::unique_type(), std::forward<args_t>(args)...));
  }

  iterator find(key_type const &key) {
    if (config_t::unique_type::value) {
      return iterator(this,
                      threaded_rbtree_equal_unique(
                          root_, const_deref_node_t{&root_.container}, key,
                          deref_key_t{&root_.container}, get_comparator()));
      ;
    } else {
      size_type where = threaded_rbtree_lower_bound(
          root_, const_deref_node_t{&root_.container}, key,
          deref_key_t{&root_.container}, get_comparator());
      return iterator(this, (where == node_type::nil_sentinel ||
                             get_comparator()(key, get_key_(where)))
                                ? node_type::nil_sentinel
                                : where);
    }
  }

  const_iterator find(key_type const &key) const {
    if (config_t::unique_type::value) {
      return const_iterator(
          this, threaded_rbtree_equal_unique(
                    root_, const_deref_node_t{&root_.container}, key,
                    deref_key_t{&root_.container}, get_comparator()));
    } else {
      size_type where = threaded_rbtree_lower_bound(
          root_, const_deref_node_t{&root_.container}, key,
          deref_key_t{&root_.container}, get_comparator());
      return const_iterator(this, (where == node_type::nil_sentinel ||
                                   get_comparator()(key, get_key_(where)))
                                      ? node_type::nil_sentinel
                                      : where);
    };
  }

  iterator erase(const_iterator where) {
    auto index =
        threaded_rbtree_move_next(where.where, deref_node_t{&root_.container});
    trb_erase_index_(where.where);
    return iterator(this, index);
  }

  size_type erase(key_type const &key) {
    size_type erase_count = 0;
    while (trb_erase_key_(key)) {
      ++erase_count;
      if (config_t::unique_type::value) {
        break;
      }
    }
    return erase_count;
  }

  iterator erase(const_iterator erase_begin, const_iterator erase_end) {
    if (erase_begin == cbegin() && erase_end == cend()) {
      clear();
      return begin();
    } else {
      while (erase_begin != erase_end) {
        erase(erase_begin++);
      }
      return iterator(this, erase_begin.where);
    }
  }

  size_type count(key_type const &key) const {
    pair_cici_t range = equal_range(key);
    return std::distance(range.first, range.second);
  }

  size_type approximate_rank(key_type const &key) const {
    double rank_ratio = threaded_rbtree_approximate_rank_ratio(
        root_, const_deref_node_t{&root_.container}, key,
        deref_key_t{&root_.container}, get_comparator());
    return size_type(rank_ratio * size());
  }

  double approximate_rank_ratio(key_type const &key) const {
    return threaded_rbtree_approximate_rank_ratio(
        root_, const_deref_node_t{&root_.container}, key,
        deref_key_t{&root_.container}, get_comparator());
  }

  iterator lower_bound(key_type const &key) {
    return iterator(this, lwb_i(key));
  }

  const_iterator lower_bound(key_type const &key) const {
    return const_iterator(this, lwb_i(key));
  }

  iterator upper_bound(key_type const &key) {
    return iterator(this, upb_i(key));
  }

  const_iterator upper_bound(key_type const &key) const {
    return const_iterator(this, upb_i(key));
  }

  pair_ii_t equal_range(key_type const &key) {
    size_type lower, upper;
    threaded_rbtree_equal_range(root_, const_deref_node_t{&root_.container},
                                key, deref_key_t{&root_.container},
                                get_comparator(), lower, upper);
    return pair_ii_t(iterator(this, lower), iterator(this, upper));
  }

  pair_cici_t equal_range(key_type const &key) const {
    size_type lower, upper;
    threaded_rbtree_equal_range(root_, const_deref_node_t{&root_.container},
                                key, deref_key_t{&root_.container},
                                get_comparator(), lower, upper);
    return pair_cici_t(const_iterator(this, lower),
                       const_iterator(this, upper));
  }

  size_type beg_i() const {
    return root_.get_most_left(const_deref_node_t{&root_.container});
  }

  size_type end_i() const { return node_type::nil_sentinel; }

  size_type rbeg_i() const {
    return root_.get_most_right(const_deref_node_t{&root_.container});
  }

  size_type rend_i() const { return node_type::nil_sentinel; }

  size_type next_i(size_type i) const {
    return threaded_rbtree_move_next(i, const_deref_node_t{&root_.container});
  }

  size_type prev_i(size_type i) const {
    return threaded_rbtree_move_prev(i, const_deref_node_t{&root_.container});
  }

  size_type lwb_i(key_type const &key) const {
    return threaded_rbtree_lower_bound(
        root_, const_deref_node_t{&root_.container}, key,
        deref_key_t{&root_.container}, get_comparator());
  }

  size_type upb_i(key_type const &key) const {
    return threaded_rbtree_upper_bound(
        root_, const_deref_node_t{&root_.container}, key,
        deref_key_t{&root_.container}, get_comparator());
  }

  size_type rlwb_i(key_type const &key) const {
    return threaded_rbtree_reverse_lower_bound(
        root_, const_deref_node_t{&root_.container}, key,
        deref_key_t{&root_.container}, get_comparator());
  }

  size_type rupb_i(key_type const &key) const {
    return threaded_rbtree_reverse_upper_bound(
        root_, const_deref_node_t{&root_.container}, key,
        deref_key_t{&root_.container}, get_comparator());
  }

  key_type const &key_at(size_type i) const {
    return config_t::get_key(config_t::get_value(root_.container, i));
  }

  value_type &elem_at(size_type i) {
    return *threaded_rbtree_strict_aliasing_cast<value_type *>(
        &config_t::get_value(root_.container, i));
  }

  value_type const &elem_at(size_type i) const {
    return *threaded_rbtree_strict_aliasing_cast<value_type const *>(
        &config_t::get_value(root_.container, i));
  }

  iterator begin() { return iterator(this, beg_i()); }

  iterator end() { return iterator(this, end_i()); }

  const_iterator begin() const { return const_iterator(this, beg_i()); }

  const_iterator end() const { return const_iterator(this, end_i()); }

  const_iterator cbegin() const { return const_iterator(this, beg_i()); }

  const_iterator cend() const { return const_iterator(this, end_i()); }

  reverse_iterator rbegin() { return reverse_iterator(this, rbeg_i()); }

  reverse_iterator rend() { return reverse_iterator(this, rend_i()); }

  const_reverse_iterator rbegin() const {
    return const_reverse_iterator(this, rbeg_i());
  }

  const_reverse_iterator rend() const {
    return const_reverse_iterator(this, rend_i());
  }

  const_reverse_iterator crbegin() const {
    return const_reverse_iterator(this, rbeg_i());
  }

  const_reverse_iterator crend() const {
    return const_reverse_iterator(this, rend_i());
  }

  bool empty() const { return root_.root.root == node_type::nil_sentinel; }

  void clear() { trb_clear_<false>(); }

  size_type size() const { return root_.get_count(); }

  size_type max_size() const {
    return std::min<size_type>(root_.container.max_size(),
                               size_type(node_type::nil_sentinel));
  }

  container_type &get_container() { return root_.container; }

  container_type const &get_container() const { return root_.container; }

  key_compare &get_comparator() { return root_; }

  key_compare const &get_comparator() const { return root_; }

 protected:
  root_t root_;

 protected:
  key_type const &get_key_(size_type index) const {
    return config_t::get_key(config_t::get_value(root_.container, index));
  }

  size_type alloc_index_() {
    if (root_.free == node_type::nil_sentinel) {
      return config_t::alloc_index(root_.container);
    }
    auto deref = deref_node_t{&root_.container};
    size_type index = root_.free;
    root_.free = deref(index).left_get_link();
    return index;
  }

  void dealloc_index_(size_type index) {
    auto deref = deref_node_t{&root_.container};
    deref(index).left_set_link(root_.free);
    root_.free = index;
  }

  void check_max_size_() {
    if (size() >= max_size() - 1) {
      throw std::length_error("threaded_rbtree too long");
    }
  }

  template <class arg_first_t, class... args_other_t>
  pair_posi_t trb_insert_(std::false_type, arg_first_t &&arg_first,
                          args_other_t &&... args_other) {
    threaded_rbtree_stack_t<node_type, stack_max_depth> stack;
    size_type index = alloc_index_();
    auto &value = config_t::get_value(root_.container, index);
    ::new (&value) storage_type(std::forward<arg_first_t>(arg_first),
                                std::forward<args_other_t>(args_other)...);
    threaded_rbtree_find_path_for_multi(root_, stack,
                                        deref_node_t{&root_.container}, index,
                                        key_compare_ex{&root_});
    threaded_rbtree_insert(root_, stack, deref_node_t{&root_.container}, index);
    return {index, true};
  }

  template <class arg_first_t, class... args_other_t>
  pair_posi_t trb_insert_(std::true_type, arg_first_t &&arg_first,
                          args_other_t &&... args_other) {
    threaded_rbtree_stack_t<node_type, stack_max_depth> stack;
    auto key = get_key_t()(arg_first, args_other...);
    bool exists = threaded_rbtree_find_path_for_unique(
        root_, stack, deref_node_t{&root_.container}, key,
        deref_key_t{&root_.container}, get_comparator());
    if (exists) {
      return {stack.get_index(stack.height - 1), false};
    }
    size_type index = alloc_index_();
    auto &value = config_t::get_value(root_.container, index);
    ::new (&value) storage_type(std::forward<arg_first_t>(arg_first),
                                std::forward<args_other_t>(args_other)...);
    threaded_rbtree_insert(root_, stack, deref_node_t{&root_.container}, index);
    return {index, true};
  }

  bool trb_erase_key_(key_type const &key) {
    threaded_rbtree_stack_t<node_type, stack_max_depth> stack;
    bool exists = threaded_rbtree_find_path_for_unique(
        root_, stack, deref_node_t{&root_.container}, key,
        deref_key_t{&root_.container}, get_comparator());
    if (!exists) {
      return false;
    }
    auto index = stack.get_index(stack.height - 1);
    threaded_rbtree_remove(root_, stack, deref_node_t{&root_.container});
    auto &value = config_t::get_value(root_.container, index);
    value.~storage_type();
    dealloc_index_(index);
    return true;
  }

  void trb_erase_index_(size_type index) {
    threaded_rbtree_stack_t<node_type, stack_max_depth> stack;
    bool exists = threaded_rbtree_find_path_for_remove(
        root_, stack, deref_node_t{&root_.container}, index,
        key_compare_ex{&root_});
    assert(exists);
    (void)exists;  // for compiler : shut up !
    threaded_rbtree_remove(root_, stack, deref_node_t{&root_.container});
    auto &value = config_t::get_value(root_.container, index);
    value.~storage_type();
    dealloc_index_(index);
  }

  template <bool clear_memory>
  void trb_clear_() {
    auto deref = deref_node_t{&root_.container};
    root_.root.root = node_type::nil_sentinel;
    root_.root.set_left(node_type::nil_sentinel);
    root_.root.set_right(node_type::nil_sentinel);
    if (clear_memory) {
      root_.free = node_type::nil_sentinel;
      for (index_type i = 0; i < root_.get_count(); ++i) {
        if (deref(i).is_used()) {
          config_t::get_value(root_.container, i).~storage_type();
        }
      }
      root_.container.clear();
    } else {
      for (index_type i = 0; i < root_.get_count(); ++i) {
        if (deref(i).is_used()) {
          deref(i).set_empty();
          config_t::get_value(root_.container, i).~storage_type();
          dealloc_index_(i);
        }
      }
    }
    root_.root.reset();
  }
};

template <class key_t, class comparator_t, class index_t, class unique_t>
struct threaded_rbtree_default_set_config_t {
  typedef key_t key_type;
  typedef key_t const mapped_type;
  typedef key_t const value_type;
  typedef key_t storage_type;
  typedef comparator_t key_compare;

  typedef threaded_rbtree_node_t<index_t> node_type;
  struct element_type {
    node_type node;
    uint8_t value[sizeof(storage_type)];
  };
  typedef std::vector<element_type> container_type;
  typedef unique_t unique_type;

  static node_type &get_node(container_type &container, std::size_t index) {
    return container[index].node;
  }

  static node_type const &get_node(container_type const &container,
                                   std::size_t index) {
    return container[index].node;
  }

  static storage_type &get_value(container_type &container, std::size_t index) {
    return *threaded_rbtree_strict_aliasing_cast<storage_type *>(
        container[index].value);
  }

  static storage_type const &get_value(container_type const &container,
                                       std::size_t index) {
    return *threaded_rbtree_strict_aliasing_cast<storage_type const *>(
        container[index].value);
  }

  static std::size_t alloc_index(container_type &container) {
    auto index = container.size();
    container.emplace_back();
    return index;
  }

  template <class in_type>
  static key_type const &get_key(in_type &&value) {
    return value;
  }
};

template <class key_t, class value_t, class comparator_t, class index_t,
          class unique_t>
struct threaded_rbtree_default_map_config_t {
  typedef key_t key_type;
  typedef value_t mapped_type;
  typedef std::pair<key_t const, value_t> value_type;
  typedef std::pair<key_t, value_t> storage_type;
  typedef comparator_t key_compare;

  typedef threaded_rbtree_node_t<index_t> node_type;
  struct element_type {
    node_type node;
    uint8_t value[sizeof(storage_type)];
  };
  typedef std::vector<element_type> container_type;
  typedef unique_t unique_type;

  static node_type &get_node(container_type &container, std::size_t index) {
    return container[index].node;
  }

  static node_type const &get_node(container_type const &container,
                                   std::size_t index) {
    return container[index].node;
  }

  static storage_type &get_value(container_type &container, std::size_t index) {
    return reinterpret_cast<storage_type &>(container[index].value);
  }

  static storage_type const &get_value(container_type const &container,
                                       std::size_t index) {
    return reinterpret_cast<storage_type const &>(container[index].value);
  }

  static std::size_t alloc_index(container_type &container) {
    auto index = container.size();
    container.emplace_back();
    return index;
  }

  template <class in_type>
  static key_type const &get_key(in_type &&value) {
    return value.first;
  }
};

template <class key_t, class comparator_t = std::less<key_t>,
          class index_t = uint32_t>
using trb_set = threaded_rbtree_impl<threaded_rbtree_default_set_config_t<
    key_t, comparator_t, index_t, std::true_type>>;

template <class key_t, class comparator_t = std::less<key_t>,
          class index_t = uint32_t>
using trb_multiset = threaded_rbtree_impl<threaded_rbtree_default_set_config_t<
    key_t, comparator_t, index_t, std::false_type>>;

template <class key_t, class value_t, class comparator_t = std::less<key_t>,
          class index_t = uint32_t>
class trb_map
    : public threaded_rbtree_impl<threaded_rbtree_default_map_config_t<
          key_t, value_t, comparator_t, index_t, std::true_type>> {
  typedef threaded_rbtree_default_map_config_t<key_t, value_t, comparator_t,
                                               index_t, std::true_type>
      config_t;
  typedef threaded_rbtree_impl<config_t> base_t;

 public:
  // explicit
  explicit trb_map(typename base_t::key_compare const &comp,
                   typename base_t::container_type const &container =
                       typename base_t::container_type())
      : base_t(comp, container) {}
  explicit trb_map(typename base_t::container_type const &container)
      : base_t(container) {}
  template <class... args_t>
  trb_map(args_t &&... args) : base_t(std::forward<args_t>(args)...) {}
  template <class... args_t>
  trb_map(std::initializer_list<typename base_t::value_type> il,
          args_t &&... args)
      : base_t(il, std::forward<args_t>(args)...) {}

  typename base_t::mapped_type &operator[](
      typename base_t::key_type const &key) {
    typename base_t::size_type offset = base_t::lwb_i(key);
    if (offset == base_t::node_type::nil_sentinel ||
        base_t::get_comparator()(key, typename base_t::deref_key_t{
                                          &base_t::root_.container}(offset))) {
      offset = base_t::trb_insert_(std::false_type(), key,
                                   typename base_t::mapped_type())
                   .first;
    }
    return config_t::get_value(base_t::root_.container, offset).second;
  }
};

template <class key_t, class value_t, class comparator_t = std::less<key_t>,
          class index_t = uint32_t>
using trb_multimap = threaded_rbtree_impl<threaded_rbtree_default_map_config_t<
    key_t, value_t, comparator_t, index_t, std::false_type>>;
