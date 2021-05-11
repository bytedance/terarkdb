#pragma once

#include "chash.h"


template<class key_t, class unique_t, class hasher_t, class key_equal_t, class allocator_t>
struct chash_set_config_t
{
    typedef key_t key_type;
    typedef key_t mapped_type;
    typedef key_t value_type;
    typedef hasher_t hasher;
    typedef key_equal_t key_equal;
    typedef allocator_t allocator_type;
    typedef std::uintptr_t offset_type;
    typedef typename std::result_of<hasher(key_type)>::type hash_value_type;
    typedef unique_t unique_type;
    static float grow_proportion(std::size_t)
    {
        return 2;
    }
    template<class in_type> static key_type const &get_key(in_type &&value)
    {
        return value;
    }
};
template<class key_t, class hasher_t = std::hash<key_t>, class key_equal_t = std::equal_to<key_t>, class allocator_t = std::allocator<key_t>>
using chash_set = contiguous_hash<chash_set_config_t<key_t, std::true_type, hasher_t, key_equal_t, allocator_t>>;
template<class key_t, class hasher_t = std::hash<key_t>, class key_equal_t = std::equal_to<key_t>, class allocator_t = std::allocator<key_t>>
using chash_multiset = contiguous_hash<chash_set_config_t<key_t, std::false_type, hasher_t, key_equal_t, allocator_t>>;