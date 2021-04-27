#pragma once

#include "chash.h"


template<class key_t, class value_t, class unique_t, class hasher_t, class key_equal_t, class allocator_t>
struct chash_map_config_t
{
    typedef key_t key_type;
    typedef value_t mapped_type;
    typedef std::pair<key_t const, value_t> value_type;
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
        return value.first;
    }
};
template<class key_t, class value_t, class hasher_t = std::hash<key_t>, class key_equal_t = std::equal_to<key_t>, class allocator_t = std::allocator<std::pair<key_t const, value_t>>>
using chash_map = contiguous_hash<chash_map_config_t<key_t, value_t, std::true_type, hasher_t, key_equal_t, allocator_t>>;
template<class key_t, class value_t, class hasher_t = std::hash<key_t>, class key_equal_t = std::equal_to<key_t>, class allocator_t = std::allocator<std::pair<key_t const, value_t>>>
using chash_multimap = contiguous_hash<chash_map_config_t<key_t, value_t, std::false_type, hasher_t, key_equal_t, allocator_t>>;