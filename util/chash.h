#pragma once

#include <cstdint>
#include <algorithm>
#include <utility>
#include <memory>
#include <cstring>
#include <stdexcept>
#include <functional>
#include <cmath>
#include <type_traits>

#if defined(__GNUC__)
#   pragma GCC diagnostic push
#   pragma GCC diagnostic ignored "-Wpragmas"
#   if defined(__GNUC__) && __GNUC__ * 1000 + __GNUC_MINOR__ >= 8000
#       pragma GCC diagnostic ignored "-Wclass-memaccess"
#   endif
#endif

namespace contiguous_hash_detail
{
    class move_trivial_tag
    {
    };
    class move_assign_tag
    {
    };
    template<class T> struct is_trivial_expand : public std::is_trivial<T>
    {
    };
    template<class K, class V> struct is_trivial_expand<std::pair<K, V>> : public std::conditional<std::is_trivial<K>::value && std::is_trivial<V>::value, std::true_type, std::false_type>::type
    {
    };
    template<class iterator_t> struct get_tag
    {
        typedef typename std::conditional<is_trivial_expand<typename std::iterator_traits<iterator_t>::value_type>::value, move_trivial_tag, move_assign_tag>::type type;
    };

    template<class iterator_t, class tag_t, class ...args_t> void construct_one(iterator_t where, tag_t, args_t &&...args)
    {
        typedef typename std::iterator_traits<iterator_t>::value_type iterator_value_t;
        ::new(std::addressof(*where)) iterator_value_t(std::forward<args_t>(args)...);
    }

    template<class iterator_t> void destroy_one(iterator_t where, move_trivial_tag)
    {
    }
    template<class iterator_t> void destroy_one(iterator_t where, move_assign_tag)
    {
        typedef typename std::iterator_traits<iterator_t>::value_type iterator_value_t;
        where->~iterator_value_t();
    }

    template<class iterator_from_t, class iterator_to_t> void move_construct_and_destroy(iterator_from_t move_begin, iterator_from_t move_end, iterator_to_t to_begin, move_trivial_tag)
    {
        std::ptrdiff_t count = move_end - move_begin;
        std::memmove(&*to_begin, &*move_begin, count * sizeof(*move_begin));
    }
    template<class iterator_from_t, class iterator_to_t> void move_construct_and_destroy(iterator_from_t move_begin, iterator_from_t move_end, iterator_to_t to_begin, move_assign_tag)
    {
        for(; move_begin != move_end; ++move_begin)
        {
            construct_one(to_begin++, move_assign_tag(), std::move(*move_begin));
            destroy_one(move_begin, move_assign_tag());
        }
    }
}

template<class config_t>
class contiguous_hash
{
public:
    typedef typename config_t::key_type key_type;
    typedef typename config_t::mapped_type mapped_type;
    typedef typename config_t::value_type value_type;
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;
    typedef typename config_t::hasher hasher;
    typedef typename config_t::key_equal key_equal;
    typedef typename config_t::allocator_type allocator_type;
    typedef typename config_t::offset_type offset_type;
    typedef typename config_t::hash_value_type hash_value_type;
    typedef value_type &reference;
    typedef value_type const &const_reference;
    typedef value_type *pointer;
    typedef value_type const *const_pointer;

    static constexpr offset_type offset_empty = offset_type(-1);

protected:
    struct hash_t
    {
        hash_value_type hash;
        hash_t()
        {
        }
        hash_t(hash_t const &) = default;
        hash_t(hash_value_type value)
        {
            hash = value & ~(hash_value_type(1) << (sizeof(hash_value_type) * 8 - 1));
        }
        hash_t &operator = (hash_t const &) = default;

        template<class any_type> any_type operator % (any_type const &value) const
        {
            return hash % value;
        }
        bool operator == (hash_t const &other) const
        {
            return hash == other.hash;
        }
        bool operator !() const
        {
            return hash == ~hash_value_type(0);
        }
        operator bool() const
        {
            return hash != ~hash_value_type(0);
        }
        void clear()
        {
            hash = ~hash_value_type(0);
        }
    };
    struct index_t
    {
        hash_t hash;
        offset_type next;
        offset_type prev;
    };
    struct value_t
    {
        typename std::aligned_storage<sizeof(value_type), std::alignment_of<value_type>::value>::type value_pod;

        value_type *value()
        {
            return reinterpret_cast<value_type *>(&value_pod);
        }
        value_type const *value() const
        {
            return reinterpret_cast<value_type const *>(&value_pod);
        }
    };

    typedef typename allocator_type::template rebind<offset_type>::other bucket_allocator_t;
    typedef typename allocator_type::template rebind<index_t>::other index_allocator_t;
    typedef typename allocator_type::template rebind<value_t>::other value_allocator_t;
    struct root_t : public hasher, public key_equal, public bucket_allocator_t, public index_allocator_t, public value_allocator_t
    {
        template<class any_hasher, class any_key_equal, class any_allocator_type> root_t(any_hasher &&hash, any_key_equal &&equal, any_allocator_type &&alloc)
            : hasher(std::forward<any_hasher>(hash))
            , key_equal(std::forward<any_key_equal>(equal))
            , bucket_allocator_t(alloc)
            , index_allocator_t(alloc)
            , value_allocator_t(std::forward<any_allocator_type>(alloc))
        {
            static_assert(std::is_unsigned<offset_type>::value && std::is_integral<offset_type>::value, "offset_type must be unsighed integer");
            static_assert(sizeof(offset_type) <= sizeof(contiguous_hash::size_type), "offset_type too big");
            static_assert(std::is_integral<hash_value_type>::value, "hash_value_type must be integer");
            bucket_count = 0;
            capacity = 0;
            size = 0;
            free_count = 0;
            free_list = offset_empty;
            setting_load_factor = 1;
            bucket = nullptr;
            index = nullptr;
            value = nullptr;
        }
        typename contiguous_hash::size_type bucket_count;
        typename contiguous_hash::size_type capacity;
        typename contiguous_hash::size_type size;
        typename contiguous_hash::size_type free_count;
        offset_type free_list;
        float setting_load_factor;
        offset_type *bucket;
        index_t *index;
        value_t *value;
    };
    template<class k_t, class v_t> struct get_key_select_t
    {
        key_type const &operator()(key_type const &value)
        {
            return value;
        }
        key_type const &operator()(value_type const &value)
        {
            return config_t::get_key(value);
        }
        template<class ...args_t> key_type const &operator()(key_type const &in, args_t &&...args)
        {
            return (*this)(in);
        }
    };
    template<class k_t> struct get_key_select_t<k_t, k_t>
    {
        key_type const &operator()(key_type const &value)
        {
            return config_t::get_key(value);
        }
        template<class in_t, class ...args_t> key_type operator()(in_t const &in, args_t const &...args)
        {
            return key_type(in, args...);
        }
    };
    typedef get_key_select_t<key_type, value_type> get_key_t;
public:
    class iterator
    {
    public:
        typedef std::forward_iterator_tag iterator_category;
        typedef typename contiguous_hash::value_type value_type;
        typedef typename contiguous_hash::difference_type difference_type;
        typedef typename contiguous_hash::reference reference;
        typedef typename contiguous_hash::pointer pointer;
    public:
        iterator(size_type _offset, contiguous_hash const *_self) : offset(_offset), self(_self)
        {
        }
        iterator(iterator const &) = default;
        iterator &operator++()
        {
            offset = self->advance_next_(offset);
            return *this;
        }
        iterator operator++(int)
        {
            iterator save(*this);
            ++*this;
            return save;
        }
        reference operator *() const
        {
            return *self->root_.value[offset].value();
        }
        pointer operator->() const
        {
            return self->root_.value[offset].value();
        }
        bool operator == (iterator const &other) const
        {
            return offset == other.offset && self == other.self;
        }
        bool operator != (iterator const &other) const
        {
            return offset != other.offset || self != other.self;
        }
        size_type pos() const
        {
            return offset;
        }
    private:
        friend class contiguous_hash;
        size_type offset;
        contiguous_hash const *self;
    };
    class const_iterator
    {
    public:
        typedef std::forward_iterator_tag iterator_category;
        typedef typename contiguous_hash::value_type value_type;
        typedef typename contiguous_hash::difference_type difference_type;
        typedef typename contiguous_hash::reference reference;
        typedef typename contiguous_hash::const_reference const_reference;
        typedef typename contiguous_hash::pointer pointer;
        typedef typename contiguous_hash::const_pointer const_pointer;
    public:
        const_iterator(size_type _offset, contiguous_hash const *_self) : offset(_offset), self(_self)
        {
        }
        const_iterator(const_iterator const &) = default;
        const_iterator(iterator const &it) : offset(it.offset), self(it.self)
        {
        }
        const_iterator &operator++()
        {
            offset = self->advance_next_(offset);
            return *this;
        }
        const_iterator operator++(int)
        {
            const_iterator save(*this);
            ++*this;
            return save;
        }
        const_reference operator *() const
        {
            return *self->root_.value[offset].value();
        }
        const_pointer operator->() const
        {
            return self->root_.value[offset].value();
        }
        bool operator == (const_iterator const &other) const
        {
            return offset == other.offset && self == other.self;
        }
        bool operator != (const_iterator const &other) const
        {
            return offset != other.offset || self != other.self;
        }
        size_type pos() const
        {
            return offset;
        }
    private:
        friend class contiguous_hash;
        size_type offset;
        contiguous_hash const *self;
    };
    class local_iterator
    {
    public:
        typedef std::forward_iterator_tag iterator_category;
        typedef typename contiguous_hash::value_type value_type;
        typedef typename contiguous_hash::difference_type difference_type;
        typedef typename contiguous_hash::reference reference;
        typedef typename contiguous_hash::pointer pointer;
    public:
        local_iterator(size_type _offset, contiguous_hash const *_self) : offset(_offset), self(_self)
        {
        }
        local_iterator(local_iterator const &) = default;
        local_iterator &operator++()
        {
            offset = self->local_advance_next_(offset);
            return *this;
        }
        local_iterator operator++(int)
        {
            local_iterator save(*this);
            ++*this;
            return save;
        }
        reference operator *() const
        {
            return *self->root_.value[offset].value();
        }
        pointer operator->() const
        {
            return self->root_.value[offset].value();
        }
        bool operator == (local_iterator const &other) const
        {
            return offset == other.offset && self == other.self;
        }
        bool operator != (local_iterator const &other) const
        {
            return offset != other.offset || self != other.self;
        }
        size_type pos() const
        {
            return offset;
        }
    private:
        friend class contiguous_hash;
        size_type offset;
        contiguous_hash const *self;
    };
    class const_local_iterator
    {
    public:
        typedef std::forward_iterator_tag iterator_category;
        typedef typename contiguous_hash::value_type value_type;
        typedef typename contiguous_hash::difference_type difference_type;
        typedef typename contiguous_hash::reference reference;
        typedef typename contiguous_hash::const_reference const_reference;
        typedef typename contiguous_hash::pointer pointer;
        typedef typename contiguous_hash::const_pointer const_pointer;
    public:
        const_local_iterator(size_type _offset, contiguous_hash const *_self) : offset(_offset), self(_self)
        {
        }
        const_local_iterator(const_local_iterator const &) = default;
        const_local_iterator(local_iterator const &it) : offset(it.offset), self(it.self)
        {
        }
        const_local_iterator &operator++()
        {
            offset = self->local_advance_next_(offset);
            return *this;
        }
        const_local_iterator operator++(int)
        {
            const_local_iterator save(*this);
            ++*this;
            return save;
        }
        const_reference operator *() const
        {
            return *self->root_.value[offset].value();
        }
        const_pointer operator->() const
        {
            return self->root_.value[offset].value();
        }
        bool operator == (const_local_iterator const &other) const
        {
            return offset == other.offset && self == other.self;
        }
        bool operator != (const_local_iterator const &other) const
        {
            return offset != other.offset || self != other.self;
        }
        size_type pos() const
        {
            return offset;
        }
    private:
        friend class contiguous_hash;
        size_type offset;
        contiguous_hash const *self;
    };
    typedef typename std::conditional<config_t::unique_type::value, std::pair<iterator, bool>, iterator>::type insert_result_t;
    typedef std::pair<iterator, bool> pair_ib_t;
protected:
    typedef std::pair<size_type, bool> pair_posi_t;
    template<class unique_type> typename std::enable_if<unique_type::value, insert_result_t>::type result_(pair_posi_t posi)
    {
        return std::make_pair(iterator(posi.first, this), posi.second);
    }
    template<class unique_type> typename std::enable_if<!unique_type::value, insert_result_t>::type result_(pair_posi_t posi)
    {
        return iterator(posi.first, this);
    }

public:
    //empty
    contiguous_hash() : root_(hasher(), key_equal(), allocator_type())
    {
    }
    //empty
    explicit contiguous_hash(size_type bucket_count, hasher const &hash = hasher(), key_equal const &equal = key_equal(), allocator_type const &alloc = allocator_type()) : root_(hash, equal, alloc)
    {
        rehash(bucket_count);
    }
    //empty
    explicit contiguous_hash(allocator_type const &alloc) : root_(hasher(), key_equal(), alloc)
    {
    }
    //empty
    contiguous_hash(size_type bucket_count, allocator_type const &alloc) : root_(hasher(), key_equal(), alloc)
    {
        rehash(bucket_count);
    }
    //empty
    contiguous_hash(size_type bucket_count, hasher const &hash, allocator_type const &alloc) : root_(hash, key_equal(), alloc)
    {
        rehash(bucket_count);
    }
    //range
    template <class iterator_t> contiguous_hash(iterator_t begin, iterator_t end, size_type bucket_count = 8, hasher const &hash = hasher(), key_equal const &equal = key_equal(), allocator_type const &alloc = allocator_type()) : root_(hash, equal, alloc)
    {
        rehash(bucket_count);
        insert(begin, end);
    }
    //range
    template <class iterator_t> contiguous_hash(iterator_t begin, iterator_t end, size_type bucket_count, allocator_type const &alloc) : root_(hasher(), key_equal(), alloc)
    {
        rehash(bucket_count);
        insert(begin, end);
    }
    //range
    template <class iterator_t> contiguous_hash(iterator_t begin, iterator_t end, size_type bucket_count, hasher const &hash, allocator_type const &alloc) : root_(hash, key_equal(), alloc)
    {
        rehash(bucket_count);
        insert(begin, end);
    }
    //copy
    contiguous_hash(contiguous_hash const &other) : root_(other.get_hasher(), other.get_key_equal(), other.get_value_allocator_())
    {
        copy_all_<false>(&other.root_);
    }
    //copy
    contiguous_hash(contiguous_hash const &other, allocator_type const &alloc) : root_(other.get_hasher(), other.get_key_equal(), alloc)
    {
        copy_all_<false>(&other.root_);
    }
    //move
    contiguous_hash(contiguous_hash &&other) : root_(hasher(), key_equal(), value_allocator_t())
    {
        swap(other);
    }
    //move
    contiguous_hash(contiguous_hash &&other, allocator_type const &alloc) : root_(std::move(other.get_hasher()), std::move(other.get_key_equal()), alloc)
    {
        copy_all_<true>(&other.root_);
    }
    //initializer list
    contiguous_hash(std::initializer_list<value_type> il, size_type bucket_count = 8, hasher const &hash = hasher(), key_equal const &equal = key_equal(), allocator_type const &alloc = allocator_type()) : contiguous_hash(il.begin(), il.end(), std::distance(il.begin(), il.end()), hash, equal, alloc)
    {
    }
    //initializer list
    contiguous_hash(std::initializer_list<value_type> il, size_type bucket_count, allocator_type const &alloc) : contiguous_hash(il.begin(), il.end(), std::distance(il.begin(), il.end()), alloc)
    {
    }
    //initializer list
    contiguous_hash(std::initializer_list<value_type> il, size_type bucket_count, hasher const &hash, allocator_type const &alloc) : contiguous_hash(il.begin(), il.end(), std::distance(il.begin(), il.end()), hash, alloc)
    {
    }
    //destructor
    ~contiguous_hash()
    {
        dealloc_all_();
    }
    //copy
    contiguous_hash &operator = (contiguous_hash const &other)
    {
        if(this == &other)
        {
            return *this;
        }
        dealloc_all_();
        get_hasher() = other.get_hasher();
        get_key_equal() = other.get_key_equal();
        get_bucket_allocator_() = other.get_bucket_allocator_();
        get_index_allocator_() = other.get_index_allocator_();
        get_value_allocator_() = other.get_value_allocator_();
        copy_all_<false>(&other.root_);
        return *this;
    }
    //move
    contiguous_hash &operator = (contiguous_hash &&other)
    {
        if(this == &other)
        {
            return *this;
        }
        swap(other);
        return *this;
    }
    //initializer list
    contiguous_hash &operator = (std::initializer_list<value_type> il)
    {
        clear();
        rehash(std::distance(il.begin(), il.end()));
        insert(il.begin(), il.end());
        return *this;
    }

    allocator_type get_allocator() const
    {
        return *static_cast<value_allocator_t const *>(&root_);
    }
    hasher hash_function() const
    {
        return *static_cast<hasher const *>(&root_);
    }
    key_equal key_eq() const
    {
        return *static_cast<key_equal const *>(&root_);
    }

    void swap(contiguous_hash &other)
    {
        std::swap(root_, other.root_);
    }

    typedef std::pair<iterator, iterator> pair_ii_t;
    typedef std::pair<const_iterator, const_iterator> pair_cici_t;
    typedef std::pair<local_iterator, local_iterator> pair_lili_t;
    typedef std::pair<const_local_iterator, const_local_iterator> pair_clicli_t;

    //single element
    insert_result_t insert(value_type const &value)
    {
        return result_<typename config_t::unique_type>(insert_value_(value));
    }
    //single element
    template<class in_value_t> typename std::enable_if<std::is_convertible<in_value_t, value_type>::value, insert_result_t>::type insert(in_value_t &&value)
    {
        return result_<typename config_t::unique_type>(insert_value_(std::forward<in_value_t>(value)));
    }
    //with hint
    iterator insert(const_iterator hint, value_type const &value)
    {
        return result_<typename config_t::unique_type>(insert_value_(value));
    }
    //with hint
    template<class in_value_t> typename std::enable_if<std::is_convertible<in_value_t, value_type>::value, insert_result_t>::type insert(const_iterator hint, in_value_t &&value)
    {
        return result_<typename config_t::unique_type>(insert_value_(std::forward<in_value_t>(value)));
    }
    //range
    template<class iterator_t> void insert(iterator_t begin, iterator_t end)
    {
        for(; begin != end; ++begin)
        {
            emplace_hint(cend(), *begin);
        }
    }
    //initializer list
    void insert(std::initializer_list<value_type> il)
    {
        insert(il.begin(), il.end());
    }

    //single element
    template<class ...args_t> insert_result_t emplace(args_t &&...args)
    {
        return result_<typename config_t::unique_type>(insert_value_(std::forward<args_t>(args)...));
    }
    //with hint
    template<class ...args_t> insert_result_t emplace_hint(const_iterator hint, args_t &&...args)
    {
        return result_<typename config_t::unique_type>(insert_value_(std::forward<args_t>(args)...));
    }

    template<class in_key_t> iterator find(in_key_t const &key)
    {
        if(root_.size == 0)
        {
            return end();
        }
        return iterator(find_value_(key), this);
    }
    template<class in_key_t> const_iterator find(in_key_t const &key) const
    {
        if(root_.size == 0)
        {
            return cend();
        }
        return const_iterator(find_value_(key), this);
    }

    template<class in_key_t, class = typename std::enable_if<std::is_convertible<in_key_t, key_type>::value && config_t::unique_type::value && !std::is_same<key_type, value_type>::value, void>::type> mapped_type &at(in_key_t const &key)
    {
        offset_type offset = root_.size;
        if(root_.size != 0)
        {
            offset = find_value_(key);
        }
        if(offset == root_.size)
        {
            throw std::out_of_range("contiguous_hash out of range");
        }
        return root_.value[offset].value()->second;
    }
    template<class in_key_t, class = typename std::enable_if<std::is_convertible<in_key_t, key_type>::value && config_t::unique_type::value && !std::is_same<key_type, value_type>::value, void>::type> mapped_type const &at(in_key_t const &key) const
    {
        offset_type offset = root_.size;
        if(root_.size != 0)
        {
            offset = find_value_(key);
        }
        if(offset == root_.size)
        {
            throw std::out_of_range("contiguous_hash out of range");
        }
        return root_.value[offset].value()->second;
    }

    template<class in_key_t, class = typename std::enable_if<std::is_convertible<in_key_t, key_type>::value && config_t::unique_type::value && !std::is_same<key_type, value_type>::value, void>::type> mapped_type &operator[](in_key_t &&key)
    {
        offset_type offset = root_.size;
        if(root_.size != 0)
        {
            offset = find_value_(key);
        }
        if(offset == root_.size)
        {
            offset = insert_value_(key, mapped_type()).first;
        }
        return root_.value[offset].value()->second;
    }

    iterator erase(const_iterator it)
    {
        if(root_.size == 0)
        {
            return end();
        }
        remove_offset_(it.offset);
        return iterator(advance_next_(it.offset), this);
    }
    local_iterator erase(const_local_iterator it)
    {
        if(root_.size == 0)
        {
            return local_iterator(offset_empty, this);
        }
        size_type next = local_advance_next_(offset_type(it.offset));
        remove_offset_(it.offset);
        return local_iterator(next, this);
    }
    size_type erase(key_type const &key)
    {
        if(root_.size == 0)
        {
            return 0;
        }
        return remove_value_(typename config_t::unique_type(), key);
    }
    iterator erase(const_iterator erase_begin, const_iterator erase_end)
    {
        if(erase_begin == cbegin() && erase_end == cend())
        {
            clear();
            return end();
        }
        else
        {
            while(erase_begin != erase_end)
            {
                erase(erase_begin++);
            }
            return iterator(erase_begin.offset, this);
        }
    }
    local_iterator erase(const_local_iterator erase_begin, const_local_iterator erase_end)
    {
        while(erase_begin != erase_end)
        {
            erase(erase_begin++);
        }
        return local_iterator(erase_begin.offset, this);
    }

    size_type count(key_type const &key) const
    {
        auto where = find(key);
        if (where == end())
        {
            return 0;
        }
        if (config_t::unique_type::value)
        {
            return 1;
        }
        else
        {
            return local_find_equal_(where.offset).second;
        }
    }

    template<class in_key_t, class = typename std::enable_if<std::is_convertible<in_key_t, key_type>::value && config_t::unique_type::value, void>::type> pair_ii_t equal_range(in_key_t const &key)
    {
        auto where = find(key);
        if(where == end())
        {
            return std::make_pair(end(), end());
        }
        else
        {
            return std::make_pair(where, iterator(advance_next_(where.offset), this));
        }
    }
    template<class in_key_t, class = typename std::enable_if<std::is_convertible<in_key_t, key_type>::value && config_t::unique_type::value, void>::type> pair_cici_t equal_range(in_key_t const &key) const
    {
        auto where = find(key);
        if(where == cend())
        {
            return std::make_pair(cend(), cend());
        }
        else
        {
            return std::make_pair(where, const_iterator(advance_next_(where.offset), this));
        }
    }
    template<class in_key_t, class = typename std::enable_if<std::is_convertible<in_key_t, key_type>::value && !config_t::unique_type::value, void>::type> pair_lili_t equal_range(in_key_t const &key)
    {
        auto where = find(key);
        if(where == end())
        {
            return std::make_pair(local_iterator(offset_empty, this), local_iterator(offset_empty, this));
        }
        else
        {
            return std::make_pair(local_iterator(where.offset, this), local_iterator(local_find_equal_(where.offset).first, this));
        }
    }
    template<class in_key_t, class = typename std::enable_if<std::is_convertible<in_key_t, key_type>::value && !config_t::unique_type::value, void>::type> pair_clicli_t equal_range(in_key_t const &key) const
    {
        auto where = find(key);
        if(where == end())
        {
            return std::make_pair(const_local_iterator(offset_empty, this), const_local_iterator(offset_empty, this));
        }
        else
        {
            return std::make_pair(const_local_iterator(where.offset, this), const_local_iterator(local_find_equal_(where.offset).first, this));
        }
    }

    iterator begin()
    {
        return iterator(find_begin_(), this);
    }
    iterator end()
    {
        return iterator(root_.size, this);
    }
    iterator pos(size_type pos)
    {
        return iterator(pos, this);
    }
    const_iterator begin() const
    {
        return const_iterator(find_begin_(), this);
    }
    const_iterator end() const
    {
        return const_iterator(root_.size, this);
    }
    const_iterator pos(size_type pos) const
    {
        return const_iterator(pos, this);
    }
    const_iterator cbegin() const
    {
        return const_iterator(find_begin_(), this);
    }
    const_iterator cend() const
    {
        return const_iterator(root_.size, this);
    }
    const_iterator cpos(size_type pos) const
    {
        return const_iterator(pos, this);
    }

    bool empty() const
    {
        return root_.size == root_.free_count;
    }
    void clear()
    {
        clear_all_();
    }
    size_type size() const
    {
        return root_.size - root_.free_count;
    }
    size_type max_size() const
    {
        return offset_empty - 1;
    }

    local_iterator begin(size_type n)
    {
        return local_iterator(root_.bucket[n], this);
    }
    local_iterator end(size_type n)
    {
        return local_iterator(offset_empty, this);
    }
    const_local_iterator begin(size_type n) const
    {
        return const_local_iterator(root_.bucket[n], this);
    }
    const_local_iterator end(size_type n) const
    {
        return const_local_iterator(offset_empty, this);
    }
    const_local_iterator cbegin(size_type n) const
    {
        return const_local_iterator(root_.bucket[n], this);
    }
    const_local_iterator cend(size_type n) const
    {
        return const_local_iterator(offset_empty, this);
    }

    size_type bucket_count() const
    {
        return root_.bucket_count;
    }
    size_type max_bucket_count() const
    {
        return max_size();
    }

    size_type bucket_size(size_type n) const
    {
        size_type step = 0;
        for(size_type i = root_.bucket[n]; i != offset_empty; i = root_.index[i].next)
        {
            ++step;
        }
        return step;
    }

    size_type bucket(key_type const &key) const
    {
        if(root_.size == 0)
        {
            return 0;
        }
        return hash_t(get_hasher()(key)) % root_.bucket_count;
    }

    void reserve(size_type count)
    {
        rehash(size_type(std::ceil(count / root_.setting_load_factor)));
        if(count > root_.capacity && root_.capacity <= max_size())
        {
            realloc_(size_type(std::ceil(std::max<float>(float(count), root_.bucket_count * root_.setting_load_factor))));
        }
    }
    void rehash(size_type count)
    {
        rehash_(typename config_t::unique_type(), std::max<size_type>({8, count, size_type(std::ceil(size() / root_.setting_load_factor))}));
    }

    void max_load_factor(float ml)
    {
        if(ml <= 0)
        {
            return;
        }
        root_.setting_load_factor = ml;
        if(root_.size != 0)
        {
            rehash_(typename config_t::unique_type(), size_type(std::ceil(size() / root_.setting_load_factor)));
        }
    }
    float max_load_factor() const
    {
        return root_.setting_load_factor;
    }
    float load_factor() const
    {
        if(root_.size == 0)
        {
            return 0;
        }
        return size() / float(root_.bucket_count);
    }

protected:
    root_t root_;

protected:

    hasher &get_hasher()
    {
        return root_;
    }
    hasher const &get_hasher() const
    {
        return root_;
    }

    key_equal &get_key_equal()
    {
        return root_;
    }
    key_equal const &get_key_equal() const
    {
        return root_;
    }

    bucket_allocator_t &get_bucket_allocator_()
    {
        return root_;
    }
    bucket_allocator_t const &get_bucket_allocator_() const
    {
        return root_;
    }
    index_allocator_t &get_index_allocator_()
    {
        return root_;
    }
    index_allocator_t const &get_index_allocator_() const
    {
        return root_;
    }
    value_allocator_t &get_value_allocator_()
    {
        return root_;
    }
    value_allocator_t const &get_value_allocator_() const
    {
        return root_;
    }

    size_type advance_next_(size_type i) const
    {
        for(++i; i < root_.size; ++i)
        {
            if(root_.index[i].hash)
            {
                break;
            }
        }
        return i;
    }

    size_type local_advance_next_(size_type i) const
    {
        return root_.index[i].next;
    }

    std::pair<size_type, size_type> local_find_equal_(size_type i) const
    {
        size_type count = 1;
        hash_t hash = root_.index[i].hash;
        size_type next = root_.index[i].next;
        while(next != offset_empty && root_.index[next].hash == hash && get_key_equal()(get_key_t()(*root_.value[i].value()), get_key_t()(*root_.value[next].value())))
        {
            next = root_.index[next].next;
            ++count;
        }
        return std::make_pair(next, count);
    }

    size_type find_begin_() const
    {
        for(size_type i = 0; i < root_.size; ++i)
        {
            if(root_.index[i].hash)
            {
                return i;
            }
        }
        return root_.size;
    }

    template<class iterator_t, class ...args_t> static void construct_one_(iterator_t where, args_t &&...args)
    {
        contiguous_hash_detail::construct_one(where, typename contiguous_hash_detail::get_tag<iterator_t>::type(), std::forward<args_t>(args)...);
    }

    template<class iterator_t> static void destroy_one_(iterator_t where)
    {
        contiguous_hash_detail::destroy_one(where, typename contiguous_hash_detail::get_tag<iterator_t>::type());
    }

    template<class iterator_from_t, class iterator_to_t> static void move_construct_and_destroy_(iterator_from_t move_begin, iterator_from_t move_end, iterator_to_t to_begin)
    {
        contiguous_hash_detail::move_construct_and_destroy(move_begin, move_end, to_begin, typename contiguous_hash_detail::get_tag<iterator_from_t>::type());
    }

    void dealloc_all_()
    {
        for(size_type i = 0; i < root_.size; ++i)
        {
            if(root_.index[i].hash)
            {
                destroy_one_(root_.value[i].value());
            }
        }
        if(root_.bucket_count != 0)
        {
            get_bucket_allocator_().deallocate(root_.bucket, root_.bucket_count);
        }
        if(root_.capacity != 0)
        {
            get_index_allocator_().deallocate(root_.index, root_.capacity);
            get_value_allocator_().deallocate(root_.value, root_.capacity);
        }
    }

    void clear_all_()
    {
        for(size_type i = 0; i < root_.size; ++i)
        {
            if(root_.index[i].hash)
            {
                destroy_one_(root_.value[i].value());
            }
        }
        if(root_.bucket_count != 0)
        {
            std::memset(root_.bucket, 0xFFFFFFFF, sizeof(offset_type) * root_.bucket_count);
        }
        if(root_.capacity != 0)
        {
            std::memset(root_.index, 0xFFFFFFFF, sizeof(index_t) * root_.capacity);
        }
        root_.size = 0;
        root_.free_count = 0;
        root_.free_list = offset_empty;
    }

    template<bool move> void copy_all_(root_t const *other)
    {
        root_.bucket_count = 0;
        root_.capacity = 0;
        root_.size = 0;
        root_.free_count = 0;
        root_.free_list = offset_empty;
        root_.setting_load_factor = other->setting_load_factor;
        root_.bucket = nullptr;
        root_.index = nullptr;
        root_.value = nullptr;
        size_type size = other->size - other->free_count;
        if(size > 0)
        {
            rehash_(std::true_type(), size);
            realloc_(size);
            for(size_type other_i = 0; other_i < other->size; ++other_i)
            {
                if(other->index[other_i].hash)
                {
                    auto i = root_.size;
                    size_type bucket = other->index[other_i].hash % root_.bucket_count;
                    if(root_.bucket[bucket] != offset_empty)
                    {
                        root_.index[root_.bucket[bucket]].prev = offset_type(i);
                    }
                    root_.index[i].prev = offset_empty;
                    root_.index[i].next = root_.bucket[bucket];
                    root_.index[i].hash = other->index[other_i].hash;
                    root_.bucket[bucket] = offset_type(i);
                    if(move)
                    {
                        construct_one_(root_.value[i].value(), std::move(*other->value[other_i].value()));
                    }
                    else
                    {
                        construct_one_(root_.value[i].value(), *other->value[other_i].value());
                    }
                    ++root_.size;
                }
            }
        }
    }

    static bool is_prime_(size_type candidate)
    {
        if((candidate & 1) != 0)
        {
            size_type limit = size_type(std::sqrt(candidate));
            for(size_type divisor = 3; divisor <= limit; divisor += 2)
            {
                if((candidate % divisor) == 0)
                {
                    return false;
                }
            }
            return true;
        }
        return (candidate == 2);
    }

    static size_type get_prime_(size_type size)
    {
        static size_type const prime_array[] =
        {
            7, 11, 17, 23, 29, 37, 47, 59, 71, 89, 107, 131, 163, 197, 239, 293, 353, 431, 521, 631, 761, 919,
            1103, 1327, 1597, 1931, 2333, 2801, 3371, 4049, 4861, 5839, 7013, 8419, 10103, 12143, 14591,
            17519, 21023, 25229, 30293, 36353, 43627, 52361, 62851, 75431, 90523, 108631, 130363, 156437,
            187751, 225307, 270371, 324449, 389357, 467237, 560689, 672827, 807403, 968897, 1162687, 1395263,
            1674319, 2009191, 2411033, 2893249, 3471899, 4166287, 4999559, 5999471, 7199369
        };
        for(auto prime : prime_array)
        {
            if(prime >= size)
            {
                return prime;
            }
        }
        for(size_type prime = (size | 1); prime < std::numeric_limits<uint32_t>::max(); prime += 2)
        {
            if(is_prime_(prime) && ((prime - 1) % 101 != 0))
            {
                return prime;
            }
        }
        return size;
    }

    void rehash_(std::true_type, size_type size)
    {
        size = std::min(get_prime_(size), max_size());
        if(root_.bucket_count != 0)
        {
            get_bucket_allocator_().deallocate(root_.bucket, root_.bucket_count);
        }
        root_.bucket = get_bucket_allocator_().allocate(size);
        std::memset(root_.bucket, 0xFFFFFFFF, sizeof(offset_type) * size);

        for(size_type i = 0; i < root_.size; ++i)
        {
            if(root_.index[i].hash)
            {
                size_type bucket = root_.index[i].hash % size;
                if(root_.bucket[bucket] != offset_empty)
                {
                    root_.index[root_.bucket[bucket]].prev = offset_type(i);
                }
                root_.index[i].prev = offset_empty;
                root_.index[i].next = root_.bucket[bucket];
                root_.bucket[bucket] = offset_type(i);
            }
        }
        root_.bucket_count = size;
    }

    void rehash_(std::false_type, size_type size)
    {
        size = std::min(get_prime_(size), max_size());
        offset_type *new_bucket = get_bucket_allocator_().allocate(size);
        std::memset(new_bucket, 0xFFFFFFFF, sizeof(offset_type) * size);

        if(root_.bucket_count != 0)
        {
            for(size_type i = 0; i < root_.bucket_count; ++i)
            {
                if(root_.bucket[i] != offset_empty)
                {
                    size_type j = root_.bucket[i], nj;
                    do
                    {
                        nj = root_.index[j].next;
                        size_type bucket = root_.index[j].hash % size;
                        if(new_bucket[bucket] != offset_empty)
                        {
                            root_.index[new_bucket[bucket]].prev = offset_type(j);
                        }
                        root_.index[j].prev = offset_empty;
                        root_.index[j].next = new_bucket[bucket];
                        new_bucket[bucket] = offset_type(j);
                        j = nj;
                    }
                    while(j != offset_empty);
                }
            }
            get_bucket_allocator_().deallocate(root_.bucket, root_.bucket_count);
        }
        root_.bucket_count = size;
        root_.bucket = new_bucket;
    }

    void realloc_(size_type size)
    {
        if(size * sizeof(value_t) > 0x1000)
        {
            size = ((size * sizeof(value_t) + std::max<size_type>(sizeof(value_t), 0x1000) - 1) & (~size_type(0) ^ 0xFFF)) / sizeof(value_t);
        }
        else if(size * sizeof(value_t) > 0x100)
        {
            size = ((size * sizeof(value_t) + std::max<size_type>(sizeof(value_t), 0x100) - 1) & (~size_type(0) ^ 0xFF)) / sizeof(value_t);
        }
        else
        {
            size = ((size * sizeof(value_t) + std::max<size_type>(sizeof(value_t), 0x10) - 1) & (~size_type(0) ^ 0xF)) / sizeof(value_t);
        }
        size = std::min(size, max_size());
        index_t *new_index = get_index_allocator_().allocate(size);
        value_t *new_value = get_value_allocator_().allocate(size);

        std::memset(new_index + root_.capacity, 0xFFFFFFFF, sizeof(index_t) * (size - root_.capacity));
        if(root_.capacity != 0)
        {
            std::memcpy(new_index, root_.index, sizeof(index_t) * root_.capacity);
            move_construct_and_destroy_(root_.value->value(), root_.value->value() + root_.capacity, new_value->value());
            get_index_allocator_().deallocate(root_.index, root_.capacity);
            get_value_allocator_().deallocate(root_.value, root_.capacity);
        }
        root_.capacity = size;
        root_.index = new_index;
        root_.value = new_value;
    }

    void check_grow_()
    {
        size_type new_size = size() + 1;
        if(new_size > root_.bucket_count * root_.setting_load_factor)
        {
            if(root_.bucket_count >= max_size())
            {
                throw std::length_error("contiguous_hash too long");
            }
            rehash_(typename config_t::unique_type(), size_type(std::ceil(root_.bucket_count * config_t::grow_proportion(root_.bucket_count))));
        }
        if(new_size > root_.capacity)
        {
            if(root_.capacity >= max_size())
            {
                throw std::length_error("contiguous_hash too long");
            }
            realloc_(size_type(std::ceil(std::max<float>(root_.capacity * config_t::grow_proportion(root_.capacity), root_.bucket_count * root_.setting_load_factor))));
        }
    }

    template<class ...args_t> pair_posi_t insert_value_(args_t &&...args)
    {
        check_grow_();
        return insert_value_uncheck_(typename config_t::unique_type(), std::forward<args_t>(args)...);
    }

    template<class in_t, class ...args_t> typename std::enable_if<std::is_same<key_type, value_type>::value && !std::is_same<typename std::remove_reference<in_t>::type, key_type>::value, pair_posi_t>::type insert_value_uncheck_(std::true_type, in_t &&in, args_t &&...args)
    {
        key_type key = get_key_t()(in, args...);
        hash_t hash = get_hasher()(key);
        size_type bucket = hash % root_.bucket_count;
        for(size_type i = root_.bucket[bucket]; i != offset_empty; i = root_.index[i].next)
        {
            if(root_.index[i].hash == hash && get_key_equal()(get_key_t()(*root_.value[i].value()), get_key_t()(key)))
            {
                return std::make_pair(i, false);
            }
        }
        size_type offset = root_.free_list == offset_empty ? root_.size : root_.free_list;
        construct_one_(root_.value[offset].value(), std::move(key));
        if(offset == root_.free_list)
        {
            root_.free_list = root_.index[offset].next;
            --root_.free_count;
        }
        else
        {
            ++root_.size;
        }
        root_.index[offset].hash = hash;
        root_.index[offset].next = root_.bucket[bucket];
        root_.index[offset].prev = offset_empty;
        if(root_.bucket[bucket] != offset_empty)
        {
            root_.index[root_.bucket[bucket]].prev = offset_type(offset);
        }
        root_.bucket[bucket] = offset_type(offset);
        return std::make_pair(offset, true);
    }
    template<class in_t, class ...args_t> typename std::enable_if<!std::is_same<key_type, value_type>::value || std::is_same<typename std::remove_reference<in_t>::type, key_type>::value, pair_posi_t>::type insert_value_uncheck_(std::true_type, in_t &&in, args_t &&...args)
    {
        hash_t hash = get_hasher()(get_key_t()(in, args...));
        size_type bucket = hash % root_.bucket_count;
        for(size_type i = root_.bucket[bucket]; i != offset_empty; i = root_.index[i].next)
        {
            if(root_.index[i].hash == hash && get_key_equal()(get_key_t()(*root_.value[i].value()), get_key_t()(in, args...)))
            {
                return std::make_pair(i, false);
            }
        }
        size_type offset = root_.free_list == offset_empty ? root_.size : root_.free_list;
        construct_one_(root_.value[offset].value(), std::forward<in_t>(in), std::forward<args_t>(args)...);
        if(offset == root_.free_list)
        {
            root_.free_list = root_.index[offset].next;
            --root_.free_count;
        }
        else
        {
            ++root_.size;
        }
        root_.index[offset].hash = hash;
        root_.index[offset].next = root_.bucket[bucket];
        root_.index[offset].prev = offset_empty;
        if(root_.bucket[bucket] != offset_empty)
        {
            root_.index[root_.bucket[bucket]].prev = offset_type(offset);
        }
        root_.bucket[bucket] = offset_type(offset);
        return std::make_pair(offset, true);
    }

    template<class in_t, class ...args_t> pair_posi_t insert_value_uncheck_(std::false_type, in_t &&in, args_t &&...args)
    {
        size_type offset = root_.free_list == offset_empty ? root_.size : root_.free_list;
        construct_one_(root_.value[offset].value(), std::forward<in_t>(in), std::forward<args_t>(args)...);
        if(offset == root_.free_list)
        {
            root_.free_list = root_.index[offset].next;
            --root_.free_count;
        }
        else
        {
            ++root_.size;
        }
        hash_t hash = get_hasher()(get_key_t()(*root_.value[offset].value()));
        size_type bucket = hash % root_.bucket_count;
        size_type where;
        for(where = root_.bucket[bucket]; where != offset_empty; where = root_.index[where].next)
        {
            if(root_.index[where].hash == hash && get_key_equal()(get_key_t()(*root_.value[where].value()), get_key_t()(*root_.value[offset].value())))
            {
                break;
            }
        }
        root_.index[offset].hash = hash;
        if(where == offset_empty)
        {
            root_.index[offset].next = root_.bucket[bucket];
            root_.index[offset].prev = offset_empty;
            if(root_.bucket[bucket] != offset_empty)
            {
                root_.index[root_.bucket[bucket]].prev = offset_type(offset);
            }
            root_.bucket[bucket] = offset_type(offset);
        }
        else
        {
            root_.index[offset].next = root_.index[where].next;
            root_.index[offset].prev = offset_type(where);
            root_.index[where].next = offset_type(offset);
            if(root_.index[offset].next != offset_empty)
            {
                root_.index[root_.index[offset].next].prev = offset_type(offset);
            }
        }
        return std::make_pair(offset, true);
    }

    template<class in_key_t> size_type find_value_(in_key_t const &key) const
    {
        hash_t hash = get_hasher()(key);
        size_type bucket = hash % root_.bucket_count;

        for(size_type i = root_.bucket[bucket]; i != offset_empty; i = root_.index[i].next)
        {
            if(root_.index[i].hash == hash && get_key_equal()(get_key_t()(*root_.value[i].value()), key))
            {
                return i;
            }
        }
        return root_.size;
    }

    size_type remove_value_(std::true_type, key_type const &key)
    {
        size_type offset = find_value_(key);
        if(offset != root_.size)
        {
            remove_offset_(offset);
            return 1;
        }
        else
        {
            return 0;
        }
    }

    size_type remove_value_(std::false_type, key_type const &key)
    {
        size_type offset = find_value_(key);
        if(offset != root_.size)
        {
            hash_t hash = root_.index[offset].hash;
            size_type count = 1;
            while(true)
            {
                size_type next = root_.index[offset].next;
                remove_offset_(offset);
                if(next == offset_empty || root_.index[next].hash != hash || !get_key_equal()(get_key_t()(*root_.value[next].value()), get_key_t()(key)))
                {
                    return count;
                }
                offset = next;
                ++count;
            }
        }
        else
        {
            return 0;
        }
    }

    void remove_offset_(size_type offset)
    {
        if(root_.index[offset].prev != offset_empty)
        {
            root_.index[root_.index[offset].prev].next = root_.index[offset].next;
        }
        else
        {
            root_.bucket[root_.index[offset].hash % root_.bucket_count] = root_.index[offset].next;
        }
        if(root_.index[offset].next != offset_empty)
        {
            root_.index[root_.index[offset].next].prev = root_.index[offset].prev;
        }
        root_.index[offset].next = root_.free_list;
        root_.index[offset].hash.clear();
        root_.free_list = offset_type(offset);
        ++root_.free_count;

        destroy_one_(root_.value[offset].value());
    }

};

#if defined(__GNUC__)
#   pragma GCC diagnostic pop
#endif
