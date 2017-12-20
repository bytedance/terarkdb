
#pragma once

#include <terark/fstring.hpp>
#include <terark/valvec.hpp>
#include <terark/util/refcount.hpp>
#include <terark/int_vector.hpp>
#include <terark/io/DataIO.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/util/fstrvec.hpp>
#include <boost/intrusive_ptr.hpp>
#include <memory>

using terark::valvec;
using terark::febitvec;


#define RET_IF_ZERO(v) \
  do {                    \
    if ((v) == 0)         \
      return 0;           \
  } while(0)

// make sure at least one '1' and at least one '0'
template<typename T>
class rank_select_fewzero {
public:
  typedef boost::mpl::false_ is_mixed;
  typedef T index_t;
  rank_select_fewzero() : m_size(0) {}
  explicit rank_select_fewzero(size_t sz) : m_size(sz) {}

  void clear() {
    m_size = 0;
    m_pospool.clear();
  }
  void risk_release_ownership() {
    m_pospool.risk_release_ownership();
  }
  /*
   * in order for 8 byte aligned up, all risk_mmap_from() &
   * mem_size() are customized
   */
  void risk_mmap_from(unsigned char* base, size_t length) {
    size_t cnt = length / sizeof(T);
    m_pospool.risk_set_data((T*)base, cnt);
    m_size = m_pospool[0];
    // for 4B and odd size, Pad0ForAlign was made
    if (sizeof(T) % 8 && m_pospool.back() == 0) {
        m_pospool.risk_release_ownership();
        cnt --;
        m_pospool.risk_set_data((T*)base, cnt);
    }
  }
  size_t mem_size() const {
    return terark::align_up(m_pospool.used_mem_size(), 8);
  }
  template<class RankSelect>
  void build_from(RankSelect& rs) {
    if (rs.isall0() || rs.isall1()) {
      THROW_STD(invalid_argument,
                "there should be cnt(0) > 0 && cnt(1) > 0");
    }
    // for fewzero, 0.1 is actually reasonable
    m_pospool.reserve(std::max<size_t>(m_size / 10, 10));
    m_pospool.push_back(m_size); // [0] for m_size
    size_t idx = 1;
    for (size_t pos = 0; pos < m_size; pos++) {
      if (rs.is0(pos)) {
        m_pospool.push_back(pos);
        idx ++;
      }
    }
    m_pospool.push_back(0); // append extra '0' for align consideration
    m_pospool.resize(idx); // resize to the actual size, extra '0' will be kept behind for align
  }
  void build_cache(bool, bool) { assert(0); }
  void swap(rank_select_fewzero& another) {
    std::swap(m_size, another.m_size);
    std::swap(m_pospool, another.m_pospool);
  }
  void set0(size_t) { assert(0); }
  void set1(size_t) { assert(0); }

  // exclude pos
  size_t rank0(size_t pos) const {
    assert(pos < m_size);
    RET_IF_ZERO(pos);
    pos --;
    size_t rank0 = terark::lower_bound_n<const valvec<T>&>(
      m_pospool, 1, m_pospool.size(), pos);
    if (rank0 >= m_pospool.size()) {
      return m_pospool.size() - 1;
    } else if (pos == m_pospool[rank0]) {
      return rank0;
    } else { // <
      return rank0 - 1;
    }
  }
  size_t rank1(size_t pos) const {
    assert(pos < m_size);
    RET_IF_ZERO(pos);
    return pos - rank0(pos);
  }
  size_t select0(size_t id) const {
    RET_IF_ZERO(id);
    if (id > max_rank0())
      return size_t(-1);
    return m_pospool[id];
  }
  /*
   * pos0 = rank0[pos0] + rank1[pos0] =>
   *   rank1[pos0] = pos0 - rank0[pos0] =>
   *   rank1[pos0] = m_pospool[id] + 1 - id to find id? =>
   * j = m_pospool[i] - i + 1, find lower_bound() that
   * id <= m_pospool[i] - i + 1,
   * if id == m_pospool[i] - i + 1:
   *   return m_pospool[i] - i
   * elif id < m_pospool[i] - i + 1:
   *   offset = id - (m_pospool[i - 1] - (i - 1) + 1)
   *   return m_pospool[i - 1] + offset
   */
  size_t select1(size_t id) const {
    RET_IF_ZERO(id);
    if (id > max_rank1())
      return size_t(-1);
    size_t rank0 = lower_bound(1, m_pospool.size(), id);
    if (rank0 < m_pospool.size() && id == rank1_from_rank0(rank0))
      return m_pospool[rank0] - 1;
    if (rank0 >= m_pospool.size()) { // beyond the last '0'
      rank0 = m_pospool.size() - 1;
    } else { // move to prev '0'
      rank0 --;
    }
    size_t prev = rank1_from_rank0(rank0);
    size_t offset = id - prev;
    return m_pospool[rank0] + offset;
  }

private:
  size_t rank1_from_rank0(size_t i) const {
    assert(i < m_pospool.size());
    return m_pospool[i] - i + 1;
  }
  // return rank0 that m_pospool[rank0] >= rank1's position
  size_t lower_bound(size_t low, size_t upp, const size_t& rank1) const {
    size_t i = low, j = upp;
    while (i < j) {
      size_t mid = (i + j) / 2;
      // same as: t = rank1_from_rank0(mid);
      size_t t = m_pospool[mid] - mid + 1;
      if (t < rank1)
        i = mid + 1;
      else
        j = mid;
    }
    return i;
  }
  // res: as the rank0 lower_bound
  bool is1(size_t pos, size_t& rank0) const {
    assert(pos < m_size);
    rank0 = terark::lower_bound_n<const terark::valvec<T>&>(
      m_pospool, 1, m_pospool.size(), pos);
    if (rank0 >= m_pospool.size()) { // not in '0's
      return true;
    } else {
      return (pos != m_pospool[rank0]);
    }
  }

public:
  size_t max_rank0() const { return m_pospool.size() - 1; }
  size_t max_rank1() const { return m_size - (m_pospool.size() - 1); }
  size_t size() const { return m_size; }
  size_t isall0() const { return false; }
  size_t isall1() const { return false; }

  const void* data() const { return m_pospool.data(); }
  bool operator[](size_t pos) const {
    assert(pos < m_size);
    return is1(pos);
  }
  // TBD: use hash_set maybe better
  bool is1(size_t pos) const {
    assert(pos < m_size);
    size_t res;
    return is1(pos, res);
  }
  bool is0(size_t pos) const {
    assert(pos < m_size);
    return !is1(pos);
  }

  const uint32_t* get_rank_cache() const { return NULL; }
  const uint32_t* get_sel0_cache() const { return NULL; }
  const uint32_t* get_sel1_cache() const { return NULL; }

  ///@returns number of continuous one/zero bits starts at bitpos
  size_t zero_seq_len(size_t pos) const {
    assert(pos < m_size);
    size_t i;
    if (is1(pos, i))
      return 0;
    size_t cnt = 1;
    while (i < m_pospool.size() - 1) {
      if (m_pospool[i] + 1 == m_pospool[i + 1])
        i++, cnt++;
      else
        break;
    }
    return cnt;
  }
  // Next() accelerate version
  size_t zero_seq_len(size_t pos, size_t& hint) const {
    assert(pos < m_size);
    if (0 < hint && hint < m_pospool.size() - 1 &&
        m_pospool[hint] < pos && pos < m_pospool[hint + 1]) {
      return 0;
    }
    size_t i;
    if (is1(pos, i)) {
      hint = i - 1;
      return 0;
    }
    size_t cnt = 1;
    while (i < m_pospool.size() - 1) {
      if (m_pospool[i] + 1 == m_pospool[i + 1])
        i++, cnt++;
      else
        break;
    }
    return cnt;
  }

  size_t zero_seq_revlen(size_t pos) const {
    assert(pos < m_size);
    RET_IF_ZERO(pos);
    pos --;
    size_t i;
    if (is1(pos, i))
      return 0;
    size_t cnt = 1;
    while (i > 1) { // m_pospool[0] is 'placeholder' -- m_size
      if (m_pospool[i - 1] + 1 == m_pospool[i])
        i--, cnt++;
      else
        break;
    }
    return cnt;
  }
  // Prev() accelerate version
  size_t zero_seq_revlen(size_t pos, size_t& hint) const {
    assert(pos < m_size);
    RET_IF_ZERO(pos);
    pos --;
    if (0 < hint && hint < m_pospool.size() - 1 &&
        m_pospool[hint] < pos && pos < m_pospool[hint + 1]) {
      return 0;
    }
    size_t i;
    if (is1(pos, i)) {
      hint = i - 1;
      return 0;
    }
    size_t cnt = 1;
    while (i > 1) { // m_pospool[0] is 'placeholder' -- m_size
      if (m_pospool[i - 1] + 1 == m_pospool[i])
        i--, cnt++;
      else
        break;
    }
    return cnt;
  }


  size_t one_seq_len(size_t pos) const {
    assert(pos < m_size);
    size_t i;
    if (!is1(pos, i))
      return 0;
    if (i >= m_pospool.size())
      return m_size - pos;
    else
      return m_pospool[i] - pos;
  }
  size_t one_seq_revlen(size_t pos) const {
    assert(pos < m_size);
    RET_IF_ZERO(pos);
    pos --;
    size_t i;
    if (!is1(pos, i))
      return 0;
    if (i == 1)
      return pos + 1;
    else
      return pos - m_pospool[i - 1];
  }

private:
  // [0] is used to store m_size
  valvec<T> m_pospool;
  size_t m_size;
};


// make sure at least one '1' and at least one '0'
template<typename T>
class rank_select_fewone {
public:
  typedef boost::mpl::false_ is_mixed;
  typedef T index_t;
  rank_select_fewone() : m_size(0) {}
  explicit rank_select_fewone(size_t sz) : m_size(sz) {}

  void clear() {
    m_size = 0;
    m_pospool.clear();
  }
  void risk_release_ownership() {
    m_pospool.risk_release_ownership();
  }
  /*
   * in order for 8 byte aligned up, both risk_mmap_from() &
   * mem_size() & build_from() customized
   */
  void risk_mmap_from(unsigned char* base, size_t length) {
    size_t cnt = length / sizeof(T);
    m_pospool.risk_set_data((T*)base, cnt);
    m_size = m_pospool[0];
    // for 4B and odd size, Pad0ForAlign was made
    if (sizeof(T) % 8 && m_pospool.back() == 0) {
      m_pospool.risk_release_ownership();
      cnt --;
      m_pospool.risk_set_data((T*)base, cnt);
    }
  }
  size_t mem_size() const {
    return terark::align_up(m_pospool.used_mem_size(), 8);
  }
  template<class RankSelect>
  void build_from(RankSelect& rs) {
    if (rs.isall0() || rs.isall1()) {
      THROW_STD(invalid_argument, "there should be cnt(0) > 0 && cnt(1) > 0");
    }
    // for fewzero, 0.1 is actually reasonable
    m_pospool.reserve(std::max<size_t>(m_size / 10, 10));
    m_pospool.push_back(m_size); // [0] for m_size
    size_t idx = 1;
    for (size_t pos = 0; pos < m_size; pos++) {
      if (rs.is1(pos)) {
        m_pospool.push_back(pos);
        idx ++;
      }
    }
    m_pospool.push_back(0); // append extra '0' for align consideration
    m_pospool.resize(idx); // resize to the actual size, extra '0' will be kept behind for align
  }
  void build_cache(bool, bool) { assert(0); }
  void swap(rank_select_fewone& another) {
    std::swap(m_size, another.m_size);
    std::swap(m_pospool, another.m_pospool);
  }
  void set0(size_t) { assert(0); }
  void set1(size_t) { assert(0); }

  // exclude pos
  size_t rank0(size_t pos) const {
    assert(pos < m_size);
    RET_IF_ZERO(pos);
    return pos - rank1(pos);
  }
  size_t rank1(size_t pos) const {
    assert(pos < m_size);
    RET_IF_ZERO(pos);
    pos --;
    size_t rank = terark::lower_bound_n<const valvec<T>&>(
      m_pospool, 1, m_pospool.size(), pos);
    if (rank == m_pospool.size()) {
      return m_pospool.size() - 1;
    } else if (pos == m_pospool[rank]) {
      return rank;
    } else { // <
      return rank - 1;
    }
  }

  size_t select0(size_t id) const {
    RET_IF_ZERO(id);
    if (id > max_rank0())
      return size_t(-1);
    size_t i = lower_bound(1, m_pospool.size(), id);
    // TBD: possible consecutive m_pos[i] + 1 == m_pos[i + 1]
    // make sure arr[m_pospool[i] - 1] is '1' not '0'
    if (i < m_pospool.size() && id == rank0_from_rank1(i))
      return m_pospool[i] - 1;
    if (i >= m_pospool.size()) { // beyond the last '0'
      i = m_pospool.size() - 1;
    } else { // move to prev '0'
      i --;
    }
    size_t prev = rank0_from_rank1(i);
    size_t offset = id - prev;
    return m_pospool[i] + offset;
  }
  size_t select1(size_t id) const {
    RET_IF_ZERO(id);
    if (id > max_rank1())
      return size_t(-1);
    return m_pospool[id];
  }

private:
  size_t rank0_from_rank1(size_t i) const {
    assert(i < m_pospool.size());
    return m_pospool[i] - i + 1;
  }
  // return rank1 that m_pospool[rank1] >= rank0's position
  size_t lower_bound(size_t low, size_t upp, const size_t& rank0) const {
    size_t i = low, j = upp;
    while (i < j) {
      size_t mid = (i + j) / 2;
      //size_t t = rank0_from_rank1(mid);
      size_t t = m_pospool[mid] - mid + 1;
      if (t < rank0)
        i = mid + 1;
      else
        j = mid;
    }
    return i;
  }

  // res: as the rank0 lower_bound
  bool is0(size_t pos, size_t& res) const {
    assert(pos < m_size);
    res = terark::lower_bound_n<const terark::valvec<T>&>(
      m_pospool, 1, m_pospool.size(), pos);
    if (res >= m_pospool.size()) { // not in '1's
      return true;
    } else {
      return (pos != m_pospool[res]);
    }
  }

public:
  size_t max_rank0() const { return m_size - (m_pospool.size() - 1); }
  size_t max_rank1() const { return m_pospool.size() - 1; }
  size_t size() const { return m_size; }
  size_t isall0() const { return false; }
  size_t isall1() const { return false; }

  const void* data() const { return m_pospool.data(); }
  bool operator[](size_t pos) const {
    assert(pos < m_size);
    return is1(pos);
  }
  // TBD: use hash_set maybe better
  bool is0(size_t pos) const {
    assert(pos < m_size);
    size_t res;
    return is0(pos, res);
  }
  bool is1(size_t pos) const {
    assert(pos < m_size);
    return !is0(pos);
  }

  const uint32_t* get_rank_cache() const { return NULL; }
  const uint32_t* get_sel0_cache() const { return NULL; }
  const uint32_t* get_sel1_cache() const { return NULL; }

  ///@returns number of continuous one/zero bits starts at bitpos
  size_t zero_seq_len(size_t pos) const {
    assert(pos < m_size);
    size_t i;
    if (!is0(pos, i))
      return 0;
    if (i >= m_pospool.size())
      return m_size - pos;
    else
      return m_pospool[i] - pos;
  }
  // Next() accelerate version
  size_t zero_seq_len(size_t pos, size_t& hint) const {
    assert(pos < m_size);
    if (0 < hint && hint < m_pospool.size() - 1 &&
        m_pospool[hint] < pos && pos < m_pospool[hint + 1]) {
      hint ++; // to next '1' where following Next() will start from
      return m_pospool[hint] - pos;
    }
    size_t i;
    if (!is0(pos, i)) {
      hint = i - 1;
      return 0;
    }
    if (i >= m_pospool.size())
      return m_size - pos;
    else
      return m_pospool[i] - pos;
  }

  size_t zero_seq_revlen(size_t pos) const {
    assert(pos < m_size);
    RET_IF_ZERO(pos);
    pos --;
    size_t i;
    if (!is0(pos, i))
      return 0;
    if (i == 1)
      return pos + 1;
    else
      return pos - m_pospool[i - 1];
  }
  // Prev() accelerate version
  size_t zero_seq_revlen(size_t pos, size_t& hint) const {
    assert(pos < m_size);
    RET_IF_ZERO(pos);
    pos --;
    if (0 < hint && hint < m_pospool.size() - 1 &&
        m_pospool[hint] < pos && pos < m_pospool[hint + 1]) {
      size_t cur = hint--; // to prev '1' where following Prev() will start from
      return pos - m_pospool[cur];
    }
    size_t i;
    if (!is0(pos, i)) {
      hint = i - 1;
      return 0;
    }
    if (i == 1)
      return pos + 1;
    else
      return pos - m_pospool[i - 1];
  }



  size_t one_seq_len(size_t pos) const {
    assert(pos < m_size);
    size_t i;
    if (is0(pos, i))
      return 0;
    size_t cnt = 1;
    while (i < m_pospool.size() - 1) {
      if (m_pospool[i] + 1 == m_pospool[i + 1])
        i++, cnt++;
      else
        break;
    }
    return cnt;
  }
  size_t one_seq_revlen(size_t pos) const {
    assert(pos < m_size);
    RET_IF_ZERO(pos);
    pos --;
    size_t i;
    if (is0(pos, i))
      return 0;
    size_t cnt = 1;
    while (i > 1) { // m_pospool[0] is placeholder
      if (m_pospool[i - 1] + 1 == m_pospool[i])
        i--, cnt++;
      else
        break;
    }
    return cnt;
  }

private:
  // [0] is used to store m_size
  valvec<T> m_pospool;
  size_t m_size;
};

