
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
  void risk_mmap_from(unsigned char* base, size_t length);
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

  size_t rank0(size_t pos) const;
  size_t rank1(size_t pos) const;
  size_t select0(size_t id) const;
  size_t select1(size_t id) const;

private:
  size_t rank1_from_rank0(size_t i) const {
    assert(i < m_pospool.size());
    return m_pospool[i] - i + 1;
  }
  // return rank0 that m_pospool[rank0] >= rank1's position
  size_t lower_bound(size_t low, size_t upp, const size_t& rank1) const;
  // res: as the rank0 lower_bound
  bool is1(size_t pos, size_t& rank0) const;

public:
  size_t max_rank0() const { return m_pospool.size() - 1; }
  size_t max_rank1() const { return m_size - (m_pospool.size() - 1); }
  size_t size() const { return m_size; }
  size_t isall0() const { return false; }
  size_t isall1() const { return false; }

  const void* data() const { return m_pospool.data(); }
  bool operator[](size_t pos) const;

  bool is1(size_t pos) const;
  bool is0(size_t pos) const;

  const uint32_t* get_rank_cache() const { return NULL; }
  const uint32_t* get_sel0_cache() const { return NULL; }
  const uint32_t* get_sel1_cache() const { return NULL; }

  ///@returns number of continuous one/zero bits starts at bitpos
  size_t zero_seq_len(size_t pos) const;
  // Next() accelerate version
  size_t zero_seq_len(size_t pos, size_t& hint) const;
  size_t zero_seq_revlen(size_t pos) const;
  // Prev() accelerate version
  size_t zero_seq_revlen(size_t pos, size_t& hint) const;

  size_t one_seq_len(size_t pos) const;
  size_t one_seq_revlen(size_t pos) const;

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
  void risk_mmap_from(unsigned char* base, size_t length);
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
  size_t rank0(size_t pos) const;
  size_t rank1(size_t pos) const;
  size_t select0(size_t id) const;
  size_t select1(size_t id) const;

private:
  size_t rank0_from_rank1(size_t i) const {
    assert(i < m_pospool.size());
    return m_pospool[i] - i + 1;
  }
  // return rank1 that m_pospool[rank1] >= rank0's position
  size_t lower_bound(size_t low, size_t upp, const size_t& rank0) const;
  // res: as the rank0 lower_bound
  bool is0(size_t pos, size_t& res) const;

public:
  size_t max_rank0() const { return m_size - (m_pospool.size() - 1); }
  size_t max_rank1() const { return m_pospool.size() - 1; }
  size_t size() const { return m_size; }
  size_t isall0() const { return false; }
  size_t isall1() const { return false; }

  const void* data() const { return m_pospool.data(); }
  bool operator[](size_t pos) const;
  bool is0(size_t pos) const;
  bool is1(size_t pos) const;

  ///@returns number of continuous one/zero bits starts at bitpos
  size_t zero_seq_len(size_t pos) const;
  // Next() accelerate version
  size_t zero_seq_len(size_t pos, size_t& hint) const;
  size_t zero_seq_revlen(size_t pos) const;
  // Prev() accelerate version
  size_t zero_seq_revlen(size_t pos, size_t& hint) const;

  size_t one_seq_len(size_t pos) const;
  size_t one_seq_revlen(size_t pos) const;

private:
  // [0] is used to store m_size
  valvec<T> m_pospool;
  size_t m_size;
};

