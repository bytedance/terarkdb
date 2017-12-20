
#include "rank_select_fewzero.h"

/*
 * in order for 8 byte aligned up, all risk_mmap_from() &
 * mem_size() are customized
 */
template<typename T>
void rank_select_fewzero<T>::risk_mmap_from(
  unsigned char* base, size_t length) {
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

// exclude pos
template<typename T>
size_t rank_select_fewzero<T>::rank0(size_t pos) const {
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
template<typename T>
size_t rank_select_fewzero<T>::rank1(size_t pos) const {
  assert(pos < m_size);
  RET_IF_ZERO(pos);
  return pos - rank0(pos);
}
template<typename T>
size_t rank_select_fewzero<T>::select0(size_t id) const {
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
template<typename T>
size_t rank_select_fewzero<T>::select1(size_t id) const {
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

// return rank0 that m_pospool[rank0] >= rank1's position
template<typename T>
size_t rank_select_fewzero<T>::lower_bound(
  size_t low, size_t upp, const size_t& rank1) const {
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
template<typename T>
bool rank_select_fewzero<T>::is1(size_t pos, size_t& rank0) const {
  assert(pos < m_size);
  rank0 = terark::lower_bound_n<const terark::valvec<T>&>(
    m_pospool, 1, m_pospool.size(), pos);
  if (rank0 >= m_pospool.size()) { // not in '0's
    return true;
  } else {
    return (pos != m_pospool[rank0]);
  }
}
template<typename T>
bool rank_select_fewzero<T>::operator[](size_t pos) const {
  assert(pos < m_size);
  return is1(pos);
}
template<typename T>
bool rank_select_fewzero<T>::is1(size_t pos) const {
  assert(pos < m_size);
  size_t res;
  return is1(pos, res);
}
template<typename T>
bool rank_select_fewzero<T>::is0(size_t pos) const {
  assert(pos < m_size);
  return !is1(pos);
}

///@returns number of continuous one/zero bits starts at bitpos
template<typename T>
size_t rank_select_fewzero<T>::zero_seq_len(size_t pos) const {
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
template<typename T>
size_t rank_select_fewzero<T>::zero_seq_len(size_t pos, size_t& hint) const {
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
template<typename T>
size_t rank_select_fewzero<T>::zero_seq_revlen(size_t pos) const {
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
template<typename T>
size_t rank_select_fewzero<T>::zero_seq_revlen(size_t pos, size_t& hint) const {
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
template<typename T>
size_t rank_select_fewzero<T>::one_seq_len(size_t pos) const {
  assert(pos < m_size);
  size_t i;
  if (!is1(pos, i))
    return 0;
  if (i >= m_pospool.size())
    return m_size - pos;
  else
    return m_pospool[i] - pos;
}
template<typename T>
size_t rank_select_fewzero<T>::one_seq_revlen(size_t pos) const {
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






/*
 * in order for 8 byte aligned up, both risk_mmap_from() &
 * mem_size() & build_from() customized
 */
template<typename T>
void rank_select_fewone<T>::risk_mmap_from(
  unsigned char* base, size_t length) {
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

template<typename T>
size_t rank_select_fewone<T>::rank0(size_t pos) const {
  assert(pos < m_size);
  RET_IF_ZERO(pos);
  return pos - rank1(pos);
}
template<typename T>
size_t rank_select_fewone<T>::rank1(size_t pos) const {
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
template<typename T>
size_t rank_select_fewone<T>::select0(size_t id) const {
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
template<typename T>
size_t rank_select_fewone<T>::select1(size_t id) const {
  RET_IF_ZERO(id);
  if (id > max_rank1())
    return size_t(-1);
  return m_pospool[id];
}

// return rank1 that m_pospool[rank1] >= rank0's position
template<typename T>
size_t rank_select_fewone<T>::lower_bound(
  size_t low, size_t upp, const size_t& rank0) const {
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
template<typename T>
bool rank_select_fewone<T>::is0(size_t pos, size_t& res) const {
  assert(pos < m_size);
  res = terark::lower_bound_n<const terark::valvec<T>&>(
    m_pospool, 1, m_pospool.size(), pos);
  if (res >= m_pospool.size()) { // not in '1's
    return true;
  } else {
    return (pos != m_pospool[res]);
  }
}
template<typename T>
bool rank_select_fewone<T>::operator[](size_t pos) const {
  assert(pos < m_size);
  return is1(pos);
}
template<typename T>
bool rank_select_fewone<T>::is0(size_t pos) const {
  assert(pos < m_size);
  size_t res;
  return is0(pos, res);
}
template<typename T>
bool rank_select_fewone<T>::is1(size_t pos) const {
  assert(pos < m_size);
  return !is0(pos);
}

///@returns number of continuous one/zero bits starts at bitpos
template<typename T>
size_t rank_select_fewone<T>::zero_seq_len(size_t pos) const {
  assert(pos < m_size);
  size_t i;
  if (!is0(pos, i))
    return 0;
  if (i >= m_pospool.size())
    return m_size - pos;
  else
    return m_pospool[i] - pos;
}
template<typename T>
size_t rank_select_fewone<T>::zero_seq_len(size_t pos, size_t& hint) const {
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
template<typename T>
size_t rank_select_fewone<T>::zero_seq_revlen(size_t pos) const {
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
template<typename T>
size_t rank_select_fewone<T>::zero_seq_revlen(size_t pos, size_t& hint) const {
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

template<typename T>
size_t rank_select_fewone<T>::one_seq_len(size_t pos) const {
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
template<typename T>
size_t rank_select_fewone<T>::one_seq_revlen(size_t pos) const {
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




template class TERARK_DLL_EXPORT rank_select_fewzero<uint32_t>;
template class TERARK_DLL_EXPORT rank_select_fewzero<uint64_t>;
template class TERARK_DLL_EXPORT rank_select_fewone<uint32_t>;
template class TERARK_DLL_EXPORT rank_select_fewone<uint64_t>;
