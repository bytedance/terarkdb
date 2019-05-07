/*
 * terark_zip_table.cc
 *
 *  Created on: 2016-08-09
 *      Author: leipeng
 */

// project headers
#include "terark_zip_table.h"
#include "terark_zip_index.h"
#include "terark_zip_common.h"
#include "terark_zip_internal.h"
#include "terark_zip_table_reader.h"

// std headers
#include <future>
#include <random>
#include <cstdlib>
#include <cstdint>
#include <fstream>
#include <util/arena.h> // for #include <sys/mman.h>
#ifdef _MSC_VER
# include <io.h>
#else
# include <sys/types.h>
# include <sys/mman.h>
#endif

// boost headers
#include <boost/predef/other/endian.h>
#include <boost/archive/iterators/binary_from_base64.hpp>
#include <boost/archive/iterators/transform_width.hpp>

// rocksdb headers
#include <table/meta_blocks.h>

// terark headers
#include <terark/lcast.hpp>
#include <terark/zbs/xxhash_helper.hpp>

// 3rd-party headers

#include <nlohmann/json.hpp>
#ifdef USE_CRYPTOPP
# include <cryptopp/cryptlib.h>
# include <cryptopp/osrng.h>
# include <cryptopp/base64.h>
# include <cryptopp/filters.h>
# include <cryptopp/eccrypto.h>
# include <cryptopp/gfpcrypt.h>
# include <cryptopp/rsa.h>
#endif
#ifdef USE_OPENSSL
extern "C" {
# include <openssl/sha.h>
# include <openssl/rsa.h>
# include <openssl/rand.h>
# include <openssl/objects.h>
# include <openssl/pem.h>
# include <openssl/bio.h>
}
#endif

static std::once_flag PrintVersionHashInfoFlag;

#ifndef _MSC_VER
const char* git_version_hash_info_core();
const char* git_version_hash_info_fsa();
const char* git_version_hash_info_zbs();
const char* git_version_hash_info_terark_zip_rocksdb();
#endif

void PrintVersionHashInfo(rocksdb::Logger* info_log) {
  std::call_once(PrintVersionHashInfoFlag, [info_log] {
# ifndef _MSC_VER
    INFO(info_log, "core %s", git_version_hash_info_core());
    INFO(info_log, "fsa %s", git_version_hash_info_fsa());
    INFO(info_log, "zbs %s", git_version_hash_info_zbs());
    INFO(info_log, "terark_zip_rocksdb %s", git_version_hash_info_terark_zip_rocksdb());
# endif
  });
}

static std::string base64_decode(const std::string& base64) {
  using namespace boost::archive::iterators;
  typedef transform_width<binary_from_base64<std::string::const_iterator>, 8, 6> base64_decode_iter;
  std::string ret;
  ret.reserve(base64.size() * 3 / 4);
  // this shit boost base64 !!!
  std::copy(base64_decode_iter(base64.begin()), base64_decode_iter(base64.end()),
            std::back_inserter(ret));
  if (base64.size() > 2 && strcmp(base64.data() + base64.size() - 2, "==") == 0) {
    ret.resize(ret.size() - 2);
  } else if (!base64.empty() && base64.back() == '=') {
    ret.pop_back();
  }
  return ret;
}


#ifdef TERARK_SUPPORT_UINT64_COMPARATOR
# if !BOOST_ENDIAN_LITTLE_BYTE && !BOOST_ENDIAN_BIG_BYTE
#   error Unsupported endian !
# endif
#endif

namespace rocksdb {

terark::profiling g_pf;

const uint64_t kTerarkZipTableMagicNumber = 0x1122334455667788;

const std::string kTerarkZipTableValueDictBlock = "TerarkZipTableValueDictBlock";
const std::string kTerarkZipTableOffsetBlock    = "TerarkZipTableOffsetBlock";
const std::string kTerarkEmptyTableKey          = "ThisIsAnEmptyTable";

using terark::XXHash64;

const std::string kTerarkZipTableExtendedBlock = "TerarkZipTableExtendedBlock";
static const uint64_t g_dTerarkTrialDuration = 30ULL * 24 * 3600 * 1000;
static const std::string g_szTerarkPublikKey =
    "MIIBIDANBgkqhkiG9w0BAQEFAAOCAQ0AMIIBCAKCAQEAxPQGCXF8uotaYLixcWL65GO8wYcZ"
    "ONEThvMn9olRVhzOpBW0/OsFuKwP6/9/zN3WK6mFKXc8qoCDIEedH/4U2JYeXQoxzQf7E3Ow"
    "8cQ+0/6oz/5USnvxN0sf28JOEogAxX2Gqub6nt2fl2T/0CeCQ7WBR76EoZa941Q6XfG2mYys"
    "BeapgXm7zWLZJieP9ChQCtEE5JM2f75VHHk99QHsUV4njhQL0weONIuFg163AsuK5QV+dUON"
    "g4UYTb3i9pkmxs2TAQt+rXaB8dpTpetTy+2nscGw+Ya4lHSZEyZlWGfktdR/jRlnyZMElXIq"
    "ZpEctXh0pkFys/ePkMiDLEAGnQIBEQ==";

const std::string kTerarkZipTableBuildTimestamp = "terark.build.timestamp";
const std::string kTerarkZipTableDictInfo       = "terark.build.dict_info";
const std::string kTerarkZipTableDictSize       = "terark.build.dict_size";
const std::string kTerarkZipTableEntropy        = "terark.build.entropy";

LicenseInfo::LicenseInfo() {
  using namespace std::chrono;
  head.size += sizeof(SstInfo);
  sst = &sst_storage;
  sst->create_date = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
  sst->sign = XXHash64(kTerarkZipTableMagicNumber)
    .update(&sst->create_date, sizeof sst->create_date)
    .digest();
  head.sign = XXHash64(kTerarkZipTableMagicNumber)
    .update(sst, sizeof *sst)
    .digest();
}

LicenseInfo::Result LicenseInfo::load_nolock(const std::string& license_file) try {
  using json = nlohmann::json;
  json signedJson = json::parse(std::ifstream(license_file, std::ios::binary));
  std::string strSign = base64_decode(signedJson["sign"].get<std::string>());
  std::string strData = base64_decode(signedJson["data"].get<std::string>());
#if defined(USE_CRYPTOPP)
  using namespace CryptoPP;
  std::string binPubKey = base64_decode(g_szTerarkPublikKey);
  StringSource pubFile(binPubKey, true, nullptr);
  RSASS<PKCS1v15, SHA256>::Verifier pub(pubFile);

  StringSource signedSource(strSign, true, nullptr);
  if (signedSource.MaxRetrievable() != pub.SignatureLength()) {
    return BadLicense;
  }
  SecByteBlock signature(pub.SignatureLength());
  signedSource.Get(signature, signature.size());

  if (!pub.VerifyMessage((const byte*) strData.data(), strData.size(),
                         signature.data(), signature.size())) {
    return BadLicense;
  }
#elif defined(USE_OPENSSL)
  int ret = 1;
  unsigned char digest[SHA256_DIGEST_LENGTH];
  SHA256_CTX sha_ctx;
  memset(&sha_ctx, 0, sizeof sha_ctx);
  ret = SHA256_Init(&sha_ctx);
  if (ret != 1) {
    return InternalError;
  }
  ret = SHA256_Update(&sha_ctx, strData.data(), strData.size());
  if (ret != 1) {
    return InternalError;
  }
  ret = SHA256_Final(digest, &sha_ctx);
  if (ret != 1) {
    return InternalError;
  }
  std::string publicKey;
  publicKey.reserve(g_szTerarkPublikKey.size() + (g_szTerarkPublikKey.size() + 63) / 64 + 60);
  publicKey += "-----BEGIN PUBLIC KEY-----\n";
  for (size_t i = 0; i < g_szTerarkPublikKey.size(); i += 64) {
    publicKey.append(g_szTerarkPublikKey.data() + i,
                     std::min<size_t>(64, g_szTerarkPublikKey.size() - i));
    publicKey += '\n';
  }
  publicKey += "-----END PUBLIC KEY-----\n";
  BIO* pub_bio = BIO_new_mem_buf((void*) publicKey.data(), (int) publicKey.size());
  if (pub_bio == nullptr) {
    return InternalError;
  }
  RSA* pub = PEM_read_bio_RSA_PUBKEY(pub_bio, NULL, NULL, NULL);
  if (pub == nullptr) {
    BIO_free(pub_bio);
    return BadLicense;
  }
  ret = RSA_verify(NID_sha256, digest, sizeof digest,
                   (const byte_t*) strSign.data(), strSign.size(), pub);
  BIO_free(pub_bio);
  RSA_free(pub);
  if (ret != 1) {
    return BadLicense;
  }
#endif

#if defined(USE_CRYPTOPP) || defined(USE_OPENSSL)
  json signedDetail = json::parse(strData);
  key_storage.id_hash = XXHash64(kTerarkZipTableMagicNumber)
    .update(signedDetail["id"].get<std::string>())
    .digest();
  key_storage.date = signedDetail["dat"].get<uint64_t>();
  key_storage.duration = signedDetail["dur"].get<uint64_t>();
  key_storage.sign = XXHash64(kTerarkZipTableMagicNumber)
    .update(&key_storage.id_hash, sizeof key_storage.id_hash)
    .update(&key_storage.date, sizeof key_storage.date)
    .update(&key_storage.duration, sizeof key_storage.duration)
    .digest();
#endif
  key = &key_storage;
  head.sign = XXHash64(kTerarkZipTableMagicNumber)
    .update(key, sizeof *key)
    .update(sst, sizeof *sst)
    .digest();
  return OK;
}
catch (...) {
  return BadLicense;
}

LicenseInfo::Result LicenseInfo::merge(const void* data, size_t size) {
  if (size == 0) {
    return Result::OK;
  }
  if (size < sizeof(Head)) {
    return Result::BadStream;
  }
  Head in_head;
  memcpy(&in_head, data, sizeof(Head));
  const byte_t* l_ptr = (const byte_t*) data + sizeof(Head);
  size_t l_size = size - sizeof(Head);
  if (strncmp(in_head.meta, head.meta, 4) != 0 || in_head.size > size) {
    return Result::BadStream;
  }
  if (in_head.sign != XXHash64(kTerarkZipTableMagicNumber).update(l_ptr, l_size).digest()) {
    return Result::BadSign;
  }
  bool head_dirty = false;
  while (l_size) {
    if (l_size < sizeof(SubHead)) {
      return Result::BadStream;
    }
    SubHead sub_head;
    memcpy(&sub_head, l_ptr, sizeof(SubHead));
    if (strncmp(sub_head.name, key_storage.head.name, 4) == 0) {
      if (sub_head.version < 1) {
        //TODO old ver ???
        return Result::BadVersion;
      }
      if (sub_head.size > l_size || sub_head.size != sizeof(KeyInfo)) {
        return Result::BadStream;
      }
      KeyInfo key_info;
      memcpy(&key_info, l_ptr, sizeof(KeyInfo));
      if (key_info.sign != XXHash64(kTerarkZipTableMagicNumber)
        .update(&key_info.id_hash, sizeof key_info.id_hash)
        .update(&key_info.date, sizeof key_info.date)
        .update(&key_info.duration, sizeof key_info.duration)
        .digest()) {
        return Result::BadSign;
      }
      std::unique_lock<std::mutex> l(mutex);
      if (key == nullptr || key->date + key->duration < key_info.date + key_info.duration) {
        key_storage = key_info;
        key = &key_storage;
        head_dirty = true;
      }
    } else if (strncmp(sub_head.name, sst_storage.head.name, 4) == 0) {
      if (sub_head.version < 1) {
        //TODO old ver ???
        return Result::BadVersion;
      }
      if (sub_head.size > l_size || sub_head.size != sizeof(SstInfo)) {
        return Result::BadStream;
      }
      SstInfo sst_info;
      memcpy(&sst_info, l_ptr, sizeof(SstInfo));
      if (sst_info.sign != XXHash64(kTerarkZipTableMagicNumber)
        .update(&sst_info.create_date, sizeof sst_info.create_date)
        .digest()) {
        return Result::BadSign;
      }
      std::unique_lock<std::mutex> l(mutex);
      if (sst->create_date > sst_info.create_date) {
        *sst = sst_info;
        head_dirty = true;
      }
    } else {
      return Result::BadStream;
    }
    l_ptr += sub_head.size;
    l_size -= sub_head.size;
  }
  if (head_dirty) {
    XXHash64 hasher(kTerarkZipTableMagicNumber);
    if (key != nullptr) {
      hasher.update(key, sizeof *key);
    }
    hasher.update(sst, sizeof *sst);
    head.sign = hasher.digest();
  }
  return Result::OK;
}

valvec<byte_t> LicenseInfo::dump() const {
  std::unique_lock<std::mutex> l(mutex);
  valvec<byte_t> result;
  result.append((const byte_t*) &head, sizeof head);
  if (key) {
    result.append((const byte_t*) key, sizeof *key);
  }
  result.append((const byte_t*) sst, sizeof *sst);
  return result;
}

bool LicenseInfo::check() const {
#if defined(USE_CRYPTOPP) || defined(USE_OPENSSL)
  using namespace std::chrono;
  uint64_t now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
  if (key) {
    std::unique_lock<std::mutex> l(mutex);
    if (now > key->date + key->duration) {
      return false;
    }
  } else {
    std::unique_lock<std::mutex> l(mutex);
    if (now > sst->create_date + g_dTerarkTrialDuration) {
      return false;
    }
  }
#endif
  return true;
}

void LicenseInfo::print_error(const char* file_name, bool startup, rocksdb::Logger* logger) const {
  using namespace std::chrono;
  std::unique_lock<std::mutex> l(mutex);
#if _MSC_VER
# define RED_BEGIN ""
# define RED_END ""
#else
# define RED_BEGIN "\033[5;31;40m"
# define RED_END "\033[0m"
#endif
  uint64_t now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
  auto get_err_str = [](const char* msg, const char* file, uint64_t time) {
    std::stringstream info;
    info << RED_BEGIN;
    info << msg;
    if (file) {
      info << file;
    }
    if (time) {
      char buf[64];
      std::time_t rawtime = time / 1000;
      struct tm* timeinfo = gmtime(&rawtime);
      strftime(buf, sizeof(buf), "%F %T", timeinfo);
      info << buf;
    }
    info << " contact@terark.com";
    info << RED_END;
    return info.str();
  };
  std::string error_str;
  if (startup) {
    if (key == nullptr) {
      if (file_name && *file_name) {
        error_str = get_err_str("Bad license file ", file_name, 0);
      } else {
        error_str = get_err_str("Trial version", nullptr, 0);
      }
    } else {
      if (now > key->date + key->duration) {
        error_str = get_err_str("License expired at ", nullptr, key->date + key->duration);
      } else if (now + g_dTerarkTrialDuration > key->date + key->duration) {
        error_str = get_err_str("License will expired at ", nullptr, key->date + key->duration);
      }
    }
  } else {
    if (key != nullptr) {
      if (now > key->date + key->duration) {
        error_str = get_err_str("License expired at ", nullptr, key->date + key->duration);
      }
    } else {
      if (now > sst->create_date + g_dTerarkTrialDuration) {
        error_str = get_err_str("Trial expired at ", nullptr, sst->create_date + g_dTerarkTrialDuration);
      }
    }
  }
  if (!error_str.empty()) {
    fprintf(stderr, "%s\n", error_str.c_str());
    if (logger) {
      Warn(logger, error_str.c_str());
    }
  }
#undef RED_BEGIN
#undef RED_END
}

const size_t CollectInfo::queue_size = 1024;
const double CollectInfo::hard_ratio = 0.9;

void CollectInfo::update(uint64_t timestamp, size_t raw_value, size_t zip_value, size_t entropy, size_t zip_store) {
  std::unique_lock<std::mutex> l(mutex);
  raw_value_size += raw_value;
  zip_value_size += zip_value;
  entropy_size += entropy;
  zip_store_size += zip_store;
  auto comp = [](const CompressionInfo& l, const CompressionInfo& r) {
    return l.timestamp > r.timestamp;
  };
  queue.emplace_back(CompressionInfo{timestamp, raw_value, zip_value, entropy, zip_store});
  std::push_heap(queue.begin(), queue.end(), comp);
  while (queue.size() > queue_size) {
    auto& front = queue.front();
    raw_value_size -= front.raw_value;
    zip_value_size -= front.zip_value;
    entropy_size -= front.entropy;
    zip_store_size -= front.zip_store;
    std::pop_heap(queue.begin(), queue.end(), comp);
    queue.pop_back();
  }
  estimate_compression_ratio = float(zip_store_size) / float(entropy_size);
}

bool CollectInfo::hard(size_t raw, size_t zip) {
  return double(zip) > hard_ratio * double(raw);
}

bool CollectInfo::hard() const {
  std::unique_lock<std::mutex> l(mutex);
  return !queue.empty() && hard(raw_value_size, zip_value_size);
}

float CollectInfo::estimate(float def_value) const {
  float ret = estimate_compression_ratio;
  return ret ? ret : def_value;
}

size_t TerarkZipMultiOffsetInfo::calc_size(size_t partCount) {
  return 8 + partCount * sizeof(KeyValueOffset);
}

void TerarkZipMultiOffsetInfo::Init(size_t partCount) {
  offset_.resize_no_init(partCount);
}

void TerarkZipMultiOffsetInfo::set(size_t i, size_t k, size_t v, size_t t) {
  offset_[i].key = k;
  offset_[i].value = v;
  offset_[i].type = t;
}

valvec<byte_t> TerarkZipMultiOffsetInfo::dump() {
  valvec<byte_t> ret;
  size_t size = calc_size(offset_.size());
  ret.resize_no_init(size);
  size_t offset = 0;
  auto push = [&](const void* d, size_t s) {
    memcpy(ret.data() + offset, d, s);
    offset += s;
  };
  uint64_t partCount = offset_.size();
  push(&partCount, 8);
  push(offset_.data(), offset_.size() * sizeof(KeyValueOffset));
  assert(offset == ret.size());
  return ret;
}

bool TerarkZipMultiOffsetInfo::risk_set_memory(const void* p, size_t s) {
  offset_.clear();
  if (s < 8) {
    return false;
  }
  auto src = (const byte_t*)p;
  uint64_t partCount;
  memcpy(&partCount, src, 8);
  if (s != calc_size(partCount)) {
    return false;
  }
  offset_.risk_set_data((KeyValueOffset*)(src + 8), partCount);
  return true;
}

void TerarkZipMultiOffsetInfo::risk_release_ownership() {
  offset_.risk_release_ownership();
}

TableFactory*
NewTerarkZipTableFactory(const TerarkZipTableOptions& tzto,
                         std::shared_ptr<TableFactory> fallback) {
  TerarkZipTableFactory* factory = new TerarkZipTableFactory(tzto, fallback);
  if (tzto.debugLevel < 0) {
    STD_INFO("NewTerarkZipTableFactory(\n%s)\n",
             factory->GetPrintableTableOptions().c_str()
    );
  }
  auto& license = factory->GetLicense();
  std::string license_file = tzto.extendedConfigFile;
  if (license_file.empty()) {
    if (const char* env = getenv("TerarkZipTable_licenseFile")) {
      license_file = env;
    } else {
      FileStream f;
      if (f.xopen(".license", "rb")) {
        license_file = ".license";
      }
    }
  }
  if (!license_file.empty() && license.key == nullptr) {
    std::unique_lock<std::mutex> l(license.mutex);
    if (license.key == nullptr) {
      license.load_nolock(license_file);
    }
  }
  license.print_error(license_file.c_str(), true, nullptr);
  return factory;
}

std::shared_ptr<TableFactory>
SingleTerarkZipTableFactory(const TerarkZipTableOptions& tzto,
                            std::shared_ptr<TableFactory> fallback) {
  static std::shared_ptr<TableFactory> factory(
      NewTerarkZipTableFactory(tzto, fallback));
  return factory;
}

bool IsForwardBytewiseComparator(const Comparator* cmp) {
#if 1
  const fstring name = cmp->Name();
  if (name.startsWith("RocksDB_SE_")) {
    return true;
  }
  return name == "leveldb.BytewiseComparator";
#else
  return BytewiseComparator() == cmp;
#endif
}

bool IsBytewiseComparator(const Comparator* cmp) {
#if 1
  const fstring name = cmp->Name();
  if (name.startsWith("RocksDB_SE_")) {
    return true;
  }
  if (name.startsWith("rev:RocksDB_SE_")) {
    // reverse bytewise compare, needs reverse in iterator
    return true;
  }
# if defined(TERARK_SUPPORT_UINT64_COMPARATOR)
  if (name == "rocksdb.Uint64Comparator") {
    return true;
  }
# endif
  return name == "leveldb.BytewiseComparator";
#else
  return BytewiseComparator() == cmp;
#endif
}

inline static
size_t GetFixedPrefixLen(const SliceTransform* tr) {
  if (tr == nullptr) {
    return 0;
  }
  fstring trName = tr->Name();
  fstring namePrefix = "rocksdb.FixedPrefix.";
  if (namePrefix != trName.substr(0, namePrefix.size())) {
    return 0;
  }
  return terark::lcast(trName.substr(namePrefix.size()));
}

TerarkZipTableFactory::TerarkZipTableFactory(
    const TerarkZipTableOptions& tzto,
    std::shared_ptr<TableFactory> fallback)
    : table_options_(tzto), fallback_factory_(fallback) {
  adaptive_factory_ = NewAdaptiveTableFactory();
  if (tzto.minPreadLen >= 0 && tzto.cacheCapacityBytes) {
    cache_.reset(LruReadonlyCache::create(
        tzto.cacheCapacityBytes, tzto.cacheShards));
  }
}

TerarkZipTableFactory::~TerarkZipTableFactory() {
  delete adaptive_factory_;
}

Status
TerarkZipTableFactory::NewTableReader(
    const TableReaderOptions& table_reader_options,
    unique_ptr <RandomAccessFileReader>&& file,
    uint64_t file_size, unique_ptr <TableReader>* table,
    bool prefetch_index_and_filter_in_cache)
const {
  PrintVersionHashInfo(table_reader_options.ioptions.info_log);
  auto userCmp = table_reader_options.internal_comparator.user_comparator();
  if (!IsBytewiseComparator(userCmp)) {
    return Status::InvalidArgument("TerarkZipTableFactory::NewTableReader()",
                                   "user comparator must be 'leveldb.BytewiseComparator'");
  }
  Footer footer;
  Status s = ReadFooterFromFile(file.get(), TERARK_ROCKSDB_5007(nullptr,)
                                file_size, &footer);
  if (!s.ok()) {
    return s;
  }
  if (footer.table_magic_number() != kTerarkZipTableMagicNumber) {
    if (adaptive_factory_) {
      // just for open table
      return adaptive_factory_->NewTableReader(table_reader_options,
                                               std::move(file), file_size, table,
                                               prefetch_index_and_filter_in_cache);
    }
    if (fallback_factory_) {
      return fallback_factory_->NewTableReader(table_reader_options,
                                               std::move(file), file_size, table,
                                               prefetch_index_and_filter_in_cache);
    }
    return Status::InvalidArgument(
      "TerarkZipTableFactory::NewTableReader()",
      "fallback_factory is null and magic_number is not kTerarkZipTable"
    );
  }
#if 0
  if (!prefetch_index_and_filter_in_cache) {
    WARN(table_reader_options.ioptions.info_log
      , "TerarkZipTableFactory::NewTableReader(): "
      "prefetch_index_and_filter_in_cache = false is ignored, "
      "all index and data will be loaded in memory\n");
  }
#endif
  BlockContents emptyTableBC;
  s = ReadMetaBlockAdapte(file.get(), file_size, kTerarkZipTableMagicNumber, table_reader_options.ioptions,
                          kTerarkEmptyTableKey, &emptyTableBC);
  if (s.ok()) {
    std::unique_ptr<TerarkEmptyTableReader>
      t(new TerarkEmptyTableReader(this, table_reader_options));
    s = t->Open(file.release(), file_size);
    if (!s.ok()) {
      return s;
    }
    *table = std::move(t);
    return s;
  }
  BlockContents offsetBC;
  s = ReadMetaBlockAdapte(file.get(), file_size, kTerarkZipTableMagicNumber, table_reader_options.ioptions,
                          kTerarkZipTableOffsetBlock, &offsetBC);
  if (s.ok()) {
    TerarkZipMultiOffsetInfo info;
    if (info.risk_set_memory(offsetBC.data.data(), offsetBC.data.size())) {
      if (info.offset_.size() > 1) {
        std::unique_ptr<TerarkZipTableMultiReader>
          t(new TerarkZipTableMultiReader(this, table_reader_options, table_options_));
        s = t->Open(file.release(), file_size);
        if (s.ok()) {
          *table = std::move(t);
        }
      } else {
        std::unique_ptr<TerarkZipTableReader>
          t(new TerarkZipTableReader(this, table_reader_options, table_options_));
        s = t->Open(file.release(), file_size);
        if (s.ok()) {
          *table = std::move(t);
        }
      }
      info.risk_release_ownership();
      return s;
    }
    return Status::InvalidArgument(
      "TerarkZipTableFactory::NewTableReader()", "bad TerarkZipMultiOffsetInfo"
    );
  }
  return Status::InvalidArgument(
    "TerarkZipTableFactory::NewTableReader()", "missing TerarkZipMultiOffsetInfo"
  );
}

// defined in terark_zip_table_builder.cc
extern
TableBuilder*
createTerarkZipTableBuilder(const TerarkZipTableFactory* table_factory,
                            const TerarkZipTableOptions& tzo,
                            const TableBuilderOptions& tbo,
                            uint32_t column_family_id,
                            WritableFileWriter* file,
                            uint32_t key_prefixLen);
extern long long g_lastTime;

TableBuilder*
TerarkZipTableFactory::NewTableBuilder(
  const TableBuilderOptions& table_builder_options,
  uint32_t column_family_id,
  WritableFileWriter* file)
const {
  PrintVersionHashInfo(table_builder_options.ioptions.info_log);
  auto userCmp = table_builder_options.internal_comparator.user_comparator();
  if (!IsBytewiseComparator(userCmp)) {
    THROW_STD(invalid_argument,
              "TerarkZipTableFactory::NewTableBuilder(): "
              "user comparator must be 'leveldb.BytewiseComparator'");
  }
  int curlevel = table_builder_options.level;
  int numlevel = table_builder_options.ioptions.num_levels;
  int minlevel = table_options_.terarkZipMinLevel;
  if (minlevel < 0) {
    minlevel = numlevel - 1;
  }
  uint32_t keyPrefixLen = 0;
  if (table_options_.isOfflineBuild) {
    if (table_options_.keyPrefixLen != 0) {
      WARN(table_builder_options.ioptions.info_log,
           "TerarkZipTableFactory::NewTableBuilder() OfflineBuild unsupport keyPrefixLen , keyPrefixLen = %zd\n",
           table_options_.keyPrefixLen
      );
    }
  } else {
    keyPrefixLen = GetFixedPrefixLen(table_builder_options.moptions.prefix_extractor.get());
    if (keyPrefixLen != 0) {
      if (table_options_.keyPrefixLen != 0) {
        if (keyPrefixLen != table_options_.keyPrefixLen) {
          WARN(table_builder_options.ioptions.info_log,
               "TerarkZipTableFactory::NewTableBuilder() found non best config , keyPrefixLen = %zd , prefix_extractor = %zd\n",
               table_options_.keyPrefixLen, keyPrefixLen
          );
        }
        keyPrefixLen = std::min<uint32_t>(keyPrefixLen, table_options_.keyPrefixLen);
      }
    } else {
      keyPrefixLen = table_options_.keyPrefixLen;
    }
  }
  if (table_builder_options.sst_purpose != kEssenceSst) {
    keyPrefixLen = 0;
  }
#if defined(TERARK_SUPPORT_UINT64_COMPARATOR) && BOOST_ENDIAN_LITTLE_BYTE
  if (fstring(userCmp->Name()) == "rocksdb.Uint64Comparator") {
    keyPrefixLen = 0;
  }
#endif
#if 1
  INFO(table_builder_options.ioptions.info_log,
       "nth_newtable{ terark = %3zd fallback = %3zd } curlevel = %d minlevel = %d numlevel = %d fallback = %p\n",
       nth_new_terark_table_, nth_new_fallback_table_, curlevel, minlevel, numlevel, fallback_factory_.get()
  );
#endif
  if (0 == nth_new_terark_table_) {
    g_lastTime = g_pf.now();
  }
  if (fallback_factory_) {
    if (curlevel >= 0 && curlevel < minlevel) {
      nth_new_fallback_table_++;
      TableBuilder* tb = fallback_factory_->NewTableBuilder(table_builder_options,
                                                            column_family_id, file);
      INFO(table_builder_options.ioptions.info_log, "TerarkZipTableFactory::NewTableBuilder() returns class: %s\n",
           ClassName(*tb).c_str());
      return tb;
    }
  }
  nth_new_terark_table_++;

  return createTerarkZipTableBuilder(
    this,
    table_options_,
    table_builder_options,
    column_family_id,
    file,
    keyPrefixLen);
}

#define PrintBuf(...) ret.append(buffer, snprintf(buffer, kBufferSize, __VA_ARGS__))

std::string TerarkZipTableFactory::GetPrintableTableOptions() const {
  std::string ret;
  ret.reserve(2000);
  const char* cvb[] = {"false", "true"};
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  const auto& tzto = table_options_;

#define M_String(name)                                       \
  ret.append(#name);                                         \
  ret.append("                         : " + strlen(#name)   \
                                      , 27 - strlen(#name)); \
  ret.append(tzto.name);                                     \
  ret.append("\n")

#define M_NumFmt(name, fmt) \
                       PrintBuf("%-24s : " fmt "\n", #name, tzto.name)
#define M_NumGiB(name) PrintBuf("%-24s : %.3fGiB\n", #name, tzto.name/GiB)
#define M_Boolea(name) PrintBuf("%-24s : %s\n", #name, cvb[!!tzto.name])
#include "terark_zip_table_property_print.h"
  return ret;
}

Status
TerarkZipTableFactory::GetOptionString(std::string* opt_string,
                                       const std::string& delimiter)
const {
  const char* cvb[] = {"false", "true"};
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  const auto& tzto = table_options_;

  std::string& ret = *opt_string;
  ret.resize(0);

#define WriteName(name) ret.append(#name).append("=")

#define M_String(name) WriteName(name).append(tzto.name).append(delimiter)
#define M_NumFmt(name, fmt) \
                       WriteName(name);PrintBuf(fmt, tzto.name); \
                       ret.append(delimiter)
#define M_NumGiB(name) WriteName(name);PrintBuf("%.3fGiB", tzto.name/GiB);\
                       ret.append(delimiter)
#define M_Boolea(name) WriteName(name);PrintBuf("%s", cvb[!!tzto.name]); \
                       ret.append(delimiter)

#include "terark_zip_table_property_print.h"

  return Status::OK();
}

Status
TerarkZipTableFactory::SanitizeOptions(const DBOptions& db_opts,
                                       const ColumnFamilyOptions& cf_opts)
const {
  auto table_factory = dynamic_cast<TerarkZipTableFactory*>(cf_opts.table_factory.get());
  assert(table_factory);
  auto& tzto = *reinterpret_cast<const TerarkZipTableOptions*>(table_factory->GetOptions());
  try {
    TempFileDeleteOnClose test;
    test.path = tzto.localTempDir + "/Terark-XXXXXX";
    test.open_temp();
    test.writer << "Terark";
    test.complete_write();
  }
  catch (...) {
    std::string msg = "ERROR: bad localTempDir : " + tzto.localTempDir;
    fprintf(stderr, "%s\n", msg.c_str());
    return Status::InvalidArgument("TerarkZipTableFactory::SanitizeOptions()", msg);
  }
  if (!IsBytewiseComparator(cf_opts.comparator)) {
    return Status::InvalidArgument("TerarkZipTableFactory::SanitizeOptions()",
                                   "user comparator must be 'leveldb.BytewiseComparator'");
  }
  return Status::OK();
}

bool TerarkZipTablePrintCacheStat(TableFactory* factory, FILE* fp) {
  auto tztf = dynamic_cast<const TerarkZipTableFactory*>(factory);
  if (tztf) {
    if (tztf->cache()) {
      tztf->cache()->print_stat_cnt(fp);
      return true;
    } else {
      fprintf(fp, "PrintCacheStat: terark user cache == nullptr\n");
    }
  } else {
    fprintf(fp, "PrintCacheStat: factory is not TerarkZipTableFactory\n");
  }
  return false;
}

} /* namespace rocksdb */
