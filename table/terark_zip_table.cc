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
#include "terark_zip_table_builder.h"
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
#if defined(TerocksPrivateCode)
# include <boost/archive/iterators/binary_from_base64.hpp>
# include <boost/archive/iterators/transform_width.hpp>
#endif // TerocksPrivateCode

// rocksdb headers
#include <table/meta_blocks.h>

// terark headers
#if defined(TerocksPrivateCode)
# include <terark/zbs/xxhash_helper.hpp>
#endif // TerocksPrivateCode

// 3rd-party headers
#if defined(TerocksPrivateCode)
# include <nlohmann/json.hpp>
# ifdef USE_CRYPTOPP
#   include <cryptopp/cryptlib.h>
#   include <cryptopp/osrng.h>
#   include <cryptopp/base64.h>
#   include <cryptopp/filters.h>
#   include <cryptopp/eccrypto.h>
#   include <cryptopp/gfpcrypt.h>
#   include <cryptopp/rsa.h>
# endif
# ifdef USE_OPENSSL
extern "C"
{
#   include <openssl/sha.h>
#   include <openssl/rsa.h>
#   include <openssl/rand.h>
#   include <openssl/objects.h>
#   include <openssl/pem.h>
#   include <openssl/bio.h>
}
# endif
#endif // TerocksPrivateCode


#if defined(TerocksPrivateCode)

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
  }
  else if (!base64.empty() && base64.back() == '=') {
    ret.pop_back();
  }
  return ret;
}

#endif // TerocksPrivateCode




#ifdef TERARK_SUPPORT_UINT64_COMPARATOR
# if !BOOST_ENDIAN_LITTLE_BYTE && !BOOST_ENDIAN_BIG_BYTE
#   error Unsupported endian !
# endif
#endif

namespace rocksdb {

terark::profiling g_pf;

const uint64_t kTerarkZipTableMagicNumber = 0x1122334455667788;

const std::string kTerarkZipTableIndexBlock = "TerarkZipTableIndexBlock";
const std::string kTerarkZipTableValueTypeBlock = "TerarkZipTableValueTypeBlock";
const std::string kTerarkZipTableValueDictBlock = "TerarkZipTableValueDictBlock";
const std::string kTerarkZipTableCommonPrefixBlock = "TerarkZipTableCommonPrefixBlock";
const std::string kTerarkEmptyTableKey = "ThisIsAnEmptyTable";

#if defined(TerocksPrivateCode)

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

#endif // TerocksPrivateCode

#if defined(TerocksPrivateCode)

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

LicenseInfo::Result LicenseInfo::load_nolock(const std::string& license_file) {
  using json = nlohmann::json;
  try {
    json signedJson = json::parse(std::ifstream(license_file, std::ios::binary));
    std::string strSign = base64_decode(signedJson["sign"].get<std::string>());
    std::string strData = base64_decode(signedJson["data"].get<std::string>());
#ifdef USE_CRYPTOPP
    using namespace CryptoPP;
    std::string binPubKey = base64_decode(g_publikKey);
    StringSource pubFile(binPubKey, true, nullptr);
    RSASS<PKCS1v15, SHA256>::Verifier pub(pubFile);

    StringSource signedSource(strSign, true, nullptr);
    if (signedSource.MaxRetrievable() != pub.SignatureLength())
    {
      return BadLicense;
    }
    SecByteBlock signature(pub.SignatureLength());
    signedSource.Get(signature, signature.size());

    if (!pub.VerifyMessage((const byte*)strData.data(), strData.size(),
      signature.data(), signature.size())) {
      return BadLicense;
    }
#endif
#ifdef USE_OPENSSL
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
    BIO* pub_bio = BIO_new_mem_buf((void*)publicKey.data(), (int)publicKey.size());
    if (pub_bio == nullptr) {
      return InternalError;
    }
    RSA* pub = PEM_read_bio_RSA_PUBKEY(pub_bio, NULL, NULL, NULL);
    if (pub == nullptr) {
      BIO_free(pub_bio);
      return BadLicense;
    }
    ret = RSA_verify(NID_sha256, digest, sizeof digest,
      (const byte_t*)strSign.data(), strSign.size(), pub);
    BIO_free(pub_bio);
    RSA_free(pub);
    if (ret != 1) {
      return BadLicense;
    }
#endif
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
  }
  catch (...) {
    return BadLicense;
  }
  key = &key_storage;
  head.sign = XXHash64(kTerarkZipTableMagicNumber)
    .update(key, sizeof *key)
    .update(sst, sizeof *sst)
    .digest();
  return OK;
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
  const byte_t *l_ptr = (const byte_t*)data + sizeof(Head);
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
    }
    else if (strncmp(sub_head.name, sst_storage.head.name, 4) == 0) {
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
    }
    else {
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
  result.append((const byte_t*)&head, sizeof head);
  if (key) {
    result.append((const byte_t*)key, sizeof *key);
  }
  result.append((const byte_t*)sst, sizeof *sst);
  return result;
}

bool LicenseInfo::check() const {
  using namespace std::chrono;
  uint64_t now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
  if (key) {
    std::unique_lock<std::mutex> l(mutex);
    if (now > key->date + key->duration) {
      return false;
    }
  }
  else {
    std::unique_lock<std::mutex> l(mutex);
    if (now > sst->create_date + g_dTerarkTrialDuration) {
      return false;
    }
  }
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
  auto get_err_str = [](const char* msg, const char *file, uint64_t time) {
    std::stringstream info;
    info << RED_BEGIN;
    info << msg;
    if (file) {
      info << file;
    }
    if (time) {
      thread_local char buf[64];
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
      }
      else {
        error_str = get_err_str("Trial version", nullptr, 0);
      }
    }
    else {
      if (now > key->date + key->duration) {
        error_str = get_err_str("License expired at ", nullptr, key->date + key->duration);
      }
      else if (now + g_dTerarkTrialDuration > key->date + key->duration) {
        error_str = get_err_str("License will expired at ", nullptr, key->date + key->duration);
      }
    }
  }
  else {
    if (key != nullptr) {
      if (now > key->date + key->duration) {
        error_str = get_err_str("License expired at ", nullptr, key->date + key->duration);
      }
    }
    else {
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

#endif // TerocksPrivateCode


class TableFactory*
  NewTerarkZipTableFactory(const TerarkZipTableOptions& tzto,
    class TableFactory* fallback) {
  int err = 0;
  try {
    TempFileDeleteOnClose test;
    test.path = tzto.localTempDir + "/Terark-XXXXXX";
    test.open_temp();
    test.writer << "Terark";
    test.complete_write();
  }
  catch (...) {
    fprintf(stderr
      , "ERROR: bad localTempDir %s %s\n"
      , tzto.localTempDir.c_str(), err ? strerror(err) : "");
    abort();
  }
  TerarkZipTableFactory* factory = new TerarkZipTableFactory(tzto, fallback);
  if (tzto.debugLevel < 0) {
    STD_INFO("NewTerarkZipTableFactory(\n%s)\n",
      factory->GetPrintableTableOptions().c_str()
    );
  }
#if defined(TerocksPrivateCode)
  auto& license = factory->GetLicense();
  if (!tzto.extendedConfigFile.empty() && license.key == nullptr) {
    std::unique_lock<std::mutex> l(license.mutex);
    if (license.key == nullptr) {
      license.load_nolock(tzto.extendedConfigFile);
    }
  }
  license.print_error(tzto.extendedConfigFile.c_str(), true, nullptr);
#endif // TerocksPrivateCode
  return factory;
}

inline static
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

Status
TerarkZipTableFactory::NewTableReader(
  const TableReaderOptions& table_reader_options,
  unique_ptr<RandomAccessFileReader>&& file,
  uint64_t file_size, unique_ptr<TableReader>* table,
  bool prefetch_index_and_filter_in_cache)
  const {
  auto userCmp = table_reader_options.internal_comparator.user_comparator();
  if (!IsBytewiseComparator(userCmp)) {
    return Status::InvalidArgument("TerarkZipTableFactory::NewTableReader()",
      "user comparator must be 'leveldb.BytewiseComparator'");
  }
  Footer footer;
  Status s = ReadFooterFromFile(file.get(), file_size, &footer);
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
  s = ReadMetaBlock(file.get(), file_size, kTerarkZipTableMagicNumber
    , table_reader_options.ioptions, kTerarkEmptyTableKey, &emptyTableBC);
  if (s.ok()) {
    std::unique_ptr<TerarkEmptyTableReader>
      t(new TerarkEmptyTableReader(table_reader_options));
    s = t->Open(file.release(), file_size);
    if (!s.ok()) {
      return s;
    }
    *table = std::move(t);
    return s;
  }
  std::unique_ptr<TerarkZipTableReader>
    t(new TerarkZipTableReader(table_reader_options, table_options_));
  s = t->Open(file.release(), file_size);
  if (s.ok()) {
    *table = std::move(t);
  }
  return s;
}

TableBuilder*
TerarkZipTableFactory::NewTableBuilder(
  const TableBuilderOptions& table_builder_options,
  uint32_t column_family_id,
  WritableFileWriter* file)
  const {
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
#if 1
  INFO(table_builder_options.ioptions.info_log
    , "nth_newtable{ terark = %3zd fallback = %3zd } curlevel = %d minlevel = %d numlevel = %d fallback = %p\n"
    , nth_new_terark_table_, nth_new_fallback_table_, curlevel, minlevel, numlevel, fallback_factory_
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
      INFO(table_builder_options.ioptions.info_log
        , "TerarkZipTableFactory::NewTableBuilder() returns class: %s\n"
        , ClassName(*tb).c_str());
      return tb;
    }
  }
  nth_new_terark_table_++;
  return new TerarkZipTableBuilder(
    table_options_,
    table_builder_options,
    column_family_id,
    file);
}

std::string TerarkZipTableFactory::GetPrintableTableOptions() const {
  std::string ret;
  ret.reserve(2000);
  const char* cvb[] = {"false", "true"};
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  const auto& tzto = table_options_;
  const double gb = 1ull << 30;

  ret += "localTempDir             : ";
  ret += tzto.localTempDir;
  ret += '\n';

#ifdef M_APPEND
# error WTF ?
#endif
#define M_APPEND(fmt, value) \
ret.append(buffer, snprintf(buffer, kBufferSize, fmt "\n", value))

  M_APPEND("indexType                : %s", tzto.indexType.c_str());
  M_APPEND("checksumLevel            : %d", tzto.checksumLevel);
  M_APPEND("entropyAlgo              : %d", (int)tzto.entropyAlgo);
  M_APPEND("indexNestLevel           : %d", tzto.indexNestLevel);
  M_APPEND("terarkZipMinLevel        : %d", tzto.terarkZipMinLevel);
  M_APPEND("debugLevel               : %d", tzto.debugLevel);
  M_APPEND("useSuffixArrayLocalMatch : %s", cvb[!!tzto.useSuffixArrayLocalMatch]);
  M_APPEND("warmUpIndexOnOpen        : %s", cvb[!!tzto.warmUpIndexOnOpen]);
  M_APPEND("warmUpValueOnOpen        : %s", cvb[!!tzto.warmUpValueOnOpen]);
  M_APPEND("disableSecondPassIter    : %s", cvb[!!tzto.disableSecondPassIter]);
  M_APPEND("estimateCompressionRatio : %f", tzto.estimateCompressionRatio);
  M_APPEND("sampleRatio              : %f", tzto.sampleRatio);
  M_APPEND("indexCacheRatio          : %f", tzto.indexCacheRatio);
  M_APPEND("softZipWorkingMemLimit   : %.3fGB", tzto.softZipWorkingMemLimit / gb);
  M_APPEND("hardZipWorkingMemLimit   : %.3fGB", tzto.hardZipWorkingMemLimit / gb);
  M_APPEND("smallTaskMemory          : %.3fGB", tzto.smallTaskMemory / gb);

#undef M_APPEND

  return ret;
}

Status
TerarkZipTableFactory::SanitizeOptions(const DBOptions& db_opts,
  const ColumnFamilyOptions& cf_opts)
  const {
  if (!IsBytewiseComparator(cf_opts.comparator)) {
    return Status::InvalidArgument("TerarkZipTableFactory::SanitizeOptions()",
      "user comparator must be 'leveldb.BytewiseComparator'");
  }
  auto indexFactory = TerarkIndex::GetFactory(table_options_.indexType);
  if (!indexFactory) {
    std::string msg = "invalid indexType: " + table_options_.indexType;
    return Status::InvalidArgument(msg);
  }
  return Status::OK();
}

} /* namespace rocksdb */
