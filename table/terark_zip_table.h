/*
 * terark_zip_table.h
 *
 *  Created on: 2016-08-09
 *      Author: leipeng
 */

#pragma once

#ifndef TERARK_ZIP_TABLE_H_
#define TERARK_ZIP_TABLE_H_

#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <stdio.h>

namespace rocksdb {

struct TerarkZipTableOptions {
  // copy of DictZipBlobStore::Options::EntropyAlgo
  enum EntropyAlgo : uint32_t {
    kNoEntropy,
    kHuffman,
    kFSE,
  };
  int32_t     indexNestLevel           = 3;

  /// 0 : check sum nothing
  /// 1 : check sum meta data and index, check on file load
  /// 2 : check sum all data, not check on file load, checksum is for
  ///     each record, this incurs 4 bytes overhead for each record
  /// 3 : check sum all data with one checksum value, not checksum each record,
  ///     if checksum doesn't match, load will fail
  int32_t     checksumLevel            = 1;
  /// checksumSmallValSize only makes sense when checksumLevel == 2(record level)
  /// any value that is less than checksumSmallValSize will be verified by crc16c otherwise crc32c is used
  int32_t     checksumSmallValSize     = 40;
  EntropyAlgo entropyAlgo              = kNoEntropy;

  ///    < 0 : only last level using terarkZip
  ///          this is equivalent to terarkZipMinLevel == num_levels-1
  /// others : use terarkZip when curlevel >= terarkZipMinLevel
  ///          this includes the two special cases:
  ///                   == 0 : all levels using terarkZip
  ///          >= num_levels : all levels using fallback TableFactory
  /// it shown that set terarkZipMinLevel = 0 is the best choice
  /// if mixed with rocksdb's native SST, those SSTs may using too much
  /// memory & SSD, which degrades the performance
  int         terarkZipMinLevel = 0;

  /// please always set to 0
  /// unless you know what it really does for
  /// 0 : no debug
  /// 1 : enable infomation output
  /// 2 : verify 2nd pass iter keys & values
  /// 3 : dump 1st & 2nd pass data to file
  uint8_t     debugLevel               = 0;
  uint8_t     indexNestScale           = 8;

  /// -1: dont use temp file for  any  index build
  ///  0: only use temp file for large index build, smart
  ///  1: only use temp file for large index build, same as NLT.tmpLevel
  ///  2: only use temp file for large index build, same as NLT.tmpLevel
  ///  3: only use temp file for large index build, same as NLT.tmpLevel
  ///  4: only use temp file for large index build, same as NLT.tmpLevel
  ///  5:      use temp file for  all  index build
  int8_t      indexTempLevel           = 0;
  bool        enableCompressionProbe   = true;
  bool        useSuffixArrayLocalMatch = false;
  bool        warmUpIndexOnOpen        = true;
  bool        warmUpValueOnOpen        = false;
  bool        disableSecondPassIter    = false;
  bool        disableCompressDict      = false;
  bool        optimizeCpuL3Cache       = true;
  bool        forceMetaInMemory        = false;
  bool        enableEntropyStore       = true;
  uint8_t     reserveBytes0[6]         = {};
  uint16_t    offsetArrayBlockUnits    = 0;


  double      sampleRatio              = 0.03;
  // should be a small value, typically 0.001
  // default is to disable indexCache, because the improvement
  // is about only 10% when set to 0.001
  double      indexCacheRatio          = 0;  //0.001;
  std::string localTempDir             = "/tmp";
  std::string indexType                = "Mixed_XL_256_32_FL";

  uint64_t    softZipWorkingMemLimit   = 16ull << 30;
  uint64_t    hardZipWorkingMemLimit   = 32ull << 30;
  uint64_t    smallTaskMemory          = 1200 << 20; // 1.2G
  // use dictZip for value when average value length >= minDictZipValueSize
  // otherwise do not use dictZip
  uint32_t    minDictZipValueSize      = 15;
  uint32_t    keyPrefixLen             = 0; // for IndexID

  uint64_t    singleIndexMinSize       = 8ULL << 20; // 8M
  uint64_t    singleIndexMaxSize       = 0x1E0000000; // 7.5G

  ///  < 0: do not use pread
  /// == 0: always use pread
  ///  > 0: use pread if BlobStore avg record len > minPreadLen
  int32_t     minPreadLen              = 0;
  int32_t     cacheShards              = 17; // to reduce lock competition
  uint64_t    cacheCapacityBytes       = 0;  // non-zero implies direct io read
  uint8_t     reserveBytes1[24]        = {};
};

void TerarkZipDeleteTempFiles(const std::string& tmpPath);

/// @memBytesLimit total memory can be used for the whole process
///   memBytesLimit == 0 indicate all physical memory can be used
void TerarkZipAutoConfigForBulkLoad(struct TerarkZipTableOptions&,
                                    struct DBOptions&,
                                    struct ColumnFamilyOptions&,
                                    size_t cpuNum = 0,
                                    size_t memBytesLimit = 0,
                                    size_t diskBytesLimit = 0);

void TerarkZipAutoConfigForOnlineDB(struct TerarkZipTableOptions&,
                                    struct DBOptions&,
                                    struct ColumnFamilyOptions&,
                                    size_t cpuNum = 0,
                                    size_t memBytesLimit = 0,
                                    size_t diskBytesLimit = 0);

void
TerarkZipAutoConfigForOnlineDB_DBOptions(struct DBOptions& dbo, size_t cpuNum = 0);

void
TerarkZipAutoConfigForOnlineDB_CFOptions(struct TerarkZipTableOptions& tzo,
                                         struct ColumnFamilyOptions& cfo,
                                         size_t memBytesLimit = 0,
                                         size_t diskBytesLimit = 0);

bool TerarkZipConfigFromEnv(struct DBOptions&, struct ColumnFamilyOptions&);

bool TerarkZipCFOptionsFromEnv(struct ColumnFamilyOptions&);

void TerarkZipDBOptionsFromEnv(struct DBOptions&);

bool TerarkZipIsBlackListCF(const std::string& cfname);

/// will check column family black list in env
///@param db_options is const but will be modified in this function
///@param cfvec      is const but will be modified in this function
void
TerarkZipMultiCFOptionsFromEnv(const struct DBOptions& db_options,
                               const std::vector<struct ColumnFamilyDescriptor>& cfvec);

const class WriteBatchEntryIndexFactory*
patricia_WriteBatchEntryIndexFactory(const WriteBatchEntryIndexFactory* fallback = nullptr);

class MemTableRepFactory*
NewPatriciaTrieRepFactory(std::shared_ptr<class MemTableRepFactory> fallback = nullptr);

class MemTableRepFactory*
NewPatriciaTrieRepFactory(const std::unordered_map<std::string, std::string>& options, class Status* s);

class TableFactory*
NewTerarkZipTableFactory(const TerarkZipTableOptions&,
                         std::shared_ptr<class TableFactory> fallback);

std::shared_ptr<class TableFactory>
SingleTerarkZipTableFactory(const TerarkZipTableOptions&,
                            std::shared_ptr<class TableFactory> fallback);

bool TerarkZipTablePrintCacheStat(const class TableFactory*, FILE*);

}  // namespace rocksdb

#endif /* TERARK_ZIP_TABLE_H_ */
