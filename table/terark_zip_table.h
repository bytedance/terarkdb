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
#include <vector>


#define TerocksPrivateCode
#if defined(TerocksPrivateCode)
// this macro for remove private code ...
#endif // TerocksPrivateCode

namespace rocksdb {

struct TerarkZipTableOptions {
  // copy of DictZipBlobStore::Options::EntropyAlgo
  enum EntropyAlgo {
    kNoEntropy,
    kHuffman,
    kFSE,
  };
  int indexNestLevel = 3;

  /// 0 : check sum nothing
  /// 1 : check sum meta data and index, check on file load
  /// 2 : check sum all data, not check on file load, checksum is for
  ///     each record, this incurs 4 bytes overhead for each record
  /// 3 : check sum all data with one checksum value, not checksum each record,
  ///     if checksum doesn't match, load will fail
  int checksumLevel = 1;

  EntropyAlgo entropyAlgo = kNoEntropy;

  ///    < 0 : only last level using terarkZip
  ///          this is equivalent to terarkZipMinLevel == num_levels-1
  /// others : use terarkZip when curlevel >= terarkZipMinLevel
  ///          this includes the two special cases:
  ///                   == 0 : all levels using terarkZip
  ///          >= num_levels : all levels using fallback TableFactory
  /// it shown that set terarkZipMinLevel = 0 is the best choice
  /// if mixed with rocksdb's native SST, those SSTs may using too much
  /// memory & SSD, which degrades the performance
  int terarkZipMinLevel = 0;

  /// please always set to 0
  /// unless you know what it really does for
  /// 0 : no debug
  /// 1 : enable infomation output
  /// 2 : verify 2nd pass iter keys
  /// 3 : verify 2nd pass iter keys & values
  /// 4 : dump 1st & 2nd pass data to file
  int debugLevel = 0;

  bool useSuffixArrayLocalMatch = false;
  bool isOfflineBuild = false;
  bool warmUpIndexOnOpen = true;
  bool warmUpValueOnOpen = false;
  bool disableSecondPassIter = false;

  /// -1: dont use temp file for  any  index build
  ///  0: only use temp file for large index build, smart
  ///  1: only use temp file for large index build, same as NLT.tmpLevel
  ///  2: only use temp file for large index build, same as NLT.tmpLevel
  ///  3: only use temp file for large index build, same as NLT.tmpLevel
  ///  4:      use temp file for  all  index build
  signed char indexTempLevel = 0;

  unsigned short offsetArrayBlockUnits = 0;

  float estimateCompressionRatio = 0.2f;
  double sampleRatio = 0.03;
  std::string localTempDir = "/tmp";
  std::string indexType = "IL_256";
  std::string extendedConfigFile;

  size_t softZipWorkingMemLimit = 16ull << 30;
  size_t hardZipWorkingMemLimit = 32ull << 30;
  size_t smallTaskMemory = 1200 << 20; // 1.2G
#if defined(TerocksPrivateCode)
  // these fields is only for TerocksPrivateCode
  // always keep these fields for binary compatibility
#endif // TerocksPrivateCode
  // use dictZip for value when average value length >= minDictZipValueSize
  // otherwise do not use dictZip
  size_t minDictZipValueSize = 30;
  size_t keyPrefixLen = 0; // for IndexID
#if defined(TerocksPrivateCode)
  // end fields for TerocksPrivateCode
#endif // TerocksPrivateCode

  // should be a small value, typically 0.001
  // default is to disable indexCache, because the improvement
  // is about only 10% when set to 0.001
  double indexCacheRatio = 0;//0.001;
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

///@param fallback take ownership of fallback
class TableFactory*
NewTerarkZipTableFactory(const TerarkZipTableOptions&,
						 class TableFactory* fallback);

}  // namespace rocksdb

#endif /* TERARK_ZIP_TABLE_H_ */
