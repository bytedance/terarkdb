/*
 * terark_zip_table.h
 *
 *  Created on: 2016Äê8ÔÂ9ÈÕ
 *      Author: leipeng
 */

#pragma once

#ifndef TERARK_ZIP_TABLE_H_
#define TERARK_ZIP_TABLE_H_

#include <string>

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

  bool useSuffixArrayLocalMatch = false;
  bool isOfflineBuild = false;
  bool warmUpIndexOnOpen = true;
  bool warmUpValueOnOpen = false;
  bool useAsyncKeyValueReader = true;

  float estimateCompressionRatio = 0.2;
  double sampleRatio = 0.03;
  std::string localTempDir = "/tmp";
  std::string indexType = "IL_256";

  size_t softZipWorkingMemLimit = 16ull << 30;
  size_t hardZipWorkingMemLimit = 32ull << 30;
  size_t smallTaskMemory = 1200 << 20; // 1.2G

  // should be a small value, typically 0.001
  // default is to disable indexCache, because the improvement
  // is about only 10% when set to 0.001
  double indexCacheRatio = 0;//0.001;
};

class TableFactory*
NewTerarkZipTableFactory(const TerarkZipTableOptions&,
						 class TableFactory* fallback);

}  // namespace rocksdb

#endif /* TERARK_ZIP_TABLE_H_ */
