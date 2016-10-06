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
	unsigned fixed_key_len = 0;
	int indexNestLevel = 3;

	/// 0 : check sum nothing
	/// 1 : check sum meta data and index, check on file load
	/// 2 : check sum all data, not check on file load, check on record read
	int checksumLevel = 1;

	EntropyAlgo entropyAlgo = kNoEntropy;

	///    < 0 : only last level using terarkZip, this is the default
	///          this is equivalent to terarkZipMinLevel == num_levels-1
	/// others : use terarkZip when curlevel >= terarkZipMinLevel
	///          this includes the two special cases:
	///                   == 0 : all levels using terarkZip
	///          >= num_levels : all levels using fallback TableFactory
	int terarkZipMinLevel = -1;

	bool useSuffixArrayLocalMatch = false;

	float estimateCompressionRatio = 0.2;
	double sampleRatio = 0.03;
	std::string localTempDir = "/tmp";
};

class TableFactory*
NewTerarkZipTableFactory(const TerarkZipTableOptions&,
						 class TableFactory* fallback);

}  // namespace rocksdb

#endif /* TERARK_ZIP_TABLE_H_ */
