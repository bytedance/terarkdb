/*
 * terark_zip_table.h
 *
 *  Created on: 2016Äê8ÔÂ9ÈÕ
 *      Author: leipeng
 */

#pragma once

#ifndef TERARK_ZIP_TABLE_H_
#define TERARK_ZIP_TABLE_H_

#include <memory>
#include <string>

namespace rocksdb {

struct TerarkZipTableOptions {
	unsigned fixed_key_len = 0;
	int indexNestLevel = 3;
	double sampleRatio = 0.03;
	std::string localTempDir = "/tmp";
};

class TableFactory* NewTerarkZipTableFactory(const TerarkZipTableOptions&);

}  // namespace rocksdb

#endif /* TERARK_ZIP_TABLE_H_ */
