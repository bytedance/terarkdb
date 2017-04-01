/*
 * terark_zip_weak_function.h
 *
 *  Created on: 2017-04-01
 *      Author: leipeng
 */

#ifndef TABLE_TERARK_ZIP_WEAK_FUNCTION_H_
#define TABLE_TERARK_ZIP_WEAK_FUNCTION_H_
#pragma once

#include "terark_zip_table.h"

namespace rocksdb {

#if !defined(_MSC_VER)

void
__attribute__((weak))
TerarkZipAutoConfigForBulkLoad(struct TerarkZipTableOptions&,
                               struct DBOptions&,
                               struct ColumnFamilyOptions&,
                               size_t cpuNum,
                               size_t memBytesLimit,
                               size_t diskBytesLimit);

void
__attribute__((weak))
TerarkZipAutoConfigForOnlineDB(struct TerarkZipTableOptions&,
                               struct DBOptions&,
                               struct ColumnFamilyOptions&,
                               size_t cpuNum,
                               size_t memBytesLimit,
                               size_t diskBytesLimit);

class TableFactory*
__attribute__((weak))
NewTerarkZipTableFactory(const TerarkZipTableOptions&,
                         class TableFactory* fallback);


bool
__attribute__((weak))
TerarkZipConfigFromEnv(struct DBOptions&,
                       struct ColumnFamilyOptions&);

bool __attribute__((weak)) TerarkZipCFOptionsFromEnv(ColumnFamilyOptions&);
void __attribute__((weak)) TerarkZipDBOptionsFromEnv(DBOptions&);

#endif // _MSC_VER

} // namespace rocksdb

#endif /* TABLE_TERARK_ZIP_WEAK_FUNCTION_H_ */
