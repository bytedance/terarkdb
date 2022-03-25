// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#define WITH_TERARKDB
#define TERARKDB_NAMESPACE terarkdb

/* #undef WITH_BYTEDANCE_METRICS */
/* #undef WITH_TERARK_ZIP */
/* #undef WITH_BOOSTLIB */
/* #undef WITH_DIAGNOSE_CACHE */
/* #undef WITH_ZENFS */

#ifdef WITH_ZENFS
#define ROCKSDB_NAMESPACE terarkdb
#define LIBZBD
#endif
