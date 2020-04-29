// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file demonstrates how to use the utility functions defined in
// rocksdb/utilities/options_util.h to open a rocksdb database without
// remembering all the rocksdb options.
#include <cstdio>
#include <string>
#include <vector>

#include "rocksdb/cache.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/juxtapose_db.h"

using namespace rocksdb;

int main()
{
  DBOptions db_opt, db_ref_opt;
  ColumnFamilyOptions cf_opt, cf_ref_opt;
  ColumnFamilyHandle* cfh;
  std::string db_path = "./db";
  std::string db_ref_path = "./db_ref";
  Status s;

  db_opt.create_if_missing = true;
  db_ref_opt.create_if_missing = true;
  cf_opt.enable_lazy_compaction = false;
  cf_ref_opt.enable_lazy_compaction = true;

  DB *db;
  s = DB::Open(Options(db_opt, cf_opt), db_path, &db);

  DB *db_ref;
  s = DB::Open(Options(db_ref_opt, cf_ref_opt), db_ref_path, &db_ref);

  JuxtaposeDB juxta(db, db_ref);


  juxta.CreateColumnFamily(cf_opt, cf_ref_opt, "pikachu", &cfh);

  ReadOptions ropt;
  WriteOptions wopt;
  FlushOptions fopt;
  juxta.Put(wopt, Slice("pika"), Slice("bobo"));
  juxta.Put(wopt, cfh, Slice("pika"), Slice("pika"));
  juxta.Put(wopt, cfh, Slice("pikapika"), Slice("pikapika"));
  juxta.Put(wopt, cfh, Slice("pikapikachu"), Slice("pikapikachu"));
  std::string ret;
  juxta.Get(ropt, "pika", &ret);
  fprintf(stdout, "pika in default : %s\n", ret.c_str());
  juxta.Get(ropt, cfh, "pika", &ret);
  fprintf(stdout, "pika in pikachu : %s\n", ret.c_str());
  auto iter = juxta.NewIterator(ropt, cfh);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    fprintf(stdout, "%s : %s\n", iter->key().ToString().c_str(),
                                 iter->value().ToString().c_str());
  }
  juxta.Delete(wopt, cfh, "pikapika");
  ret.clear();
  s = juxta.Get(ropt, cfh, "pikapika", &ret);
  assert(s.IsNotFound());
  WriteBatch b;
  b.Put(Slice("pikapikapikachu"), Slice("chupikapikapika"));
  b.Put(Slice("pikapikachupika"), Slice("pikachupikapika"));
  s = juxta.Write(wopt, &b);
  assert(s.ok());
  ret.clear();
  juxta.Get(ropt, Slice("pikapikapikachu"), &ret);
  fprintf(stdout, "pikapikapikachu in default : %s\n", ret.c_str());
  delete iter;
  juxta.Flush(fopt);
  juxta.DestroyColumnFamilyHandle(cfh);
  juxta.Close();

  return 0;
}
