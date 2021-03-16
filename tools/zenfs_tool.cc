// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
// Copyright (c) 2021-present, Bytedance Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#if defined(GFLAGS) && !defined(ROCKSDB_LITE) && defined(LIBZBD)

#include <cstdio>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <iostream>
#include <fstream>
#include <streambuf>

#include "env/env_zenfs.h"
#include "util/gflags_compat.h"
using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;

DEFINE_string(zbd, "", "Path to a zoned block device.");
DEFINE_string(aux_path, "",
              "Path for auxiliary file storage (log and lock files).");
DEFINE_bool(force, false, "Force file system creation.");
DEFINE_string(path, "", "Path to directory to list files under");
DEFINE_int32(finish_threshold, 0, "Finish used zones if less than x% left");
DEFINE_int32(max_active_zones, 0, "Max active zone limit");
DEFINE_int32(max_open_zones, 0, "Max active zone limit");

namespace TERARKDB_NAMESPACE {

ZonedBlockDevice *zbd_open(bool readonly) {
  ZonedBlockDevice *zbd = new ZonedBlockDevice(FLAGS_zbd, nullptr);
  Status open_status = zbd->Open(readonly);

  if (!open_status.ok()) {
    fprintf(stderr, "Failed to open zoned block device: %s, error: %s\n",
            FLAGS_zbd.c_str(), open_status.ToString().c_str());
    delete zbd;
    return nullptr;
  }

  return zbd;
}

Status zenfs_mount(ZonedBlockDevice *zbd, ZenEnv **zenEnv, bool readonly) {
  Status s;

  *zenEnv = new ZenEnv(zbd, Env::Default(), nullptr);
  s = (*zenEnv)->Mount(readonly);
  if (!s.ok()) {
    delete *zenEnv;
    *zenEnv = nullptr;
  }

  return s;
}

int zenfs_tool_mkfs() {
  Status s;
  DIR* aux_dir;

  if (FLAGS_aux_path.empty()) {
    fprintf(stderr, "You need to specify --aux_path\n");
    return 1;
  }

  aux_dir = opendir(FLAGS_aux_path.c_str());
  if (ENOENT != errno) {
    fprintf(stderr, "Error: aux path exists\n");
    closedir(aux_dir);
    return 1;
  }

  ZonedBlockDevice *zbd = zbd_open(false);
  if (zbd == nullptr) return 1;

  ZenEnv *zenEnv;
  s = zenfs_mount(zbd, &zenEnv, false);
  if ((s.ok() || !s.IsNotFound()) && !FLAGS_force) {
    fprintf(
        stderr,
        "Existing filesystem found, use --force if you want to replace it.\n");
    return 1;
  }

  if (zenEnv != nullptr) delete zenEnv;

  zbd = zbd_open(false);
  zenEnv = new ZenEnv(zbd, Env::Default(), nullptr);

  if (FLAGS_aux_path.back() != '/') FLAGS_aux_path.append("/");

  s = zenEnv->MkFS(FLAGS_aux_path, FLAGS_finish_threshold,
                   FLAGS_max_open_zones, FLAGS_max_active_zones);
  if (!s.ok()) {
    fprintf(stderr, "Failed to create file system, error: %s\n",
            s.ToString().c_str());
    delete zenEnv;
    return 1;
  }

  fprintf(stdout, "ZenEnv file system created. Free space: %lu MB\n",
          zbd->GetFreeSpace() / (1024 * 1024));

  delete zenEnv;
  return 0;
}

void list_children(ZenEnv *zenEnv, std::string path) {
  std::vector<std::string> result;
  Status io_status = zenEnv->GetChildren(path, &result);

  if (!io_status.ok()) return;

  for (const auto f : result) {
    fprintf(stdout, "%s\n", f.c_str());
  }
}

int zenfs_tool_list() {
  Status s;
  ZonedBlockDevice *zbd = zbd_open(true);
  if (zbd == nullptr) return 1;

  ZenEnv *zenEnv;
  s = zenfs_mount(zbd, &zenEnv, true);
  if (!s.ok()) {
    fprintf(stderr, "Failed to mount filesystem, error: %s\n",
            s.ToString().c_str());
    return 1;
  }

  list_children(zenEnv, FLAGS_path);

  return 0;
}

int zenfs_tool_df() {
  Status s;
  ZonedBlockDevice *zbd = zbd_open(true);
  if (zbd == nullptr) return 1;

  ZenEnv *zenEnv;
  s = zenfs_mount(zbd, &zenEnv, true);
  if (!s.ok()) {
    fprintf(stderr, "Failed to mount filesystem, error: %s\n",
            s.ToString().c_str());
    return 1;
  }
  uint64_t used = zbd->GetUsedSpace();
  uint64_t free = zbd->GetFreeSpace();
  uint64_t reclaimable = zbd->GetReclaimableSpace();

  /* Avoid divide by zero */
  if (used == 0) used = 1;

  fprintf(stdout, "Free: %lu MB\nUsed: %lu MB\nReclaimable: %lu MB\nSpace amplification: %lu%%\n",
              free / (1024 * 1024), used / (1024 * 1024), reclaimable / (1024 * 1024),
              (100 * reclaimable) / used);

  return 0;
}

int zenfs_tool_lsuuid() {
  std::map<std::string, std::string>::iterator it;
  std::map<std::string, std::string> zenFileSystems = ListZenFileSystems();

  for (it = zenFileSystems.begin(); it != zenFileSystems.end(); it++)
    fprintf(stdout, "%s\t%s\n", it->first.c_str(), it->second.c_str());

  return 0;
}

static std::map<std::string, Env::WriteLifeTimeHint> wlth_map;


Env::WriteLifeTimeHint GetWriteLifeTimeHint(std::string filename) {
  if (wlth_map.find(filename) != wlth_map.end()) {
    return wlth_map[filename];
  }
  return Env::WriteLifeTimeHint::WLTH_NOT_SET;
}

int SaveWriteLifeTimeHints() {
  std::ofstream wlth_file(FLAGS_path + "/write_lifetime_hints.dat");

  if (!wlth_file.is_open()) {
    fprintf(stderr, "Failed to store time hints\n");
    return 1;
  }

  for (auto it = wlth_map.begin(); it != wlth_map.end(); it++) {
      wlth_file << it->first << "\t" << it->second << "\n";
  }

  wlth_file.close();
  return 0;
}

void ReadWriteLifeTimeHints() {
  std::ifstream wlth_file(FLAGS_path + "/write_lifetime_hints.dat");

  if (!wlth_file.is_open()) {
    fprintf(stderr, "WARNING: failed to read write life times\n");
    return;
  }

  std::string filename;
  uint32_t lth;

  while ( wlth_file >> filename >> lth ) {
    wlth_map.insert(std::make_pair(filename, (Env::WriteLifeTimeHint)lth));
    fprintf(stdout, "read: %s %u \n", filename.c_str(), lth);
  }

  wlth_file.close();
}

Status zenfs_tool_copy_file(Env *f_fs, std::string f, Env *t_fs, std::string t) {
  EnvOptions eopt;
  Status s;
  std::unique_ptr<SequentialFile> f_file;
  std::unique_ptr<WritableFile> t_file;
  size_t buffer_sz = 1024 * 1024;
  uint64_t to_copy;
  char *buffer;
 
  fprintf(stdout, "%s\n", f.c_str());
 
  s = f_fs->GetFileSize(f, &to_copy);
  if (!s.ok()) { return s; }

  s = f_fs->NewSequentialFile(f, &f_file, eopt);
  if (!s.ok()) { return s; }
  
  s = t_fs->NewWritableFile(t, &t_file, eopt);
  if (!s.ok()) { return s; }

  t_file->SetWriteLifeTimeHint(GetWriteLifeTimeHint(t));

  buffer = (char *)malloc(buffer_sz);
  if (buffer == nullptr) {
    return Status::IOError("Failed to allocate copy buffer");
  }

  while (to_copy > 0) {
    size_t chunk_sz = to_copy;
    Slice chunk_slice;
  
    if (chunk_sz > buffer_sz)
      chunk_sz = buffer_sz;
  
    s = f_file->Read(chunk_sz, &chunk_slice, buffer);
    if (!s.ok()) { break; }
    
    s = t_file->Append(chunk_slice);
    to_copy -= chunk_slice.size();
  }
  
  free(buffer);
  if (!s.ok()) { return s; }
  
  return t_file->Fsync();
}

Status zenfs_tool_copy_dir(Env *f_fs, std::string f_dir, Env *t_fs, std::string t_dir) {
  Status s;
  std::vector<std::string> files;

  s = f_fs->GetChildren(f_dir, &files);
  if (!s.ok()) { return s; }
  
  for (const auto f : files) {
    std::string filename = f_dir + f;
    bool is_dir;

    if (f == "." || f == ".." || f == "write_lifetime_hints.dat")
      continue;

    // s = f_fs->IsDirectory(filename, &is_dir);
    // if (!s.ok()) { return s; }
    
    std::string dest_filename;

    if (t_dir == "") {  
       dest_filename = f;
    } else {
       dest_filename = t_dir + "/" + f; 
    }
   
    if (is_dir) {
      s = t_fs->CreateDir(dest_filename);
      if (!s.ok()) { return s; }
      s =  zenfs_tool_copy_dir(f_fs, filename + "/", t_fs, dest_filename);
      if (!s.ok()) { return s; }
    } else {
      s = zenfs_tool_copy_file(f_fs, filename, t_fs, dest_filename);
      if (!s.ok()) { return s; }
    }
  }

  return s;
}

int zenfs_tool_backup() {
  Status status;
  Status io_status;
  ZonedBlockDevice *zbd;
  ZenEnv *zenEnv;

  zbd = zbd_open(false);
  if (zbd == nullptr) return 1;

  status = zenfs_mount(zbd, &zenEnv, false);
  if (!status.ok()) {
    fprintf(stderr, "Failed to mount filesystem, error: %s\n",
            status.ToString().c_str());
    return 1;
  }

  io_status = zenfs_tool_copy_dir(zenEnv, "", Env::Default(), FLAGS_path);
  if (!io_status.ok()) {
    fprintf(stderr, "Copy failed, error: %s\n", io_status.ToString().c_str());
    return 1;
  }

  wlth_map = zenEnv->GetWriteLifeTimeHints();
  return SaveWriteLifeTimeHints();
}

int zenfs_tool_restore() {
  Status status;
  ZonedBlockDevice *zbd;
  ZenEnv *zenEnv;

  ReadWriteLifeTimeHints();

  zbd = zbd_open(false);
  if (zbd == nullptr) return 1;

  status = zenfs_mount(zbd, &zenEnv, false);
  if (!status.ok()) {
    fprintf(stderr, "Failed to mount filesystem, error: %s\n",
            status.ToString().c_str());
    return 1;
  }
  
  status = zenfs_tool_copy_dir(Env::Default(), FLAGS_path, zenEnv, "");
  if (!status.ok()) {
    fprintf(stderr, "Copy failed, error: %s\n", status.ToString().c_str());
    return 1;
  }
  return 0;
}
}  // namespace TERARKDB_NAMESPACE

int zenfs_tool(int argc, char **argv) {
  SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                  +" <command> [OPTIONS]...\nCommands: mkfs, list, ls-uuid");
  if (argc < 2) {
    fprintf(stderr, "You need to specify a command.\n");
    return 1;
  }

  std::string subcmd(argv[1]);
  ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_zbd.empty() && subcmd != "ls-uuid") {
    fprintf(stderr, "You need to specify a zoned block device using --zbd\n");
    return 1;
  }
  if (subcmd == "mkfs") {
    return TERARKDB_NAMESPACE::zenfs_tool_mkfs();
  } else if (subcmd == "list") {
    return TERARKDB_NAMESPACE::zenfs_tool_list();
  } else if (subcmd == "ls-uuid") {
    return TERARKDB_NAMESPACE::zenfs_tool_lsuuid();
  } else if (subcmd == "df") {
    return TERARKDB_NAMESPACE::zenfs_tool_df();
  } else if (subcmd == "backup") {
    return TERARKDB_NAMESPACE::zenfs_tool_backup();
  } else if (subcmd == "restore") {
    return TERARKDB_NAMESPACE::zenfs_tool_restore();
  } else {
    fprintf(stderr, "Subcommand not recognized: %s\n", subcmd.c_str());
    return 1;
  }

  return 0;
}

#endif  // defined(GFLAGS) && !defined(ROCKSDB_LITE) && defined(LIBZBD)
