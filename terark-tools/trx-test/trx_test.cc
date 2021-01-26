#include "trx_test.hpp"

namespace rocksdb {
uint32_t key_prefix = 8;
void CreateUnCommittedData() {
  Options options;
  options.create_if_missing = true;
  options.max_write_buffer_number = 2;
  options.write_buffer_size = 4 * 1024;
  options.blob_size = -1;
  options.level0_file_num_compaction_trigger = 2;
  options.merge_operator = MergeOperators::CreateFromStringId("stringappend");
  auto env = Env::Default();
  options.env = env;
  options.two_write_queues = true;
  std::string dbname = "trxdb";

  DestroyDB(dbname, options);
  TransactionDBOptions txn_db_options;
  txn_db_options.write_policy = TxnDBWritePolicy::WRITE_PREPARED;
  Status s;
  TransactionDB *db = nullptr;
  s = TransactionDB::Open(options, txn_db_options, dbname, &db);

  TransactionOptions txn_options;
  // Header (12 bytes) + NOOP (1 byte) + 2 * 8 bytes for data.
  txn_options.max_write_batch_size = 100;
  std::random_device rd;
  std::mt19937_64 mt(rd());
  std::uniform_int_distribution<uint64_t> gen(1, 16);
  std::uniform_int_distribution<uint64_t> genval(199, 12382344);

  uint32_t key_prefix_storage = __builtin_bswap32(key_prefix);

  std::vector<Transaction *> trxs(100, nullptr);
  for (size_t i = 0; i < trxs.size(); i++) {
    trxs[i] = db->BeginTransaction(WriteOptions(), txn_options);
    assert(trxs[i] != nullptr);
    s = trxs[i]->SetName("myxid-" + std::to_string(i));
  }

  for (size_t i = 0; i < trxs.size(); i++) {
    char key_buf[12] = {0};
    memcpy(&key_buf[0], &key_prefix_storage, sizeof(int));
    uint64_t randkey = gen(mt);
    memcpy(&key_buf[0] + 4, &randkey, sizeof(uint64_t));
    const rocksdb::Slice key =
        rocksdb::Slice(reinterpret_cast<char *>(key_buf), sizeof(key_buf));

    char value_buf[16] = {0};
    uint64_t randval = genval(mt);
    memcpy(&value_buf[0], &randval, sizeof(uint64_t));
    randval = genval(mt);
    memcpy(&value_buf[0], &randval, sizeof(uint64_t));
    randval = genval(mt);
    const rocksdb::Slice value =
        rocksdb::Slice(reinterpret_cast<char *>(value_buf), sizeof(value_buf));

    trxs[i]->GetWriteBatch()->GetWriteBatch()->Put(key, value);
    trxs[i]->Prepare();

    if (i==0) {
      trxs[i]->Commit();
    }
  }

  FlushOptions fo;
  fo.wait = true;
  db->Flush(fo);

  abort();

  using namespace std::chrono_literals;
  std::cout << "sleep wait kill" << std::endl;
  std::this_thread::sleep_for(60s);

  assert(true);
}
void OpenAndSeek() {
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBIter::FindNextUserEntryInternal::Reseek", [&](void *arg) {
        std::string *lastkey = reinterpret_cast<std::string *>(arg);
        std::cout << "Reseek: " << Slice(*lastkey).ToString(true) << std::endl;
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  Options options;
  options.create_if_missing = false;
  options.max_write_buffer_number = 2;
  options.write_buffer_size = 4 * 1024;
  options.level0_file_num_compaction_trigger = 2;
  options.merge_operator = MergeOperators::CreateFromStringId("stringappend");
  auto env = Env::Default();
  options.env = env;
  options.two_write_queues = true;
  std::string dbname = "trxdb";

  TransactionDBOptions txn_db_options;
  txn_db_options.write_policy = TxnDBWritePolicy::WRITE_PREPARED;
  TransactionDB *db = nullptr;
  auto s = TransactionDB::Open(options, txn_db_options, dbname, &db);
  assert(s.ok());

  char seek_key_buf[4];
  uint32_t key_prefix_storage = __builtin_bswap32(key_prefix);
  memcpy(&seek_key_buf[0], &key_prefix_storage, sizeof(int));
  const rocksdb::Slice seek_key(reinterpret_cast<char *>(&seek_key_buf[0]),
                                sizeof(int));
  rocksdb::ReadOptions read_options;
  read_options.total_order_seek = true;
  auto it = db->NewIterator(read_options, db->DefaultColumnFamily());
  for (it->Seek(seek_key); it->Valid(); it->Next()) {
  }
}
}  // namespace rocksdb

int main(int argc, char **argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_open_and_seek) {
    rocksdb::OpenAndSeek();
  } else {
    rocksdb::CreateUnCommittedData();
  }

  return 0;
}
