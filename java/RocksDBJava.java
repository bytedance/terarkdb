import org.rocksdb.RocksDB;
import org.rocksdb.*;
import org.rocksdb.Options;

public class RocksDBJava {
  public static void main(String[] args){
    RocksDB.loadLibrary();
    System.out.println("Hello RocksDB!");

		// the Options class contains a set of configurable DB options
		// that determines the behaviour of the database.
		try (final Options options = new Options().setCreateIfMissing(true)) {
			// a factory method that returns a RocksDB instance
			try (final RocksDB db = RocksDB.open(options, "/tmp/")) {
        byte[] key1 = "key1".getBytes();
        byte[] key2 = "key2".getBytes();

        final byte[] value = db.get(key1);
        if (value == null) {
          db.put(key2, "correct value".getBytes());
          final byte[] v = db.get(key2);
          System.out.println(new String(v));
        }
    
        // do something
			}
		} catch (RocksDBException e) {
      e.printStackTrace();
    }
  }
}
