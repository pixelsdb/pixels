package io.pixelsdb.pixels.index.rocksdb;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksDBCloudTest {
    static {
        // 加载 RocksDB 的 JNI 库
        RocksDB.loadLibrary();
    }

    public static void main(String[] args) {
        // 1. 设置 RocksDB 选项
        // 如果数据库不存在则创建

        try (Options options = new Options()
                .setCreateIfMissing(true); RocksDB db = RocksDB.open(options, "/tmp/rocksdb-test")) {
            // 2. 打开数据库（数据会存储在 /tmp/rocksdb-test 目录）

            // 3. 写入一条数据
            byte[] key = "test-key".getBytes();
            byte[] value = "test-value".getBytes();
            db.put(key, value);

            // 4. 读取数据
            byte[] readValue = db.get(key);
            if (readValue != null) {
                System.out.println("Read value: " + new String(readValue));
            } else {
                System.out.println("Value not found!");
            }

            // 5. 删除数据（可选）
            db.delete(key);
            System.out.println("Data deleted.");

        } catch (RocksDBException e) {
            System.err.println("RocksDB error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 6. 关闭数据库
            // 7. 清理选项
            System.out.println("Test completed.");
        }
    }
}