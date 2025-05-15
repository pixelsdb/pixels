package io.pixelsdb.pixels.index.rocksdb;

public class RocksetTest {
    // Native方法声明
    private native long CreateCloudFileSystem0(
        String bucketName,
        String s3Prefix,
        long[] baseEnvPtrOut);
    private native long OpenDBCloud0(
        long cloudEnvPtr,
        String localDbPath,
        String persistentCachePath,
        long persistentCacheSizeGB,
        boolean readOnly);
    private native void DBput0(long dbHandle, byte[] key, byte[] value);
    private native byte[] DBget0(long dbHandle, byte[] key);
    private native void DBdelete0(long dbHandle, byte[] key);
    private native void CloseDB0(long dbHandle, long baseEnvPtr, long cloudEnvPtr);  // 新增的关闭方法

    // 加载JNI库
    static {
        System.loadLibrary("RocksetJni");
    }

    // 包装native方法的public方法
    public long CreateDBCloud(
        String bucketName,
        String s3Prefix,
        String localDbPath,
        String persistentCachePath,
        long persistentCacheSizeGB,
        boolean readOnly,
        long[] baseEnvPtrOut,
        long cloudEnvPtr) {
        cloudEnvPtr = CreateCloudFileSystem0(
            bucketName, s3Prefix, baseEnvPtrOut);
        if (cloudEnvPtr == 0) {
            throw new RuntimeException("Failed to create CloudFileSystem");
        }

        long dbHandle = OpenDBCloud0(
            cloudEnvPtr, localDbPath, persistentCachePath, persistentCacheSizeGB, readOnly);
        if (dbHandle == 0) {
            CloseDB0(0, baseEnvPtrOut[0], cloudEnvPtr); // 清理 base_env
            throw new RuntimeException("Failed to open DBCloud");
        }

        return dbHandle;
    }

    public void DBput(long dbHandle, byte[] key, byte[] value) {
        DBput0(dbHandle, key, value);
    }

    public byte[] DBget(long dbHandle, byte[] key) {
        return DBget0(dbHandle, key);
    }

    public void DBdelete(long dbHandle, byte[] key) {
        DBdelete0(dbHandle, key);
    }

    // 新增的关闭方法
    public void CloseDB(long dbHandle, long baseEnvPtr, long cloudEnvPtr) {
        if (dbHandle != 0) {
            CloseDB0(dbHandle, baseEnvPtr, cloudEnvPtr);
        }
    }

    public static void main(String[] args) {
        RocksetTest test = new RocksetTest();
        long dbHandle = 0;
        long[] baseEnvPtrOut = new long[1];
        long cloudEnvPtr = 0;

        try {
            // 1. 创建数据库
            String bucketName = "pixels-turbo-public";
            String s3Prefix = "test/rocksdb-cloud/";
            String localDbPath = "/tmp/rocksdb_cloud_test";
            String persistentCachePath = "/tmp/cache";
            long persistentCacheSizeGB = 1L;
            boolean readOnly = false;

            System.out.println("Creating RocksDB-Cloud instance...");
            dbHandle = test.CreateDBCloud(bucketName, s3Prefix, localDbPath, 
            persistentCachePath, persistentCacheSizeGB, readOnly, baseEnvPtrOut, cloudEnvPtr);
            System.out.println("DB handle: " + dbHandle);

            // 2. 测试写入
            byte[] testKey = "test_key".getBytes();
            byte[] testValue = "test_value".getBytes();

            System.out.println("Putting key-value pair...");
            test.DBput(dbHandle, testKey, testValue);

            // 3. 测试读取
            System.out.println("Getting value...");
            byte[] retrievedValue = test.DBget(dbHandle, testKey);
            if (retrievedValue != null) {
                System.out.println("Retrieved value: " + new String(retrievedValue));
            } else {
                System.out.println("Key not found");
            }

            // 4. 测试删除
            System.out.println("Deleting key...");
            test.DBdelete(dbHandle, testKey);

            // 验证删除
            byte[] deletedValue = test.DBget(dbHandle, testKey);
            if (deletedValue == null) {
                System.out.println("Key successfully deleted");
            }
        } finally {
            // 5. 确保关闭数据库
            if (dbHandle != 0) {
                System.out.println("Closing DB...");
                test.CloseDB(dbHandle, baseEnvPtrOut[0], cloudEnvPtr);
            }
        }
    }
}