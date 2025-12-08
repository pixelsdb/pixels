package io.pixelsdb.pixels.storage.s3qs;

public class S3QueueMessage
{
    private String objectPath;
    private int WorkerNum;
    private int PartitionNum;
    private String bucketName;
    private long timestamp;
    private String metadata; // 可选：其他元数据

    // 无参构造函数
    public S3QueueMessage() {
    }

    public S3QueueMessage(String objectPath) {
        this.objectPath = objectPath;
        this.timestamp = System.currentTimeMillis();
    }

    // getter 和 setter 方法（JSON 库需要）
    public String getObjectPath() {
        return objectPath;
    }

    public S3QueueMessage setObjectPath(String objectPath) {
        this.objectPath = objectPath;
        return this;
    }

    public int getWorkerNum() {
        return this.WorkerNum;
    }

    public S3QueueMessage setWorkerNum(int WorkerNum) {
        this.WorkerNum = WorkerNum;
        return this;
    }

    public int getPartitionNum() {
        return this.PartitionNum;
    }

    public S3QueueMessage setPartitionNum(int PartitionNum) {
        this.PartitionNum = PartitionNum;
        return this;
    }

    public String getBucketName() {
        return bucketName;
    }

    public S3QueueMessage setBucketName(String bucketName) {
        this.bucketName = bucketName;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public S3QueueMessage setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public String getMetadata() {
        return metadata;
    }

    public S3QueueMessage setMetadata(String metadata) {
        this.metadata = metadata;
        return this;
    }
}
