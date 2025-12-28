package io.pixelsdb.pixels.storage.s3qs;

public class S3QueueMessage
{
    private String objectPath;
    private int workerNum = 0;
    private int partitionNum = 0;
    private boolean endWork = false;//specific to workers, indicating whether his work is finished
    private String receiptHandle = "";//specific to consumers, indicating which mesg is consumed.
    private long timestamp = System.currentTimeMillis();
    private String metadata = "";

    // 无参构造函数
    public S3QueueMessage() {
    }

    public S3QueueMessage(String objectPath) {
        this.objectPath = objectPath;
        this.timestamp = System.currentTimeMillis();
    }


    public String getObjectPath() {
        return objectPath;
    }

    public S3QueueMessage setObjectPath(String objectPath) {
        this.objectPath = objectPath;
        return this;
    }

    public int getWorkerNum() {
        return this.workerNum;
    }

    public S3QueueMessage setWorkerNum(int WorkerNum) {
        this.workerNum = WorkerNum;
        return this;
    }

    public int getPartitionNum() {
        return this.partitionNum;
    }

    public S3QueueMessage setPartitionNum(int PartitionNum) {
        this.partitionNum = PartitionNum;
        return this;
    }

    public boolean getEndWork() {
        return this.endWork;
    }

    public S3QueueMessage setEndwork(boolean endwork) {
        this.endWork = endwork;
        return this;
    }

    public String getReceiptHandle() {
        return this.receiptHandle;
    }

    public S3QueueMessage setReceiptHandle(String ReceiptHandle) {
        this.receiptHandle = ReceiptHandle;
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
