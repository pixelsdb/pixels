/*
 * Copyright 2025 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.storage.s3qs;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Properties;

/**
 * The structured message body sent through a S3QS partition queue.
 *
 * A producer task sends {@link MessageType#DATA} after an S3 object is durably
 * written, and sends {@link MessageType#PRODUCER_END} to every partition when
 * the producer task has no more data for this shuffle edge.
 */
public class S3QueueMessage
{
    public enum MessageType
    {
        DATA,
        PRODUCER_END
    }

    private static final int VERSION = 1;
    private static final String EMPTY = "";

    private int version = VERSION;
    private MessageType messageType = MessageType.DATA;
    private String shuffleId = EMPTY;
    private int partitionId = 0;
    private int producerId = 0;
    private int producerAttemptId = 0;
    private long sequenceId = 0L;
    private long timestamp = System.currentTimeMillis();
    private String objectPath = EMPTY;
    private long rowCount = -1L;
    private long byteSize = -1L;
    private String endReason = EMPTY;
    private String receiptHandle = EMPTY;
    private String metadata = EMPTY;

    public S3QueueMessage() { }

    /**
     * Construct a DATA message using the given object path prefix. The final
     * object path is filled by {@link S3Queue#offer(S3QueueMessage)}.
     */
    public S3QueueMessage(String objectPath)
    {
        this.objectPath = objectPath;
        this.timestamp = System.currentTimeMillis();
    }

    public static S3QueueMessage data(String shuffleId, int partitionId, int producerId,
                                      int producerAttemptId, long sequenceId, String objectPath)
    {
        return new S3QueueMessage()
                .setMessageType(MessageType.DATA)
                .setShuffleId(shuffleId)
                .setPartitionId(partitionId)
                .setProducerId(producerId)
                .setProducerAttemptId(producerAttemptId)
                .setSequenceId(sequenceId)
                .setObjectPath(objectPath);
    }

    public static S3QueueMessage producerEnd(String shuffleId, int partitionId, int producerId,
                                             int producerAttemptId, long sequenceId)
    {
        return new S3QueueMessage()
                .setMessageType(MessageType.PRODUCER_END)
                .setShuffleId(shuffleId)
                .setPartitionId(partitionId)
                .setProducerId(producerId)
                .setProducerAttemptId(producerAttemptId)
                .setSequenceId(sequenceId)
                .setEndReason("NORMAL");
    }

    /**
     * Serialize this message into a SQS message body.
     */
    public String toMessageBody() throws IOException
    {
        Properties properties = new Properties();
        properties.setProperty("version", String.valueOf(version));
        properties.setProperty("messageType", messageType.name());
        properties.setProperty("shuffleId", nullToEmpty(shuffleId));
        properties.setProperty("partitionId", String.valueOf(partitionId));
        properties.setProperty("producerId", String.valueOf(producerId));
        properties.setProperty("producerAttemptId", String.valueOf(producerAttemptId));
        properties.setProperty("sequenceId", String.valueOf(sequenceId));
        properties.setProperty("timestamp", String.valueOf(timestamp));
        properties.setProperty("objectPath", nullToEmpty(objectPath));
        properties.setProperty("rowCount", String.valueOf(rowCount));
        properties.setProperty("byteSize", String.valueOf(byteSize));
        properties.setProperty("endReason", nullToEmpty(endReason));
        properties.setProperty("metadata", nullToEmpty(metadata));

        StringWriter writer = new StringWriter();
        properties.store(writer, "s3qs-shuffle-message");
        return writer.toString();
    }

    /**
     * Parse a structured S3QS shuffle message from a SQS message body.
     */
    public static S3QueueMessage fromMessageBody(String body) throws IOException
    {
        Properties properties = new Properties();
        properties.load(new StringReader(body));
        S3QueueMessage message = new S3QueueMessage();
        message.setVersion(Integer.parseInt(properties.getProperty("version", String.valueOf(VERSION))));
        message.setMessageType(MessageType.valueOf(properties.getProperty("messageType", MessageType.DATA.name())));
        message.setShuffleId(properties.getProperty("shuffleId", EMPTY));
        message.setPartitionId(Integer.parseInt(properties.getProperty("partitionId", "0")));
        message.setProducerId(Integer.parseInt(properties.getProperty("producerId", "0")));
        message.setProducerAttemptId(Integer.parseInt(properties.getProperty("producerAttemptId", "0")));
        message.setSequenceId(Long.parseLong(properties.getProperty("sequenceId", "0")));
        message.setTimestamp(Long.parseLong(properties.getProperty("timestamp", "0")));
        message.setObjectPath(properties.getProperty("objectPath", EMPTY));
        message.setRowCount(Long.parseLong(properties.getProperty("rowCount", "-1")));
        message.setByteSize(Long.parseLong(properties.getProperty("byteSize", "-1")));
        message.setEndReason(properties.getProperty("endReason", EMPTY));
        message.setMetadata(properties.getProperty("metadata", EMPTY));
        return message;
    }

    private static String nullToEmpty(String value)
    {
        return value == null ? EMPTY : value;
    }

    public boolean isData()
    {
        return messageType == MessageType.DATA;
    }

    public boolean isProducerEnd()
    {
        return messageType == MessageType.PRODUCER_END;
    }

    public int getVersion()
    {
        return version;
    }

    public S3QueueMessage setVersion(int version)
    {
        this.version = version;
        return this;
    }

    public MessageType getMessageType()
    {
        return messageType;
    }

    public S3QueueMessage setMessageType(MessageType messageType)
    {
        this.messageType = messageType == null ? MessageType.DATA : messageType;
        return this;
    }

    public String getShuffleId()
    {
        return shuffleId;
    }

    public S3QueueMessage setShuffleId(String shuffleId)
    {
        this.shuffleId = nullToEmpty(shuffleId);
        return this;
    }

    public String getObjectPath()
    {
        return objectPath;
    }

    public S3QueueMessage setObjectPath(String objectPath)
    {
        this.objectPath = nullToEmpty(objectPath);
        return this;
    }

    public int getPartitionId()
    {
        return partitionId;
    }

    public S3QueueMessage setPartitionId(int partitionId)
    {
        this.partitionId = partitionId;
        return this;
    }

    public int getPartitionNum()
    {
        return partitionId;
    }

    public S3QueueMessage setPartitionNum(int partitionNum)
    {
        return setPartitionId(partitionNum);
    }

    public int getProducerId()
    {
        return producerId;
    }

    public S3QueueMessage setProducerId(int producerId)
    {
        this.producerId = producerId;
        return this;
    }

    public int getWorkerNum()
    {
        return producerId;
    }

    public S3QueueMessage setWorkerNum(int workerNum)
    {
        return setProducerId(workerNum);
    }

    public int getProducerAttemptId()
    {
        return producerAttemptId;
    }

    public S3QueueMessage setProducerAttemptId(int producerAttemptId)
    {
        this.producerAttemptId = producerAttemptId;
        return this;
    }

    public long getSequenceId()
    {
        return sequenceId;
    }

    public S3QueueMessage setSequenceId(long sequenceId)
    {
        this.sequenceId = sequenceId;
        return this;
    }

    public boolean getEndWork()
    {
        return isProducerEnd();
    }

    public S3QueueMessage setEndwork(boolean endwork)
    {
        this.messageType = endwork ? MessageType.PRODUCER_END : MessageType.DATA;
        return this;
    }

    public String getReceiptHandle()
    {
        return receiptHandle;
    }

    public S3QueueMessage setReceiptHandle(String receiptHandle)
    {
        this.receiptHandle = nullToEmpty(receiptHandle);
        return this;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public S3QueueMessage setTimestamp(long timestamp)
    {
        this.timestamp = timestamp;
        return this;
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public S3QueueMessage setRowCount(long rowCount)
    {
        this.rowCount = rowCount;
        return this;
    }

    public long getByteSize()
    {
        return byteSize;
    }

    public S3QueueMessage setByteSize(long byteSize)
    {
        this.byteSize = byteSize;
        return this;
    }

    public String getEndReason()
    {
        return endReason;
    }

    public S3QueueMessage setEndReason(String endReason)
    {
        this.endReason = nullToEmpty(endReason);
        return this;
    }

    public String getMetadata()
    {
        return metadata;
    }

    public S3QueueMessage setMetadata(String metadata)
    {
        this.metadata = nullToEmpty(metadata);
        return this;
    }
}
