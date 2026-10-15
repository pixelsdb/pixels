/*
 * Copyright 2026 PixelsDB.
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

/**
 * The result of polling a S3QS partition queue.
 *
 * It keeps the SQS receipt handle together with the structured shuffle message
 * so the caller can decide whether to open a data object or record a control
 * marker before acknowledging the message.
 */
public class S3QueuePollResult
{
    private final String receiptHandle;
    private final S3QueueMessage message;

    public S3QueuePollResult(String receiptHandle, S3QueueMessage message)
    {
        this.receiptHandle = receiptHandle;
        this.message = message;
    }

    public String getReceiptHandle()
    {
        return receiptHandle;
    }

    public S3QueueMessage getMessage()
    {
        return message;
    }
}
