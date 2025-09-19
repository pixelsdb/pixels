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
package io.pixelsdb.pixels.storage.sqs;

import static java.util.Objects.requireNonNull;

/**
 * An SQS stream path is in the format:
 * sqsstream://s3_bucket_name:sqs_queue_name
 *
 * @author hank
 * @create 2025-09-19
 */
public class SqsStreamPath
{
    private final String bucketName;
    private final String queueName;
    private final boolean valid;

    public SqsStreamPath(String path)
    {
        requireNonNull(path);
        if (path.contains("://"))
        {
            path = path.substring(path.indexOf("://") + 3);
        }
        int colon = path.indexOf(':');
        if (colon > 0)
        {
            String s3Prefix = path.substring(0, colon);
            if (!s3Prefix.endsWith("/"))
            {
                s3Prefix += "/";
            }
            this.bucketName = s3Prefix;
            this.queueName = path.substring(colon + 1);
            this.valid = true;
        }
        else
        {
            this.bucketName = null;
            this.queueName = null;
            this.valid = false;
        }
    }

    public String getBucketName()
    {
        return bucketName;
    }

    public String getQueueName()
    {
        return queueName;
    }

    public boolean isValid()
    {
        return valid;
    }
}
