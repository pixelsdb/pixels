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
package io.pixelsdb.pixels.storage.sqs3;

import io.pixelsdb.pixels.common.physical.Storage;

import static java.util.Objects.requireNonNull;

/**
 * An SQS stream path is in the format:
 * s3qs://s3_base_path:sqs_queue_url
 *
 *<p/>
 * The s3_base_path is formed of s3_bucket_name/s3_key_prefix.
 * The sqs_queue_url does not need to contain the https:// protocol prefix.
 *
 * @author hank
 * @create 2025-09-19
 */
public class S3QSPath
{
    private static final String SchemePrefix = Storage.Scheme.s3qs.name() + "://";
    private final String bucketName;
    private final String keyPrefix;
    private final String queueUrl;
    private final boolean valid;

    public S3QSPath(String path)
    {
        requireNonNull(path);
        if (path.startsWith(SchemePrefix))
        {
            path = path.substring(SchemePrefix.length());
        }
        int colon = path.indexOf(':');
        if (colon > 0)
        {
            String s3BasePath = path.substring(0, colon);
            if (!s3BasePath.endsWith("/"))
            {
                s3BasePath += "/";
            }
            int slash = s3BasePath.lastIndexOf('/');
            this.bucketName = s3BasePath.substring(0, slash);
            this.keyPrefix = s3BasePath.substring(slash + 1);
            String queueUrl = path.substring(colon + 1);
            if (!queueUrl.startsWith("https://"))
            {
                queueUrl = "https://" + queueUrl;
            }
            this.queueUrl = queueUrl;
            this.valid = true;
        }
        else
        {
            this.bucketName = null;
            this.keyPrefix = null;
            this.queueUrl = null;
            this.valid = false;
        }
    }

    public String getBucketName()
    {
        return bucketName;
    }

    public String getKeyPrefix()
    {
        return keyPrefix;
    }

    public String getQueueUrl()
    {
        return queueUrl;
    }

    public boolean isValid()
    {
        return valid;
    }
}
