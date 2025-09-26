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
 * s3qs://s3_path::sqs_queue_url
 *
 *<p/>
 * The s3_path is formed of s3_bucket_name/s3_key.
 * The sqs_queue_url does not need to contain the https:// protocol prefix.
 *
 * @author hank
 * @create 2025-09-19
 */
public class S3QSPath
{
    private static final String SchemePrefix = Storage.Scheme.s3qs.name() + "://";
    private final String objectPath;
    private final String queueUrl;
    private final boolean valid;

    public S3QSPath(String path)
    {
        requireNonNull(path);
        if (path.startsWith(SchemePrefix))
        {
            path = path.substring(SchemePrefix.length());
        }
        int colon = path.indexOf("::");
        if (colon > 0)
        {
            this.objectPath = path.substring(0, colon);
            String queueUrl = path.substring(colon + 2);
            if (!queueUrl.startsWith("https://"))
            {
                queueUrl = "https://" + queueUrl;
            }
            this.queueUrl = queueUrl;
            this.valid = true;
        }
        else
        {
            this.objectPath = null;
            this.queueUrl = null;
            this.valid = false;
        }
    }

    public String getObjectPath()
    {
        return objectPath;
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
