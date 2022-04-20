/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.common.physical.storage;

import io.pixelsdb.pixels.common.exception.StorageException;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Objects;

import static io.pixelsdb.pixels.common.lock.EtcdAutoIncrement.GenerateId;
import static io.pixelsdb.pixels.common.lock.EtcdAutoIncrement.InitId;
import static io.pixelsdb.pixels.common.utils.Constants.*;
import static java.util.Objects.requireNonNull;

/**
 * For MinIO, we assume that each table is stored in a separate folder
 * (i.e., a prefix or empty object in a bucket). And all the pixels
 * files in this table are stored as individual objects in the folder.
 * <br/>
 * To reduce the size of dependencies, we use AWS S3 SDK to access MinIO.
 * <br/>
 *
 * @author hank
 * Created at: 09/04/2022
 */
public final class MinIO extends AbstractS3
{
    // private static Logger logger = LogManager.getLogger(MinIO.class);
    private static final String SchemePrefix = Scheme.minio.name() + "://";

    private static String MinIOEndpoint = null;
    private static String MinIOAccessKey = null;
    private static String MinIOSecretKey = null;

    static
    {
        if (EnableCache)
        {
            /**
             * Issue #222:
             * The etcd file id is only used for cache coordination.
             * Thus, we do not initialize the id key when cache is disabled.
             */
            InitId(MINIO_ID_KEY);
        }
    }

    /**
     * Set the configurations for MinIO. If any configuration is different from the default (null) or
     * previous value, the MinIO storage instance in StorageFactory is reloaded for the configuration
     * changes to take effect. In this case, the previous MinIO storage instance acquired from the
     * StorageFactory can be used without any impact.
     * <br/>
     * If the configurations are not changed, this method is a no-op.
     *
     * @param endpoint the new endpoint of MinIO
     * @param accessKey the new access key of MinIO
     * @param secretKey the new secret key of MinIO
     * @throws IOException
     */
    public static void ConfigMinIO(String endpoint, String accessKey, String secretKey) throws IOException
    {
        requireNonNull(endpoint, "endpoint is null");
        requireNonNull(accessKey, "accessKey is null");
        requireNonNull(secretKey, "secretKey is null");

        if (!Objects.equals(MinIOEndpoint, endpoint) ||
                !Objects.equals(MinIOAccessKey, accessKey) ||
                !Objects.equals(MinIOSecretKey, secretKey))
        {
            MinIOEndpoint = endpoint;
            MinIOAccessKey = accessKey;
            MinIOSecretKey = secretKey;
            StorageFactory.Instance().reload(Scheme.minio);
        }
    }

    public MinIO()
    {
        requireNonNull(MinIOEndpoint, "MinIO endpoint is not set");
        requireNonNull(MinIOAccessKey, "MinIO access key is not set");
        requireNonNull(MinIOSecretKey, "MinIO secret key is not set");

        this.s3 = S3Client.builder().httpClientBuilder(ApacheHttpClient.builder()
                .connectionTimeout(Duration.ofSeconds(ConnTimeoutSec))
                .socketTimeout(Duration.ofSeconds(ConnTimeoutSec))
                .connectionAcquisitionTimeout(Duration.ofSeconds(ConnAcquisitionTimeoutSec))
                .maxConnections(MaxRequestConcurrency))
                .endpointOverride(URI.create(MinIOEndpoint))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(MinIOAccessKey, MinIOSecretKey))).build();
    }

    @Override
    protected String getPathKey(String path)
    {
        return MINIO_META_PREFIX + path;
    }

    // Reuse S3.Path.

    @Override
    public Scheme getScheme()
    {
        return Scheme.minio;
    }

    @Override
    public String ensureSchemePrefix(String path) throws IOException
    {
        if (path.startsWith(SchemePrefix))
        {
            return path;
        }
        if (path.contains("://"))
        {
            throw new IOException("Path '" + path +
                    "' already has a different scheme prefix than '" + SchemePrefix + "'.");
        }
        return SchemePrefix + path;
    }

    @Override
    public void close() throws IOException
    {
        if (s3 != null)
        {
            s3.close();
        }
    }

    @Override
    protected boolean existsOrGenIdSucc(Path path) throws IOException
    {
        if (!EnableCache)
        {
            throw new StorageException("Should not check or generate file id when cache is disabled");
        }
        if (!path.valid)
        {
            throw new IOException("Path '" + path.toString() + "' is not valid.");
        }
        if (EtcdUtil.Instance().getKeyValue(getPathKey(path.toString())) != null)
        {
            return true;
        }
        if (this.existsInS3(path))
        {
            // the file id does not exist, register a new id for this file.
            long id = GenerateId(MINIO_ID_KEY);
            EtcdUtil.Instance().putKeyValue(getPathKey(path.toString()), Long.toString(id));
            return true;
        }
        return false;
    }
}
