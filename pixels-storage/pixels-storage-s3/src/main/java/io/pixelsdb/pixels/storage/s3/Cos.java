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
package io.pixelsdb.pixels.storage.s3;

import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * For Tencent Cloud COS, we assume that each table is stored in a separate folder
 * (i.e., a prefix or empty object in a bucket). And all the pixels
 * files in this table are stored as individual objects in the folder.
 * <br/>
 * To reuse the S3-compatible storage implementation, we use AWS S3 SDK to access COS.
 * <br/>
 * 
 * @author Dongyang Geng
 * @create 2026-07-07
 */
public final class Cos extends AbstractS3
{
    private static final String SchemePrefix = Scheme.cos.name() + "://";

    private static String cosRegion;
    private static String cosEndpoint;
    private static String cosAccessKey;
    private static String cosSecretKey;

    static
    {
        cosRegion = ConfigFactory.Instance().getProperty("cos.region");
        cosEndpoint = ConfigFactory.Instance().getProperty("cos.endpoint");
        cosAccessKey = ConfigFactory.Instance().getProperty("cos.access.key");
        cosSecretKey = ConfigFactory.Instance().getProperty("cos.secret.key");
    }

    /**
     * Set the configurations for COS. If any configuration is different from the default (null) or
     * previous value, the COS storage instance in StorageFactory is reloaded for the configuration
     * changes to take effect. In this case, the previous COS storage instance acquired from the
     * StorageFactory can be used without any impact.
     * <br/>
     * If the configurations are not changed, this method is a no-op.
     * <br/>
     * @param region COS region
     * @param endpoint COS endpoint
     * @param accessKey COS access key
     * @param secretKey COS secret key
     * @throws IOException if the configuration is invalid
     */
    public static void ConfigCos(String region, String endpoint, String accessKey, String secretKey)
            throws IOException
    {
        requireNonNull(region, "region is null");
        requireNonNull(endpoint, "endpoint is null");
        requireNonNull(accessKey, "accessKey is null");
        requireNonNull(secretKey, "secretKey is null");

        if (!Objects.equals(cosRegion, region) ||
                !Objects.equals(cosEndpoint, endpoint) ||
                !Objects.equals(cosAccessKey, accessKey) ||
                !Objects.equals(cosSecretKey, secretKey))
        {
            cosRegion = region;
            cosEndpoint = endpoint;
            cosAccessKey = accessKey;
            cosSecretKey = secretKey;
            StorageFactory.Instance().reload(Scheme.cos);
        }
    }

    public Cos()
    {
        connect();
    }

    private void connect()
    {
        requireNonNull(cosRegion, "COS region is not set");
        requireNonNull(cosEndpoint, "COS endpoint is not set");
        requireNonNull(cosAccessKey, "COS access key is not set");
        requireNonNull(cosSecretKey, "COS secret key is not set");
        
        // 
        this.s3 = S3Client.builder().httpClientBuilder(ApacheHttpClient.builder()
                        .connectionTimeout(Duration.ofSeconds(ConnTimeoutSec))
                        .socketTimeout(Duration.ofSeconds(ConnTimeoutSec))
                        .connectionAcquisitionTimeout(Duration.ofSeconds(ConnAcquisitionTimeoutSec))
                        .maxConnections(MaxRequestConcurrency))
                .endpointOverride(URI.create(cosEndpoint))
                .region(Region.of(cosRegion))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(cosAccessKey, cosSecretKey))).build();
        
    }

    @Override
    public void reconnect()
    {
        connect();
    }

    // Reuse S3.Path.

    @Override
    public Scheme getScheme()
    {
        return Scheme.cos;
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
}