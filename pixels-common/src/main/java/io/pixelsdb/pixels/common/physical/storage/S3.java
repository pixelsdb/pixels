/*
 * Copyright 2021 PixelsDB.
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
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.time.Duration;

import static io.pixelsdb.pixels.common.lock.EtcdAutoIncrement.GenerateId;
import static io.pixelsdb.pixels.common.lock.EtcdAutoIncrement.InitId;
import static io.pixelsdb.pixels.common.utils.Constants.S3_ID_KEY;
import static io.pixelsdb.pixels.common.utils.Constants.S3_META_PREFIX;

/**
 * For S3, we assume that each table is stored in a separate folder
 * (i.e., a prefix or empty object in a bucket). And all the pixels
 * files in this table are stored as individual objects in the folder.
 * <br/>
 *
 * @author hank
 * Created at: 20/08/2021
 */
public final class S3 extends AbstractS3
{
    /*
     * Most of the methods in this class are moved into AbstractS3.
     */

    private static final Logger logger = LogManager.getLogger(S3.class);
    private static final String SchemePrefix = Scheme.s3.name() + "://";

    private final static boolean enableRequestDiversion;

    private final S3AsyncClient s3Async;
    private final S3AsyncClient s3Async1M;
    private final S3AsyncClient s3Async10M;

    static
    {
        if (EnableCache)
        {
            /**
             * Issue #222:
             * The etcd file id is only used for cache coordination.
             * Thus, we do not initialize the id key when cache is disabled.
             */

            InitId(S3_ID_KEY);
        }

        enableRequestDiversion = Boolean.parseBoolean(
                ConfigFactory.Instance().getProperty("s3.enable.request.diversion"));
        logger.info("Request diversion enabled: " + enableRequestDiversion);
    }

    public static boolean isRequestDiversionEnabled()
    {
        return enableRequestDiversion;
    }

    public S3()
    {
        String[] concurrencyAssign = null;
        if (enableRequestDiversion)
        {
            String assign = ConfigFactory.Instance().getProperty("s3.request.concurrency.assign");
            logger.info("Request concurrency assignment: " + assign);
            concurrencyAssign = assign.split(":");
        }

        /*
        s3Async = S3AsyncClient.builder()
                .httpClientBuilder(NettyNioAsyncHttpClient.builder()
                        .connectionTimeout(Duration.ofSeconds(connectionTimeoutSec))
                        .putChannelOption(ChannelOption.SO_RCVBUF, 1024*1024*1024)
                        .connectionAcquisitionTimeout(Duration.ofSeconds(connectionAcquisitionTimeoutSec))
                        .eventLoopGroup(SdkEventLoopGroup.builder().numberOfThreads(clientServiceThreads).build())
                        .maxConcurrency(maxRequestConcurrency).maxPendingConnectionAcquires(maxPendingRequests)).build();
        */

        int maxConcurrency, maxConcurrency1M, maxConcurrency10M;
        if (enableRequestDiversion)
        {
            maxConcurrency = (int) (MaxRequestConcurrency /100.0*Integer.parseInt(concurrencyAssign[0]));
            maxConcurrency1M = (int) (MaxRequestConcurrency /100.0*Integer.parseInt(concurrencyAssign[1]));
            maxConcurrency10M = (int) (MaxRequestConcurrency /100.0*Integer.parseInt(concurrencyAssign[2]));
        }
        else
        {
            maxConcurrency = MaxRequestConcurrency;
            maxConcurrency1M = 0;
            maxConcurrency10M = 0;
        }

        s3Async = S3AsyncClient.builder()
                .httpClientBuilder(AwsCrtAsyncHttpClient.builder()
                        .maxConcurrency(maxConcurrency))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallTimeout(Duration.ofSeconds(ConnTimeoutSec)).retryPolicy(RetryMode.ADAPTIVE)
                        .apiCallAttemptTimeout(Duration.ofSeconds(ConnAcquisitionTimeoutSec))
                        .build()).build();

        if (enableRequestDiversion)
        {
            s3Async1M = S3AsyncClient.builder()
                    .httpClientBuilder(AwsCrtAsyncHttpClient.builder()
                            .maxConcurrency(maxConcurrency1M))
                    .overrideConfiguration(ClientOverrideConfiguration.builder()
                            .apiCallTimeout(Duration.ofSeconds(ConnTimeoutSec))
                            .apiCallAttemptTimeout(Duration.ofSeconds(ConnAcquisitionTimeoutSec))
                            .build()).build();

            s3Async10M = S3AsyncClient.builder()
                    .httpClientBuilder(AwsCrtAsyncHttpClient.builder()
                            .maxConcurrency(maxConcurrency10M))
                    .overrideConfiguration(ClientOverrideConfiguration.builder()
                            .apiCallTimeout(Duration.ofSeconds(ConnTimeoutSec))
                            .apiCallAttemptTimeout(Duration.ofSeconds(ConnAcquisitionTimeoutSec))
                            .build()).build();
        }
        else
        {
            s3Async1M = s3Async10M = null;
        }

        s3 = S3Client.builder().httpClientBuilder(ApacheHttpClient.builder()
                .connectionTimeout(Duration.ofSeconds(ConnTimeoutSec))
                .socketTimeout(Duration.ofSeconds(ConnTimeoutSec))
                .connectionAcquisitionTimeout(Duration.ofSeconds(ConnAcquisitionTimeoutSec))
                .maxConnections(MaxRequestConcurrency)).build();
    }

    @Override
    protected String getPathKey(String path)
    {
        return S3_META_PREFIX + path;
    }

    private String getPathFrom(String key)
    {
        if (key.startsWith(S3_META_PREFIX))
        {
            return key.substring(S3_META_PREFIX.length());
        }
        return null;
    }

    @Override
    public Scheme getScheme()
    {
        return Scheme.s3;
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
        if (s3Async != null)
        {
            s3Async.close();
        }
        if (s3Async1M != null)
        {
            s3Async1M.close();
        }
        if (s3Async10M != null)
        {
            s3Async10M.close();
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
            long id = GenerateId(S3_ID_KEY);
            EtcdUtil.Instance().putKeyValue(getPathKey(path.toString()), Long.toString(id));
            return true;
        }
        return false;
    }

    public S3AsyncClient getAsyncClient()
    {
        return s3Async;
    }

    public S3AsyncClient getAsyncClient1M()
    {
        return s3Async1M;
    }

    public S3AsyncClient getAsyncClient10M()
    {
        return s3Async10M;
    }
}
