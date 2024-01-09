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
package io.pixelsdb.pixels.storage.s3;

import io.etcd.jetcd.KeyValue;
import io.pixelsdb.pixels.common.physical.ObjectPath;
import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.EtcdUtil;
import io.pixelsdb.pixels.storage.s3.io.S3InputStream;
import io.pixelsdb.pixels.storage.s3.io.S3OutputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * The abstract class for all the AWS S3 compatible storage systems.
 * <br/>
 * For S3, we assume that each table is stored in a separate folder
 * (i.e., a prefix or empty object in a bucket). And all the pixels
 * files in this table are stored as individual objects in the folder.
 * <br/>
 *
 * @author hank
 * @create 2022-09-04 23:55
 */
public abstract class AbstractS3 implements Storage
{
    /*
     * The implementations of most methods in this class are from its subclass S3.
     */

    protected static int ConnTimeoutSec = 3600;
    protected static int ConnAcquisitionTimeoutSec = 3600;
    protected static int ClientServiceThreads = 20;
    protected static int MaxRequestConcurrency = 200;
    protected static int MaxPendingRequests = 50_000;
    protected final static boolean EnableCache;
    protected static final int FilesPerDeleteRequest = 1000;

    protected S3Client s3 = null;

    static
    {
        EnableCache = Boolean.parseBoolean(
                ConfigFactory.Instance().getProperty("cache.enabled"));
        ConnTimeoutSec = Integer.parseInt(
                ConfigFactory.Instance().getProperty("s3.connection.timeout.sec"));
        ConnAcquisitionTimeoutSec = Integer.parseInt(
                ConfigFactory.Instance().getProperty("s3.connection.acquisition.timeout.sec"));
        ClientServiceThreads = Integer.parseInt(
                ConfigFactory.Instance().getProperty("s3.client.service.threads"));
        MaxRequestConcurrency = Integer.parseInt(
                ConfigFactory.Instance().getProperty("s3.max.request.concurrency"));
        MaxPendingRequests = Integer.parseInt(
                ConfigFactory.Instance().getProperty("s3.max.pending.requests"));
    }

    public AbstractS3() { }

    public abstract void reconnect();

    /**
     * Get the key for the file metadata (e.g., file id) in etcd.
     * @param path
     * @return
     */
    abstract protected String getPathKey(String path);

    @Override
    abstract public Scheme getScheme();

    @Override
    abstract public String ensureSchemePrefix(String path) throws IOException;

    /**
     * Get the statuses of the files or subdirectories in the given path if it is
     * a directory or multiple directories seperated by semicolon (;).
     * Note that S3 does not support real directories, a directory / folder is simply
     * a key prefix or an empty object with its name ends with '/'.
     * @param path the given path, may contain multiple directories that are seperated by semicolon.
     * @return the statuses of the files or subdirectories.
     * @throws IOException
     */
    @Override
    public List<Status> listStatus(String... path) throws IOException
    {
        List<Status> statuses = null;
        for (String eachPath : path)
        {
            ObjectPath p = new ObjectPath(eachPath);
            if (!p.valid)
            {
                throw new IOException("Path '" + eachPath + "' is not valid.");
            }
            ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder().bucket(p.bucket);
            if (p.key != null)
            {
                builder.prefix(p.key);
            }
            ListObjectsV2Request request = builder.build();
            ListObjectsV2Response response = s3.listObjectsV2(request);
            List<S3Object> objects = new ArrayList<>(response.keyCount());
            while (response.isTruncated())
            {
                objects.addAll(response.contents());
                request = builder.continuationToken(response.nextContinuationToken()).build();
                response = s3.listObjectsV2(request);
            }
            objects.addAll(response.contents());
            if (statuses == null)
            {
                statuses = new ArrayList<>(objects.size());
            }
            /*
             * Issue #618:
             * If p is a folder (i.e., its key ends with "/"), we should filter out the empty object with exact the
             * same key. If p is not a folder, we should not filter out the object with the same key. However, in this
             * case we should filter out the empty object with the key p.key+"/", because in this case the user intend
             * to list a path of a folder but forgot to append "/" to the path.
             */
            String maybeFolderPath = p.isFolder ? p.key : p.key + "/";
            for (S3Object object : objects)
            {
                if (object.key().equals(maybeFolderPath) && object.size() == 0)
                {
                    // exclude the directory (i.e., eachPath) itself
                    continue;
                }
                p.key = object.key();
                statuses.add(new Status(p.toStringWithPrefix(this),
                        object.size(), p.key.endsWith("/"), 1));
            }
        }
        return statuses;
    }

    @Override
    public List<String> listPaths(String... path) throws IOException
    {
        return this.listStatus(path).stream().map(Status::getPath)
                .collect(Collectors.toList());
    }

    /**
     * For S3, the replication is always 1.
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public Status getStatus(String path) throws IOException
    {
        ObjectPath p = new ObjectPath(path);
        if (!p.valid)
        {
            throw new IOException("Path '" + path + "' is not valid.");
        }
        if (p.isFolder)
        {
            return new Status(p.toStringWithPrefix(this), 0, true, 1);
        }

        HeadObjectRequest request = HeadObjectRequest.builder().bucket(p.bucket).key(p.key).build();
        try
        {
            HeadObjectResponse response = s3.headObject(request);
            return new Status(p.toStringWithPrefix(this),
                    response.contentLength(), false, 1);
        } catch (Exception e)
        {
            throw new IOException("Failed to get object head of '" + path + "'", e);
        }
    }

    @Override
    public long getFileId(String path) throws IOException
    {
        requireNonNull(path, "path is null");
        if (EnableCache)
        {
            ObjectPath p = new ObjectPath(path);
            if (!p.valid)
            {
                throw new IOException("Path '" + path + "' is not valid.");
            }
            // try to generate the id in etcd if it does not exist.
            if (!this.existsOrGenIdSucc(p))
            {
                throw new IOException("Path '" + path + "' does not exist.");
            }
            KeyValue kv = EtcdUtil.Instance().getKeyValue(getPathKey(p.toString()));
            return Long.parseLong(kv.getValue().toString(StandardCharsets.UTF_8));
        }
        else
        {
            // Issue #222: return an arbitrary id when cache is disable.
            return path.hashCode();
        }
    }

    @Override
    public boolean mkdirs(String path) throws IOException
    {
        ObjectPath p = new ObjectPath(path);
        if (!p.valid)
        {
            throw new IOException("Path '" + path + "' is not valid.");
        }
        if (!p.isFolder)
        {
            throw new IOException("Path '" + path + "' is a directory, " +
                    "the key for S3 directory (folder) must ends with '/'.");
        }
        if (this.existsInS3(p))
        {
            throw new IOException("Path '" + path + "' already exists.");
        }

        if (!this.existsInS3(new ObjectPath(p.bucket)))
        {
            CreateBucketRequest request = CreateBucketRequest.builder().bucket(p.bucket).build();
            s3.createBucket(request);
            S3Waiter waiter = s3.waiter();
            HeadBucketRequest requestWait = HeadBucketRequest.builder()
                    .bucket(p.bucket).build();
            waiter.waitUntilBucketExists(requestWait);
        }

        if (p.key != null)
        {
            PutObjectRequest request = PutObjectRequest.builder().bucket(p.bucket).key(p.key).build();
            s3.putObject(request, RequestBody.empty());

            S3Waiter waiter = s3.waiter();
            HeadObjectRequest requestWait = HeadObjectRequest.builder()
                    .bucket(p.bucket).key(p.key).build();
            waiter.waitUntilObjectExists(requestWait);
        }
        return true;
    }

    /**
     * For S3, this open method is only used to read the data object
     * fully and sequentially. And it will load the whole object into
     * memory, so be careful for large objects.
     * @param path
     * @return
     * @throws IOException
     */
    @Override
    public DataInputStream open(String path) throws IOException
    {
        ObjectPath p = new ObjectPath(path);
        if (!p.valid)
        {
            throw new IOException("Path '" + path + "' is not valid.");
        }
        if (!this.existsInS3(p))
        {
            throw new IOException("Path '" + path + "' does not exist.");
        }
        return new DataInputStream(new S3InputStream(s3, p.bucket, p.key));
    }

    /**
     * Open an output stream to write a file into S3.
     * The access latency of cloud storage, such as S3, is high. If we are sure that the file path
     * does not exist (e.g., the file name a UUID), we can skip file existence checking by setting
     * overwrite to true.
     * @param path
     * @param overwrite
     * @param bufferSize
     * @return
     * @throws IOException
     */
    @Override
    public DataOutputStream create(String path, boolean overwrite, int bufferSize) throws IOException
    {
        ObjectPath p = new ObjectPath(path);
        if (!p.valid)
        {
            throw new IOException("Path '" + path + "' is not valid.");
        }
        if (!overwrite && this.existsInS3(p))
        {
            throw new IOException("Path '" + path + "' already exists.");
        }
        return new DataOutputStream(new S3OutputStream(s3, p.bucket, p.key, bufferSize));
    }

    @Override
    public boolean delete(String path, boolean recursive) throws IOException
    {
        ObjectPath p = new ObjectPath(path);
        if (!p.valid)
        {
            throw new IOException("Path '" + path + "' is not valid.");
        }
        if (!this.existsInS3(p))
        {
            if (EnableCache)
            {
                EtcdUtil.Instance().deleteByPrefix(getPathKey(p.toString()));
            }
            // Issue #170: path-not-exist is not an exception for deletion.
            return false;
        }
        if (p.isFolder)
        {
            if (!recursive)
            {
                throw new IOException("Non-recursive deletion of directory is not supported in S3-like storage.");
            }
            // The ListObjects S3 API, which is used by listStatus, is already recursive.
            List<Status> statuses = this.listStatus(path);
            int numStatuses = statuses.size();
            for (int i = 0; i < numStatuses; )
            {
                // Currently, AWS SDK only supports deleting 1000 objects per request.
                List<ObjectIdentifier> objectsToDelete = new ArrayList<>(
                        Math.min(numStatuses, FilesPerDeleteRequest));
                for (int j = 0; j < FilesPerDeleteRequest && i < numStatuses; ++j, ++i)
                {
                    ObjectPath sub = new ObjectPath(statuses.get(i).getPath());
                    objectsToDelete.add(ObjectIdentifier.builder().key(sub.key).build());
                }
                try
                {
                    DeleteObjectsRequest request = DeleteObjectsRequest.builder().bucket(p.bucket)
                            .delete(Delete.builder().objects(objectsToDelete).build()).build();
                    s3.deleteObjects(request);
                } catch (Exception e)
                {
                    throw new IOException("Failed to delete objects under '" + path + "'.", e);
                }
            }
        }
        else
        {
            DeleteObjectRequest request = DeleteObjectRequest.builder().bucket(p.bucket).key(p.key).build();
            try
            {
                s3.deleteObject(request);
            } catch (Exception e)
            {
                throw new IOException("Failed to delete object '" + p + "' from S3.", e);
            }
        }
        if (EnableCache)
        {
            EtcdUtil.Instance().deleteByPrefix(getPathKey(p.toString()));
        }
        return true;
    }

    @Override
    public boolean supportDirectCopy()
    {
        return true;
    }

    @Override
    public boolean directCopy(String src, String dest) throws IOException
    {
        ObjectPath srcPath = new ObjectPath(src);
        ObjectPath destPath = new ObjectPath(dest);
        if (!srcPath.valid)
        {
            throw new IOException("Path '" + src + "' is invalid.");
        }
        if (!destPath.valid)
        {
            throw new IOException("Path '" + dest + "' is invalid.");
        }
        if (!this.existsInS3(srcPath))
        {
            throw new IOException("Path '" + src + "' does not exist.");
        }
        if (this.existsInS3(destPath))
        {
            throw new IOException("Path '" + dest + "' already exists.");
        }
        CopyObjectRequest copyReq = CopyObjectRequest.builder()
                .sourceBucket(srcPath.bucket).sourceKey(srcPath.key)
                .destinationBucket(destPath.bucket).destinationKey(destPath.key).build();
        try
        {
            s3.copyObject(copyReq);
            return true;
        }
        catch (RuntimeException e)
        {
            throw new IOException("Failed to copy object from '" + src + "' to '" + dest + "'", e);
        }
    }

    @Override
    abstract public void close() throws IOException;

    @Override
    public boolean exists(String path) throws IOException
    {
        return this.existsInS3(new ObjectPath(path));
    }

    /**
     * If a file or directory exists in S3.
     * @param path
     * @return
     * @throws IOException
     */
    protected boolean existsInS3(ObjectPath path) throws IOException
    {
        if (!path.valid)
        {
            throw new IOException("Path '" + path + "' is not valid.");
        }

        try
        {
            if (path.key == null)
            {
                HeadBucketRequest request = HeadBucketRequest.builder()
                        .bucket(path.bucket).build();
                s3.headBucket(request);
                return true;
            }
            else if (path.isFolder)
            {
                ListObjectsV2Request request = ListObjectsV2Request.builder()
                        .bucket(path.bucket).prefix(path.key).maxKeys(1).build();
                return s3.listObjectsV2(request).keyCount() > 0;
            }
            else
            {
                HeadObjectRequest request = HeadObjectRequest.builder()
                        .bucket(path.bucket).key(path.key).build();
                s3.headObject(request);
                return true;
            }
        } catch (Exception e)
        {
            if (e instanceof NoSuchKeyException ||
            e instanceof NoSuchBucketException)
            {
                return false;
            }
            throw new IOException("Failed to check the existence of '" + path + "'", e);
        }
    }

    abstract protected boolean existsOrGenIdSucc(ObjectPath path) throws IOException;

    @Override
    public boolean isFile(String path) throws IOException
    {
        return !(new ObjectPath(path).isFolder);
    }

    @Override
    public boolean isDirectory(String path) throws IOException
    {
        return new ObjectPath(path).isFolder;
    }

    public S3Client getClient()
    {
        return s3;
    }
}
