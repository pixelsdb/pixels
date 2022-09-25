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

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;
import io.etcd.jetcd.KeyValue;
import io.pixelsdb.pixels.common.exception.StorageException;
import io.pixelsdb.pixels.common.physical.Status;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.physical.storage.AbstractS3.Path;
import io.pixelsdb.pixels.common.utils.EtcdUtil;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.pixelsdb.pixels.common.lock.EtcdAutoIncrement.GenerateId;
import static io.pixelsdb.pixels.common.lock.EtcdAutoIncrement.InitId;
import static io.pixelsdb.pixels.common.physical.storage.AbstractS3.EnableCache;
import static io.pixelsdb.pixels.common.utils.Constants.*;
import static java.util.Objects.requireNonNull;

/**
 * The Storage implementation of Google Cloud Storage.
 * @author hank
 * @date 9/25/22
 */
public class GCS implements Storage
{
    private static final String SchemePrefix = Scheme.gcs.name() + "://";

    protected static final int RequestsPerBatch = 100;

    private static String projectId = null;
    private static String location = null;


    static
    {
        if (EnableCache)
        {
            /**
             * Issue #222:
             * The etcd file id is only used for cache coordination.
             * Thus, we do not initialize the id key when cache is disabled.
             */
            InitId(GCS_ID_KEY);
        }
    }

    /**
     * Set the configurations for GCS. If any configuration is different from the default or
     * previous value, the GCS storage instance in StorageFactory is reloaded for the configuration
     * changes to take effect. In this case, the previous Redis storage instance acquired from the
     * StorageFactory can be used without any impact.
     * <br/>
     * If the configurations are not changed, this method is a no-op.
     *
     * @param projectId the project id of GCS
     * @param location the location, e.g.,
     * @throws IOException
     */
    public static void ConfigGCS(String projectId, String location) throws IOException
    {
        requireNonNull(projectId, "project is null");

        if (!Objects.equals(GCS.projectId, projectId) || !Objects.equals(GCS.location, location))
        {
            GCS.projectId = projectId;
            GCS.location = location;
            StorageFactory.Instance().reload(Scheme.gcs);
        }
    }

    private com.google.cloud.storage.Storage gcs;

    public GCS ()
    {
        gcs = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    }

    @Override
    public Scheme getScheme()
    {
        return Scheme.gcs;
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
    public List<Status> listStatus(String path) throws IOException
    {
        Path p = new Path(path);
        if (!p.valid)
        {
            throw new IOException("Path '" + path + "' is not valid.");
        }
        Page<Blob> blobs;
        if (p.key != null)
        {
            blobs = this.gcs.list(p.bucket,
                    com.google.cloud.storage.Storage.BlobListOption.prefix(p.key),
                    com.google.cloud.storage.Storage.BlobListOption.currentDirectory());
        }
        else
        {
            blobs = this.gcs.list(p.bucket,
                    com.google.cloud.storage.Storage.BlobListOption.currentDirectory());
        }

        List<Status> statuses = new ArrayList<>();
        for (Blob blob : blobs.iterateAll())
        {
            Status status = new Status(path, blob.getSize(), blob.isDirectory(), 1);
            statuses.add(status);
        }
        return statuses;
    }

    @Override
    public List<String> listPaths(String path) throws IOException
    {
        return this.listStatus(path).stream().map(Status::getPath)
                .collect(Collectors.toList());
    }

    @Override
    public Status getStatus(String path) throws IOException
    {
        Path p = new Path(path);
        if (!p.valid)
        {
            throw new IOException("Path '" + path + "' is not valid.");
        }
        if (p.isFolder)
        {
            return new Status(p.toString(), 0, true, 1);
        }

        Blob blob = this.gcs.get(p.bucket, p.key,
                com.google.cloud.storage.Storage.BlobGetOption.fields(
                        com.google.cloud.storage.Storage.BlobField.SIZE));
        try
        {
            return new Status(p.toString(), blob.getSize(), false, 1);
        } catch (Exception e)
        {
            throw new IOException("Failed to get object metadata of '" + path + "'", e);
        }
    }

    @Override
    public long getFileId(String path) throws IOException
    {
        requireNonNull(path, "path is null");
        if (EnableCache)
        {
            Path p = new Path(path);
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

    private String getPathKey(String path)
    {
        return GCS_META_PREFIX + path;
    }

    private boolean existsOrGenIdSucc(Path path) throws IOException
    {
        if (!EnableCache)
        {
            throw new StorageException("Should not check or generate file id when cache is disabled");
        }
        if (!path.valid)
        {
            throw new IOException("Path '" + path + "' is not valid.");
        }
        if (EtcdUtil.Instance().getKeyValue(getPathKey(path.toString())) != null)
        {
            return true;
        }
        if (this.existsInGCS(path))
        {
            // the file id does not exist, register a new id for this file.
            long id = GenerateId(GCS_ID_KEY);
            EtcdUtil.Instance().putKeyValue(getPathKey(path.toString()), Long.toString(id));
            return true;
        }
        return false;
    }

    /**
     * If a file or directory exists in GCS.
     * @param path
     * @return
     * @throws IOException
     */
    private boolean existsInGCS(Path path) throws IOException
    {
        if (!path.valid)
        {
            throw new IOException("Path '" + path + "' is not valid.");
        }

        try
        {
            if (path.key == null)
            {
                return this.gcs.get(path.bucket) != null;
            }
            else
            {
                return this.gcs.get(path.bucket, path.key) != null;
            }
        } catch (Exception e)
        {
            if (e instanceof com.google.cloud.storage.StorageException)
            {
                return false;
            }
            throw new IOException("Failed to check the existence of '" + path + "'", e);
        }
    }

    @Override
    public boolean mkdirs(String path) throws IOException
    {
        Path p = new Path(path);
        if (!p.valid)
        {
            throw new IOException("Path '" + path + "' is not valid.");
        }
        if (!p.isFolder)
        {
            throw new IOException("Path '" + path + "' is a directory, " +
                    "the key for S3 directory (folder) must ends with '/'.");
        }

        if (!this.existsInGCS(new Path(p.bucket)))
        {
            this.gcs.create(BucketInfo.newBuilder(p.bucket)
                    .setStorageClass(StorageClass.STANDARD)
                    .setLocation(location).build());
        }

        if (p.key != null)
        {
            BlobId blobId = BlobId.of(p.bucket, p.key);
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
            this.gcs.create(blobInfo, com.google.cloud.storage.Storage.BlobTargetOption.doesNotExist());
        }
        return true;
    }

    @Override
    public DataInputStream open(String path) throws IOException
    {
        Path p = new Path(path);
        if (!p.valid)
        {
            throw new IOException("Path '" + path + "' is not valid.");
        }
        if (!this.existsInGCS(p))
        {
            throw new IOException("Path '" + path + "' does not exist.");
        }
        ReadChannel readChannel = this.gcs.reader(BlobId.of(p.bucket, p.key));
        return new DataInputStream(new BufferedInputStream(
                Channels.newInputStream(readChannel), GCS_BUFFER_SIZE));
    }

    @Override
    public DataOutputStream create(String path, boolean overwrite, int bufferSize) throws IOException
    {
        Path p = new Path(path);
        if (!p.valid)
        {
            throw new IOException("Path '" + path + "' is not valid.");
        }
        BlobId blobId = BlobId.of(p.bucket, p.key);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        WriteChannel writeChannel;
        if (overwrite)
        {
            writeChannel = this.gcs.writer(blobInfo);
        }
        else
        {
            writeChannel = this.gcs.writer(blobInfo,
                    com.google.cloud.storage.Storage.BlobWriteOption.doesNotExist());
        }
        return new DataOutputStream(new BufferedOutputStream(
                Channels.newOutputStream(writeChannel), bufferSize));
    }

    @Override
    public boolean delete(String path, boolean recursive) throws IOException
    {
        Path p = new Path(path);
        if (!p.valid)
        {
            throw new IOException("Path '" + path + "' is not valid.");
        }
        if (!this.existsInGCS(p))
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
                throw new IOException("Non-recursive deletion of directory is not supported in GCS storage.");
            }
            // The ListObjects S3 API, which is used by listStatus, is already recursive.
            List<Status> statuses = this.listStatus(path);
            int numStatuses = statuses.size();
            for (int i = 0; i < numStatuses; )
            {
                // Currently, AWS SDK only supports deleting 1000 objects per request.
                StorageBatch deleteBatch = this.gcs.batch();
                for (int j = 0; j < RequestsPerBatch && i < numStatuses; ++j, ++i)
                {
                    Path sub = new Path(statuses.get(i).getPath());
                    deleteBatch.delete(sub.bucket, sub.key);
                }
                try
                {
                   deleteBatch.submit();
                } catch (Exception e)
                {
                    throw new IOException("Failed to delete objects under '" + path + "'.", e);
                }
            }
        }
        else
        {
            try
            {
                this.gcs.delete(p.bucket, p.key);
            } catch (Exception e)
            {
                throw new IOException("Failed to delete object '" + p + "' from GCS.", e);
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
        Path srcPath = new Path(src);
        Path destPath = new Path(dest);
        if (!srcPath.valid)
        {
            throw new IOException("Path '" + src + "' is invalid.");
        }
        if (!destPath.valid)
        {
            throw new IOException("Path '" + dest + "' is invalid.");
        }
        if (!this.existsInGCS(srcPath))
        {
            throw new IOException("Path '" + src + "' does not exist.");
        }
        com.google.cloud.storage.Storage.BlobTargetOption destPrecondition =
                com.google.cloud.storage.Storage.BlobTargetOption.doesNotExist();
        com.google.cloud.storage.Storage.CopyRequest copyRequest =
                com.google.cloud.storage.Storage.CopyRequest.newBuilder()
                        .setSource(srcPath.bucket, srcPath.key)
                        .setTarget(BlobId.of(destPath.bucket, destPath.key), destPrecondition).build();
        try
        {
            this.gcs.copy(copyRequest);
            return true;
        }
        catch (Exception e)
        {
            throw new IOException("Failed to copy object from '" + src + "' to '" + dest + "'", e);
        }
    }

    @Override
    public void close() throws IOException { }

    @Override
    public boolean exists(String path) throws IOException
    {
        return this.existsInGCS(new Path(path));
    }

    @Override
    public boolean isFile(String path) throws IOException
    {
        return !(new Path(path).isFolder);
    }

    @Override
    public boolean isDirectory(String path) throws IOException
    {
        return new Path(path).isFolder;
    }

    public com.google.cloud.storage.Storage getGCS()
    {
        return this.gcs;
    }
}
