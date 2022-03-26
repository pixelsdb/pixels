/*
 * Copyright 2018 PixelsDB.
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
package io.pixelsdb.pixels.common.lock;

import io.etcd.jetcd.*;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.pixelsdb.pixels.common.utils.StringUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: tao
 * @author hank
 * @date: Create in 2018-10-27 18:31
 **/
public class LockInternals
{
    private static Logger logger = LogManager.getLogger(LockInternals.class);

    private final String path;
    private final Client client;
    private final String basePath;
    private final String lockName;
    private boolean verbose = false;
    private Long leaseId = 0L;
    private static AtomicInteger count = new AtomicInteger(0);
    private volatile Map<String, Long> pathToVersion = new HashMap<>();
    private ScheduledExecutorService keepAliveService;

    public LockInternals(Client client, String path, String lockName)
    {
        this.client = client;
        this.basePath = StringUtil.validatePath(path);
        this.lockName = lockName;
        this.path = StringUtil.makePath(path, lockName);
        Lease leaseClient = client.getLeaseClient();
        try
        {
            this.leaseId = leaseClient.grant(60).get(10, TimeUnit.SECONDS).getID();
        }
        catch (InterruptedException | ExecutionException | TimeoutException e1)
        {
            logger.error("[create-lease-error]: " + e1);
            return;
        }
        keepAliveService = Executors.newSingleThreadScheduledExecutor();
        keepAliveService.scheduleAtFixedRate(new KeepAliveTask(leaseClient, leaseId), 1, 12, TimeUnit.SECONDS);
        keepAliveService.shutdown();
    }

    LockInternals verbose(boolean verbose)
    {
        this.verbose = verbose;
        return this;
    }

    private void DEBUG(String msg)
    {
        if (verbose)
        {
            logger.debug(msg);
        }
    }

    String attemptLock(long timeout, TimeUnit unit) throws Exception
    {
        // startMillis, millisToWait maybe useful later, refer 'InterProcessReadWriteLock' in 'org.apache.curator'
        String ourPath = null;
        boolean hasTheLock = false;
        boolean isDone = false;

        while (!isDone)
        {
            isDone = true;
            ourPath = this.createsTheLock(this.client, this.path);
            hasTheLock = this.internalLockLoop(ourPath, timeout, unit);
        }
        return hasTheLock ? ourPath : null;
    }

    /**
     * create key
     *
     * @param client the client
     * @param path   basePath + 'READ' or 'WRIT'
     * @return the key put in etcd, like '/read-write-lock/cf273ce3-23e7-45da-a480-dd5318692f26_READ_0'
     * @throws Exception
     */
    public synchronized String createsTheLock(Client client, String path) throws Exception
    {
        path = StringUtil.validatePath(path);
        String name = UUID.randomUUID() + path;

        String ourPath = StringUtil.makePath(path, name) + count.getAndIncrement();

        try
        {
            PutResponse putResponse = client.getKVClient()
                    .put(ByteSequence.from(ourPath, StandardCharsets.UTF_8),
                            ByteSequence.from("", StandardCharsets.UTF_8),
                            PutOption.newBuilder().withLeaseId(this.leaseId).build())
                    .get(10, TimeUnit.SECONDS);

            long revisionOfMyself = putResponse.getHeader().getRevision();
            pathToVersion.put(ourPath, revisionOfMyself);
            DEBUG("[create-lock-key-success]: " + ourPath + ": " + revisionOfMyself);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e1)
        {
            logger.error("[create-lock-key-error]: " + e1);
        }
        return ourPath;
    }

    private synchronized boolean internalLockLoop(String ourPath, long timeout, TimeUnit unit) throws Exception
    {
        boolean haveTheLock = false;
        boolean doDelete = false;
        try
        {
            while (true)
            {
                List<KeyValue> children = this.getSortedChildren();

                long revisionOfMyself = this.pathToVersion.get(ourPath);
                if (revisionOfMyself == children.get(0).getCreateRevision())
                {
                    DEBUG("[lock-success]: " + ourPath + "(" + revisionOfMyself + ")");
                    haveTheLock = true;
                    break;
                }
                // current is 'READ'
                if (ourPath.contains("_READ_"))
                {
                    int preIndex = 0;
                    // true: all 'READ', false: exist 'WRIT'
                    boolean isRead = true;
                    for (int index = children.size() - 1; index >= 0; index--)
                    {
                        KeyValue kv = children.get(index);
                        long revision = kv.getCreateRevision();
                        // no or exist 'WRIT'
                        if (revision >= revisionOfMyself)
                        {
                            continue;
                        }
                        else
                        {
                            String beforeKey = kv.getKey().toString(StandardCharsets.UTF_8);
                            if (beforeKey.contains("_WRIT_"))
                            {
                                preIndex = index;
                                isRead = false;
                                break;
                            }
                        }
                    }
                    if (isRead)
                    {
                        haveTheLock = true;
                        DEBUG("[read-lock-success]: " + ourPath + "(" + revisionOfMyself + ") [read-only]");
                        break;
                    }
                    else
                    {
                        // listen last 'WRIT'
                        ByteSequence preKeyBS = children.get(preIndex).getKey();
                        CountDownLatch latch = new CountDownLatch(1);
                        Watch.Watcher watcher = client.getWatchClient().watch(preKeyBS, WatchOption.DEFAULT, watchResponse ->
                        {
                            for (WatchEvent event : watchResponse.getEvents())
                            {
                                if (event.getEventType() == WatchEvent.EventType.DELETE)
                                {
                                    // listen to the DELETE even on the last WRIT.
                                    latch.countDown();
                                    break;
                                }
                            }
                        });

                        try
                        {
                            DEBUG("[read-lock-wait]: " + ourPath + "(" + revisionOfMyself +
                                    "), [wait for]: " + preKeyBS.toString(StandardCharsets.UTF_8));
                            if (latch.await(timeout, unit))
                            {
                                DEBUG("[read-lock-success]: " + ourPath + "(" + revisionOfMyself +
                                        ") [read-write]");
                                haveTheLock = true;
                            }
                            else
                            {
                                DEBUG("[read-lock-timeout]: " + ourPath + "(" + revisionOfMyself +
                                        ") [read-write]");
                                haveTheLock = false;
                            }
                            if (watcher != null)
                            {
                                // close() to avoid leaving unneeded watchers which is a type of resource leak
                                watcher.close();
                            }
                            break;
                        }
                        catch (InterruptedException e)
                        {
                            logger.error("[read-lock-error]: failed to listen key.");
                        }
                    }
                }
                else
                {
                    // current is 'WRIT'
                    DEBUG("[write-lock-wait]: " + ourPath + "(" + revisionOfMyself +")");
                    long startMillis = System.currentTimeMillis();
                    Long millisToWait = unit != null ? unit.toMillis(timeout) : null;
                    // wait all the keys before this key to be deleted
                    if (canGetWriteLock(ourPath))
                    {
                        DEBUG("[write-lock-success]: " + ourPath + "(" +
                                revisionOfMyself + ")");
                        haveTheLock = true;
                        break;
                    }
                    else
                    {
                        // don't try write lock too often
                        try
                        {
                            Thread.sleep(1000);
                            if (System.currentTimeMillis() - startMillis >= millisToWait)
                            {
                                DEBUG("[write-lock-timeout]: " + ourPath + "(" +
                                        revisionOfMyself + ")");
                                haveTheLock = false;
                                break;
                            }
                        }
                        catch (InterruptedException e)
                        {
                            logger.error("Interrupted when waiting to acquire write lock.", e);
                        }
                    }
                }
            }
        }
        catch (Exception var21)
        {
            doDelete = true;
            throw var21;
        }
        finally
        {
            if (doDelete)
            {
                this.deleteOurPath(ourPath);
            }
        }
        return haveTheLock;
    }

    /**
     * can get the write lock
     *
     * @return true if the first key, false if not
     */
    private boolean canGetWriteLock(String path)
    {
        List<KeyValue> children = null;
        try
        {
            children = this.getSortedChildren();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        //only id the first key is myself can get the write-lock
        long revisionOfMyself = this.pathToVersion.get(path);
        boolean result = revisionOfMyself == children.get(0).getModRevision();

        return result;
    }

    /**
     * get all the key with this prefix, order by MOD or VERSION
     *
     * @return List<KeyValue>
     * @throws Exception
     */
    List<KeyValue> getSortedChildren() throws Exception
    {
        List<KeyValue> kvList = client.getKVClient().get(ByteSequence.from(basePath, StandardCharsets.UTF_8),
                GetOption.newBuilder().withPrefix(ByteSequence.from(basePath, StandardCharsets.UTF_8))
                        .withSortField(GetOption.SortTarget.MOD).build())
                .get().getKvs();
        return kvList;
    }

    /**
     * delete the given key
     *
     * @param ourPath
     * @throws Exception
     */
    private void deleteOurPath(String ourPath) throws Exception
    {
        try
        {
            client.getKVClient().delete(ByteSequence.from(ourPath, StandardCharsets.UTF_8)).get(10,
                    TimeUnit.SECONDS);
            DEBUG("[unLock-success]: " + ourPath);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            DEBUG("[unlock-error]: " + e);
        }
    }

    public void releaseLock(String lockPath) throws Exception
    {
        deleteOurPath(lockPath);
        this.keepAliveService.shutdownNow();
    }

    public static class KeepAliveTask implements Runnable
    {
        private Lease leaseClient;
        private long leaseId;

        KeepAliveTask(Lease leaseClient, long leaseId)
        {
            this.leaseClient = leaseClient;
            this.leaseId = leaseId;
        }

        @Override
        public void run()
        {
            leaseClient.keepAliveOnce(leaseId);
        }
    }

}
