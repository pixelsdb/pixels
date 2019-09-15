package io.pixelsdb.pixels.common.lock;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.Lease;
import com.coreos.jetcd.Watch;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.kv.PutResponse;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.PutOption;
import com.coreos.jetcd.watch.WatchEvent;
import com.coreos.jetcd.watch.WatchResponse;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ZKPaths;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: tao
 * @date: Create in 2018-10-27 18:31
 **/
public class LockInternals
{

    private final String path;
    private final Client client;
    private final String basePath;
    private final String lockName;
    private Long leaseId = 0L;
    private static AtomicInteger count = new AtomicInteger(0);
    private volatile Map<String, Long> pathToVersion = new HashMap<>();

    public LockInternals(Client client, String path, String lockName)
    {
        this.client = client;
        this.basePath = PathUtils.validatePath(path);
        this.lockName = lockName;
        this.path = ZKPaths.makePath(path, lockName);
        Lease leaseClient = client.getLeaseClient();
        try
        {
            this.leaseId = leaseClient.grant(60).get(10, TimeUnit.SECONDS).getID();
        }
        catch (InterruptedException | ExecutionException | TimeoutException e1)
        {
            System.out.println("[error]: create lease failed:" + e1);
            return;
        }
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(new KeepAliveTask(leaseClient, leaseId), 1, 12, TimeUnit.SECONDS);
    }

    String attemptLock(long time, TimeUnit unit) throws Exception
    {
        // startMillis, millisToWait maybe useful later, refer 'InterProcessReadWriteLock' in 'org.apache.curator'
        long startMillis = System.currentTimeMillis();
        Long millisToWait = unit != null ? unit.toMillis(time) : null;
        String ourPath = null;
        boolean hasTheLock = false;
        boolean isDone = false;

        while (!isDone)
        {
            isDone = true;
            ourPath = this.createsTheLock(this.client, this.path);
            hasTheLock = this.internalLockLoop(ourPath);
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
        ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(path);
        String name = UUID.randomUUID().toString() + pathAndNode.getNode();

        String ourPath = ZKPaths.makePath(pathAndNode.getPath(), name) + count.getAndIncrement();

        try
        {
            PutResponse putResponse = client.getKVClient()
                    .put(ByteSequence.fromString(ourPath),
                            ByteSequence.fromString(""),
                            PutOption.newBuilder().withLeaseId(this.leaseId).build())
                    .get(10, TimeUnit.SECONDS);

            long revisionOfMyself = putResponse.getHeader().getRevision();
            pathToVersion.put(ourPath, revisionOfMyself);
            System.out.println("[createsTheLock]: " + ourPath + ": " + revisionOfMyself);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e1)
        {
            System.out.println("[error]: lock operation failed:" + e1);
        }
        return ourPath;
    }

    private synchronized boolean internalLockLoop(String ourPath) throws Exception
    {
        boolean haveTheLock = false;
        boolean doDelete = false;
        try
        {
            while (!haveTheLock)
            {
                List<KeyValue> children = this.getSortedChildren();

                long revisionOfMyself = this.pathToVersion.get(ourPath);
                if (revisionOfMyself == children.get(0).getCreateRevision())
                {
                    System.out.println("[lock]: lock successfully. [revision]:" + revisionOfMyself + ", " + ourPath);
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
                            String beforeKey = kv.getKey().toStringUtf8();
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
                        System.out.println("[Share lock]: " + ourPath + ", " + revisionOfMyself);
                        break;
                    }
                    else
                    {
                        // listen last 'WRIT'
                        ByteSequence preKeyBS = children.get(preIndex).getKey();
                        Watch.Watcher watcher = client.getWatchClient().watch(preKeyBS);
                        WatchResponse res = null;

                        try
                        {
                            System.out.println("[lock-read]: waiting: " + ourPath + ", " + revisionOfMyself + ", watch the lock: " + preKeyBS.toStringUtf8());
                            res = watcher.listen();
                        }
                        catch (InterruptedException e)
                        {
                            System.out.println("[error]: failed to listen key.");
                        }

                        List<WatchEvent> eventlist = res.getEvents();
                        for (WatchEvent event : eventlist)
                        {
                            if (event.getEventType().toString().equals("DELETE"))
                            {
                                System.out.println("[lock-read]: lock successfully. [revision]:" + revisionOfMyself + "," + ourPath);
                                if (watcher != null)
                                {
                                    System.out.println(watcher.hashCode() + " close" + "," + ourPath);
                                    // close() to avoid leaving unneeded watchers which is a type of resource leak
                                    watcher.close();
                                }
                                haveTheLock = true;
                                break;
                            }
                        }
                    }
                }
                else
                {
                    // current is 'WRIT'
                    System.out.println("[lock-write]: keep waiting." + ourPath + ", " + revisionOfMyself);
                    // wait all the key before to be deleted
                    while (true)
                    {
                        if (canGetWriteLock(ourPath))
                        {
                            System.out.println("[lock-write]: lock successfully. [revision]:" + revisionOfMyself + "," + ourPath);
                            haveTheLock = true;
                            break;
                        }
                        else
                        {
                            // write too often
                            try
                            {
                                Thread.sleep(1000);
                            }
                            catch (InterruptedException e)
                            {
                                e.printStackTrace();
                            }
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
        System.out.println(path);
        System.out.println("Current locksize: " + children.size() + ", getWriteLock: " + result + ", revision: " + revisionOfMyself);

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
        List<KeyValue> kvList = client.getKVClient().get(ByteSequence.fromString(basePath),
                GetOption.newBuilder().withPrefix(ByteSequence.fromString(basePath))
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
            client.getKVClient().delete(ByteSequence.fromString(ourPath)).get(10,
                    TimeUnit.SECONDS);
            System.out.println("[unLock]: unlock successfully.[lockName]:" + ourPath);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            System.out.println("[error]: unlock failed：" + e);
        }
    }

    public void releaseLock(String lockPath) throws Exception
    {
        deleteOurPath(lockPath);
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
