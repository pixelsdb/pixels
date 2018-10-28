package cn.edu.ruc.iir.pixels.cache;

import cn.edu.ruc.iir.pixels.common.utils.Constants;
import cn.edu.ruc.iir.pixels.common.utils.EtcdUtil;
import com.coreos.jetcd.Lease;
import com.coreos.jetcd.Watch;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.options.WatchOption;
import com.coreos.jetcd.watch.WatchEvent;
import com.coreos.jetcd.watch.WatchResponse;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * pixels cache manager.
 *
 * @author guodong
 */
public class PixelsCacheManager
{
    public enum CacheManagerStatus {
        INITIALIZING(0), READABLE(1), UPDATING(2), LOST(-1);

        private int statusId;
        CacheManagerStatus(int statusId)
        {
            this.statusId = statusId;
        }
    }

    private final PixelsCacheReader cacheReader;
    private final PixelsCacheWriter cacheWriter;
    private final EtcdUtil etcdUtil;
    private final ScheduledExecutorService scheduledExecutor;
    private final ExecutorService watcherListenerExecutor;

    private static CacheManagerStatus cacheManagerStatus = CacheManagerStatus.INITIALIZING;

    public PixelsCacheManager(PixelsCacheReader cacheReader, PixelsCacheWriter cacheWriter)
    {
        this.cacheReader = cacheReader;
        this.cacheWriter = cacheWriter;
        this.etcdUtil = EtcdUtil.Instance();
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        this.watcherListenerExecutor = Executors.newSingleThreadExecutor();
        initialize();
    }

    /**
     * Initialize CacheManager
     *
     * 1. check if cache file exists.
     *    if exists, check if existing cache version is the same as current cache version in etcd.
     *      if not, existing cache is out of date, goto step #2.
     * 2. else, update caches with latest layout in etcd/mysql.
     * 3. update the status of CacheManager in etcd
     * 4. start a scheduled thread to update node(CacheManager) status
     * 5. add a watcher to listen to changes of the cache version in etcd.
     *    if there is a new version, we need update caches according to new layouts.
     * */
    private void initialize()
    {
        int existingVersion = cacheReader.getVersion();
        int currentVersion = Integer.parseInt(etcdUtil.getKeyValue(Constants.CACHE_VERSION_LITERAL)
                                                      .getValue().toStringUtf8());
        if (existingVersion >= 0) {
            if (etcdUtil.getKeyValue(Constants.CACHE_VERSION_LITERAL) != null) {
                if (currentVersion > existingVersion) {
                    cacheManagerStatus = CacheManagerStatus.UPDATING;
                    cacheWriter.updateAll(currentVersion);
                }
            }
        }
        else {
            cacheWriter.updateAll(currentVersion);
        }
        // update local caching status
        cacheManagerStatus = CacheManagerStatus.READABLE;
        Lease leaseClient = etcdUtil.getClient().getLeaseClient();
        // get a lease from etcd with a specified ttl
        // add this caching node into etcd with a granted lease
        try {
            long leaseId = leaseClient.grant(60).get(30, TimeUnit.SECONDS).getID();
            etcdUtil.putKeyValueWithLeaseId("node_", "" + cacheManagerStatus.statusId, leaseId); // todo node_id
        }
        catch (Exception e) {
            e.printStackTrace();
            // todo deal with exception
        }
        // start a scheduled thread to update node status periodically
        scheduledExecutor.scheduleAtFixedRate(new CacheManagerStatusRegister("", 30), 1, 10, TimeUnit.SECONDS);
        // start the watcher listener
        watcherListenerExecutor.submit(new CacheWatcherListener(this));
    }

    private void notifyEvent(WatchEvent event)
    {
        if (event.getEventType() == WatchEvent.EventType.PUT) {
            // update
            int version = Integer.parseInt(event.getKeyValue().getValue().toStringUtf8());
            cacheWriter.updateAll(version);
        }
        else if (event.getEventType() == WatchEvent.EventType.DELETE) {
            // deal with error, cache coordinator may be corrupted.
        }
        else {
        }
    }

    /**
     * Listener to watch changes of the cache version.
     * */
    private static class CacheWatcherListener
        implements Runnable
    {
        private final EtcdUtil etcdUtil;
        private final Watch watch;
        private final Watch.Watcher watcher;
        private final PixelsCacheManager cacheManager;

        public CacheWatcherListener(PixelsCacheManager cacheManager)
        {
            this.cacheManager = cacheManager;
            this.etcdUtil = EtcdUtil.Instance();
            this.watch = etcdUtil.getClient().getWatchClient();
            this.watcher = watch.watch(ByteSequence.fromString(Constants.CACHE_VERSION_LITERAL), WatchOption.DEFAULT);
        }

        @Override
        public void run()
        {
            while (true) {
                try {
                    WatchResponse watchResponse = watcher.listen();
                    for (WatchEvent event : watchResponse.getEvents()) {
                        cacheManager.notifyEvent(event);
                    }
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Scheduled register to update caching node status and keep its registration alive.
     * */
    private static class CacheManagerStatusRegister
            implements Runnable
    {
        private final EtcdUtil etcdUtil;
        private final Lease leaseClient;
        private final String id;
        private final long leaseId;

        public CacheManagerStatusRegister(String id, long leaseId)
        {
            this.etcdUtil = EtcdUtil.Instance();
            this.leaseClient = etcdUtil.getClient().getLeaseClient();
            this.id = id;
            this.leaseId = leaseId;

        }

        @Override
        public void run()
        {
            try {
                etcdUtil.putKeyValue("node_" + id, "" + cacheManagerStatus.statusId);
                leaseClient.keepAliveOnce(leaseId);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void stop()
        {
            leaseClient.revoke(leaseId);
            leaseClient.close();
        }
    }
}
