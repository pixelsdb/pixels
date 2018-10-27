package cn.edu.ruc.iir.pixels.cache;

import cn.edu.ruc.iir.pixels.common.utils.Constants;
import cn.edu.ruc.iir.pixels.common.utils.EtcdUtil;
import com.coreos.jetcd.Watch;
import com.coreos.jetcd.watch.WatchResponse;

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
    private final ScheduledExecutorService scheduledExecutorService;

    private static CacheManagerStatus cacheManagerStatus = CacheManagerStatus.INITIALIZING;

    public PixelsCacheManager(PixelsCacheReader cacheReader, PixelsCacheWriter cacheWriter)
    {
        this.cacheReader = cacheReader;
        this.cacheWriter = cacheWriter;
        this.etcdUtil = EtcdUtil.Instance();
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
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
        if (existingVersion >= 0) {
            if (etcdUtil.getKeyValue(Constants.CACHE_VERSION_LITERAL) != null) {
                int currentVersion = Integer.parseInt(etcdUtil.getKeyValue(Constants.CACHE_VERSION_LITERAL)
                                                              .getValue().toStringUtf8());
                if (currentVersion > existingVersion) {
                    cacheWriter.updateAll();
                }
            }
        }
        else {
            cacheWriter.updateAll();
        }
        cacheManagerStatus = CacheManagerStatus.READABLE;
        etcdUtil.putKeyValue("node_", "" + cacheManagerStatus.statusId);
        scheduledExecutorService.scheduleAtFixedRate(new CacheManagerStatusRegister(30), 1, 10, TimeUnit.SECONDS);
        Watch.Watcher cacheVersionWatcher = etcdUtil.getCustomWatcherForKey(Constants.CACHE_VERSION_LITERAL);
        try {
            WatchResponse watchResponse = cacheVersionWatcher.listen();
            String latestCacheVersion = watchResponse.getEvents().get(0).getKeyValue().getValue().toStringUtf8();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static class CacheManagerStatusRegister
            implements Runnable
    {
        private final EtcdUtil etcdUtil;
        private final long expiration;

        public CacheManagerStatusRegister(long expiration)
        {
            this.etcdUtil = EtcdUtil.Instance();
            this.expiration = expiration;
        }

        @Override
        public void run()
        {
            etcdUtil.putKeyValueWithExpireTime("node_", "" + cacheManagerStatus.statusId, expiration);
        }
    }
}
