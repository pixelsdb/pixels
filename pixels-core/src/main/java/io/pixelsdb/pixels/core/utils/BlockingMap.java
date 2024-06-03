package io.pixelsdb.pixels.core.utils;

import io.pixelsdb.pixels.core.PixelsReaderStreamImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class BlockingMap<K, V> {
    private static final Logger logger = LogManager.getLogger(BlockingMap.class);
    private final Map<K, ArrayBlockingQueue<V>> map = new ConcurrentHashMap<>();

    private BlockingQueue<V> getQueue(K key) {
        assert(key != null);
        return map.computeIfAbsent(key, k -> new ArrayBlockingQueue<>(1));
    }

    public void put(K key, V value) {
        // This will throw an exception if the key is already present in the map - we've set the capacity of the queue to 1.
        // Can also use queue.offer(value) if do not want an exception thrown.
        if ( !getQueue(key).add(value) ) {
            logger.error("Ignoring duplicate key");
        }
    }

    public V get(K key) throws InterruptedException {
        V ret = getQueue(key).poll(60, TimeUnit.SECONDS);
        if (ret == null) {
            throw new RuntimeException("BlockingMap.get() timed out");
        }
        return ret;
    }

    public boolean exist(K key) {
        return getQueue(key).peek() != null;
    }

    public V get(K key, long timeout, TimeUnit unit) throws InterruptedException {
        return getQueue(key).poll(timeout, unit);
    }
}
