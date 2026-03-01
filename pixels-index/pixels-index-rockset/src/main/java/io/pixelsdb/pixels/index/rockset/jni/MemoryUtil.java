package io.pixelsdb.pixels.index.rockset.jni;

/**
 * This file is modified from RocksDB's own JNI bindings.
 * @author Rolland1944
 * @create 2026-02-22
 */

import java.util.*;

/**
 * JNI passthrough for MemoryUtil.
 */
public class MemoryUtil {

  /**
   * <p>Returns the approximate memory usage of different types in the input
   * list of DBs and Cache set.  For instance, in the output map the key
   * kMemTableTotal will be associated with the memory
   * usage of all the mem-tables from all the input rocksdb instances.</p>
   *
   * <p>Note that for memory usage inside Cache class, we will
   * only report the usage of the input "cache_set" without
   * including those Cache usage inside the input list "dbs"
   * of DBs.</p>
   *
   * @param dbs List of dbs to collect memory usage for.
   * @param caches Set of caches to collect memory usage for.
   * @return Map from {@link MemoryUsageType} to memory usage as a {@link Long}.
   */
  @SuppressWarnings("PMD.CloseResource")
  public static Map<MemoryUsageType, Long> getApproximateMemoryUsageByType(
      final List<RocksetDB> dbs, final Set<RocksetCache> caches) {
    final int dbCount = (dbs == null) ? 0 : dbs.size();
    final int cacheCount = (caches == null) ? 0 : caches.size();
    final long[] dbHandles = new long[dbCount];
    final long[] cacheHandles = new long[cacheCount];
    if (dbCount > 0) {
      final ListIterator<RocksetDB> dbIter = dbs.listIterator();
      while (dbIter.hasNext()) {
        dbHandles[dbIter.nextIndex()] = dbIter.next().handle();
      }
    }
    if (cacheCount > 0) {
      // NOTE: This index handling is super ugly but I couldn't get a clean way to track both the
      // index and the iterator simultaneously within a Set.
      int i = 0;
      for (final RocksetCache cache : caches) {
        cacheHandles[i] = cache.nativeHandle;
        i++;
      }
    }
    final Map<Byte, Long> byteOutput = getApproximateMemoryUsageByType(dbHandles, cacheHandles);
    final Map<MemoryUsageType, Long> output = new HashMap<>();
    for (final Map.Entry<Byte, Long> longEntry : byteOutput.entrySet()) {
      output.put(MemoryUsageType.getMemoryUsageType(longEntry.getKey()), longEntry.getValue());
    }
    return output;
  }

  private static native Map<Byte, Long> getApproximateMemoryUsageByType(
      final long[] dbHandles, final long[] cacheHandles);
}
