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
package io.pixelsdb.pixels.common.turbo.cleanup;

import com.alibaba.fastjson.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import io.pixelsdb.pixels.common.physical.Storage;
import java.io.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.junit.jupiter.api.Assertions.*;

class TestTemporaryResourceManager
{
    @TempDir Path directory;

    @Test
    void discoversDriversOutsideTheSubmissionThreadsContextLoader() throws Exception
    {
        Thread thread = Thread.currentThread();
        ClassLoader original = thread.getContextClassLoader();
        thread.setContextClassLoader(new ClassLoader(null) {});
        try (TemporaryResourceManager manager = new TemporaryResourceManager(
                new CleanupJournal(directory.resolve("journal"))))
        {
            String id = manager.register("plugin-query", Collections.singletonList(
                    new CleanupResource("storage-prefix", directory.resolve("data").toUri().toString() + "/")));
            assertEquals(1, manager.snapshot(id).getIntValue("resourceCount"));
        }
        finally { thread.setContextClassLoader(original); }
    }

    @Test
    void releaseAndPositiveStopEvidenceAreBothRequired() throws Exception
    {
        Driver driver = new Driver();
        try (TemporaryResourceManager manager = manager(driver))
        {
            String shuffle = register(manager, driver, "shuffle");
            manager.release(shuffle);
            manager.runOnce();
            assertEquals("WAITING_FOR_STOP", manager.snapshot(shuffle).getString("status"));
            assertEquals(0, driver.deletes.get());
            assertFalse(manager.completion(shuffle).isDone());
            manager.confirmStopped(shuffle);
            manager.runOnce();
            assertEquals("CLEANED", manager.snapshot(shuffle).getString("status"));
            assertTrue(manager.completion(shuffle).isDone());

            String result = register(manager, driver, "result");
            manager.confirmStopped(result);
            manager.runOnce();
            assertTrue(driver.live.contains("result"));
            assertEquals("ACTIVE", manager.snapshot(result).getString("status"));
            manager.release(result); // Reader closes independently of shuffle/compute completion.
            manager.runOnce();
            assertFalse(driver.live.contains("result"));
        }
    }

    @Test
    void anotherProcessCanResumeWithoutTheOriginalQueryOrCallbacks() throws Exception
    {
        Driver driver = new Driver();
        String id;
        try (TemporaryResourceManager origin = manager(driver))
        {
            id = register(origin, driver, "recoverable");
            origin.release(id);
            origin.confirmStopped(id);
        }
        try (TemporaryResourceManager recovery = manager(driver))
        {
            recovery.runOnce();
            assertEquals("CLEANED", recovery.snapshot(id).getString("status"));
            recovery.release(id);
            recovery.confirmStopped(id);
            recovery.runOnce();
            assertEquals(1, driver.deletes.get());
        }
    }

    @Test
    void acknowledgedDeleteMustStillBeVerifiedAndFailuresDoNotBlockOtherResources() throws Exception
    {
        Driver driver = new Driver();
        driver.delayDeletion = true;
        try (TemporaryResourceManager manager = manager(driver))
        {
            String id = register(manager, driver, "slow");
            manager.release(id);
            manager.confirmStopped(id);
            manager.runOnce();
            JSONObject state = manager.snapshot(id);
            assertEquals("RETRY_WAIT", state.getString("status"));
            assertTrue(state.getString("lastError").contains("still exists"));
            assertFalse(manager.completion(id).isDone());
            driver.delayDeletion = false;
            // Advance only this persisted retry deadline; no sleeps or query-lifetime callbacks.
            state.put("nextRetryAtMs", 0);
            new CleanupJournal(directory).write(id + ".status.json", state);
        }
        try (TemporaryResourceManager recovery = manager(driver))
        {
            recovery.runOnce();
            assertEquals("CLEANED", recovery.snapshot(new CleanupJournal(directory).tasks().get(0)
                    .replace(".resources.json", "")).getString("status"));
        }
    }

    @Test
    void partialFailureContinuesOtherResourcesAndSurvivesRestart() throws Exception
    {
        Driver driver = new Driver();
        driver.live.addAll(Arrays.asList("denied", "allowed"));
        driver.denied = "denied";
        String id;
        try (TemporaryResourceManager manager = manager(driver))
        {
            id = manager.register("query", Arrays.asList(new CleanupResource("test", "denied"),
                    new CleanupResource("test", "allowed")));
            manager.release(id);
            manager.confirmStopped(id);
            manager.runOnce();
            assertTrue(driver.live.contains("denied"));
            assertFalse(driver.live.contains("allowed"));
            assertEquals("RETRY_WAIT", manager.snapshot(id).getString("status"));
        }
        driver.denied = null;
        JSONObject state = new CleanupJournal(directory).read(id + ".status.json");
        state.put("nextRetryAtMs", 0);
        new CleanupJournal(directory).write(id + ".status.json", state);
        try (TemporaryResourceManager recovery = manager(driver))
        {
            recovery.runOnce();
            assertEquals("CLEANED", recovery.snapshot(id).getString("status"));
            assertTrue(driver.live.isEmpty());
        }
    }

    @Test
    void blockedDeletionDoesNotBlockNewQueryRegistrationOrRelease() throws Exception
    {
        CountDownLatch deleting = new CountDownLatch(1), resume = new CountDownLatch(1);
        Driver driver = new Driver()
        {
            public void delete(String location) throws IOException
            {
                deleting.countDown();
                try { assertTrue(resume.await(5, TimeUnit.SECONDS)); }
                catch (InterruptedException failure) { throw new IOException(failure); }
                super.delete(location);
            }
        };
        ExecutorService background = Executors.newSingleThreadExecutor();
        try (TemporaryResourceManager manager = manager(driver))
        {
            String first = register(manager, driver, "first");
            manager.release(first);
            manager.confirmStopped(first);
            Future<?> sweep = background.submit(() ->
            {
                try { manager.runOnce(); } catch (IOException failure) { throw new RuntimeException(failure); }
            });
            assertTrue(deleting.await(1, TimeUnit.SECONDS));
            String next = register(manager, driver, "next");
            manager.release(next);
            manager.confirmStopped(next);
            manager.start(); // Must not acquire the sweeping/deletion lock.
            resume.countDown();
            sweep.get(2, TimeUnit.SECONDS);
        }
        finally { resume.countDown(); background.shutdownNow(); }
    }

    @Test
    void missingDriverAndUnsafePrefixesFailBeforeAllocation() throws Exception
    {
        try (TemporaryResourceManager manager = manager(new Driver()))
        {
            assertThrows(IOException.class, () -> manager.register("query",
                    Collections.singletonList(new CleanupResource("unknown", "somewhere"))));
            assertTrue(new CleanupJournal(directory).tasks().isEmpty());
        }
        StoragePrefixCleanupDriver driver = new StoragePrefixCleanupDriver();
        assertThrows(IOException.class, () -> driver.validate("s3://bucket/"));
        assertThrows(IOException.class, () -> driver.validate("s3://bucket/query/../"));
        assertThrows(IOException.class, () -> driver.validate("s3://bucket/query"));
        driver.validate("s3://bucket/query/");
    }

    @Test
    void cleanupCannotDeleteItsOwnJournal() throws Exception
    {
        try (TemporaryResourceManager manager = new TemporaryResourceManager(new CleanupJournal(directory),
                Collections.singletonList(new StoragePrefixCleanupDriver())))
        {
            assertThrows(IOException.class, () -> manager.register("query", Collections.singletonList(
                    new CleanupResource("storage-prefix", "file://" + directory + "/"))));
            assertTrue(new CleanupJournal(directory).tasks().isEmpty());
        }
    }

    @Test
    void staleUnknownWriterIsReportedButOnlyPositiveEvidenceAllowsDeletion() throws Exception
    {
        Driver driver = new Driver();
        try (TemporaryResourceManager manager = manager(driver))
        {
            String id = register(manager, driver, "unknown-writer");
            manager.release(id);
            CleanupJournal journal = new CleanupJournal(directory);
            JSONObject intent = journal.read(id + ".resources.json");
            intent.put("createdAtMs", System.currentTimeMillis() - TimeUnit.DAYS.toMillis(2));
            journal.write(id + ".resources.json", intent);
            manager.runOnce();
            assertEquals("NEEDS_ATTENTION", manager.snapshot(id).getString("status"));
            assertTrue(driver.live.contains("unknown-writer"));
            assertFalse(manager.completion(id).isDone());
            manager.confirmStopped(id);
            manager.runOnce();
            assertEquals("CLEANED", manager.snapshot(id).getString("status"));
        }
    }

    @Test
    void objectJournalRetriesEvidencePublicationAndIsReadableByAnotherManager() throws Exception
    {
        Storage storage = mock(Storage.class);
        Map<String, byte[]> documents = new ConcurrentHashMap<>();
        AtomicBoolean failStopped = new AtomicBoolean(true);
        AtomicInteger stopWrites = new AtomicInteger();
        when(storage.exists(anyString())).thenAnswer(call -> documents.containsKey(call.getArgument(0)));
        when(storage.open(anyString())).thenAnswer(call ->
                new DataInputStream(new ByteArrayInputStream(documents.get(call.getArgument(0)))));
        when(storage.listPaths(anyString())).thenAnswer(call -> new ArrayList<>(documents.keySet()));
        when(storage.create(anyString(), anyBoolean(), anyInt())).thenAnswer(call ->
        {
            String path = call.getArgument(0);
            if (path.endsWith(".stopped.json"))
            {
                stopWrites.incrementAndGet();
                if (failStopped.getAndSet(false)) { throw new IOException("temporary object-store outage"); }
            }
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            return new DataOutputStream(bytes)
            {
                public void close() throws IOException { super.close(); documents.put(path, bytes.toByteArray()); }
            };
        });
        CleanupJournal journal = new CleanupJournal(storage, "s3://bucket/management/");
        Driver driver = new Driver();
        driver.live.add("remote");
        String id;
        try (TemporaryResourceManager manager = new TemporaryResourceManager(journal, Collections.singletonList(driver)))
        {
            id = manager.register("query", Collections.singletonList(new CleanupResource("test", "remote")));
            manager.release(id);
            manager.confirmStoppedAsync(id).get(3, TimeUnit.SECONDS);
            manager.completion(id).get(2, TimeUnit.SECONDS);
            assertTrue(stopWrites.get() >= 2);
        }
        try (TemporaryResourceManager recovery = new TemporaryResourceManager(journal, Collections.singletonList(driver)))
        {
            assertEquals("CLEANED", recovery.snapshot(id).getString("status"));
            assertTrue(driver.live.isEmpty());
        }
    }

    private TemporaryResourceManager manager(Driver driver)
    { return new TemporaryResourceManager(new CleanupJournal(directory), Collections.singletonList(driver)); }

    private String register(TemporaryResourceManager manager, Driver driver, String location) throws IOException
    {
        String id = manager.register("query", Collections.singletonList(new CleanupResource("test", location)));
        // Creation only happens after durable registration returned.
        assertNotNull(new CleanupJournal(directory).read(id + ".resources.json"));
        driver.live.add(location);
        return id;
    }

    private static class Driver implements CleanupDriver
    {
        final Set<String> live = ConcurrentHashMap.newKeySet();
        final AtomicInteger deletes = new AtomicInteger();
        boolean delayDeletion;
        String denied;
        public String getType() { return "test"; }
        public boolean exists(String location) { return live.contains(location); }
        public void delete(String location) throws IOException
        {
            deletes.incrementAndGet();
            if (location.equals(denied)) { throw new IOException("access denied"); }
            if (!delayDeletion) { live.remove(location); }
        }
    }
}
