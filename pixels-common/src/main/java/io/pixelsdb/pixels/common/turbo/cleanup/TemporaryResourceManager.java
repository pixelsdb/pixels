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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Query-independent temporary resource reaper, usable by any execution backend.
 * Intent, release and stopped evidence have separate documents: a reaper never overwrites
 * engine evidence. Duplicate reapers are safe because drivers must be idempotent.
 */
public final class TemporaryResourceManager implements AutoCloseable
{
    public enum Status { ACTIVE, WAITING_FOR_STOP, CLEANING, RETRY_WAIT, NEEDS_ATTENTION, CLEANED }
    private static final Logger logger = LogManager.getLogger(TemporaryResourceManager.class);
    private static final long RETRY_MS = 5000;
    private final CleanupJournal journal;
    private final Map<String, CleanupDriver> drivers = new HashMap<>();
    private final Map<String, CompletableFuture<Void>> completions = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<Void>> publications = new ConcurrentHashMap<>();
    private final AtomicBoolean sweeping = new AtomicBoolean();
    private ScheduledExecutorService scheduler;

    public TemporaryResourceManager(CleanupJournal journal)
    {
        // SQL submission threads can belong to Trino's engine, outside the plugin loader.
        this(journal, ServiceLoader.load(CleanupDriver.class, CleanupDriver.class.getClassLoader()));
    }

    public TemporaryResourceManager(CleanupJournal journal, Iterable<CleanupDriver> drivers)
    {
        this.journal = Objects.requireNonNull(journal, "journal is null");
        for (CleanupDriver driver : drivers)
        {
            if (this.drivers.putIfAbsent(driver.getType(), driver) != null)
            { throw new IllegalArgumentException("duplicate cleanup driver: " + driver.getType()); }
        }
    }

    private static final class DefaultHolder
    {
        private static final TemporaryResourceManager INSTANCE = configured();
    }

    public static TemporaryResourceManager Instance() { return DefaultHolder.INSTANCE; }

    private static TemporaryResourceManager configured()
    {
        String location = ConfigFactory.Instance().getProperty("executor.cleanup.journal");
        if (location == null || location.trim().isEmpty())
        {
            String home = System.getenv("PIXELS_HOME");
            if (home == null) { throw new IllegalStateException("PIXELS_HOME or executor.cleanup.journal is required"); }
            location = Paths.get(home, "var", "cleanup").toString();
        }
        try
        {
            return new TemporaryResourceManager(CleanupJournal.open(location));
        }
        catch (IOException failure) { throw new UncheckedIOException(failure); }
    }

    /** Persist the complete intent BEFORE creating any resource or launching work using it. */
    public String register(String owner, Collection<CleanupResource> resources) throws IOException
    {
        String id = UUID.randomUUID().toString();
        JSONObject record = new JSONObject(true);
        record.put("version", 1);
        record.put("owner", Objects.requireNonNull(owner, "owner is null"));
        record.put("createdAtMs", System.currentTimeMillis());
        record.put("resources", resources);
        for (CleanupResource resource : resources)
        {
            driver(resource.getType()).validate(resource.getLocation());
            if (resource.getType().equals("storage-prefix") && journal.coveredBy(resource.getLocation()))
            { throw new IOException("temporary resource prefix must not contain the cleanup journal"); }
        }
        journal.write(id + ".resources.json", record);
        return id;
    }

    /** Caller has closed admission and all logical users (including result readers) have released this scope. */
    public void release(String id) throws IOException { marker(id, "released"); }

    /** Caller has positive evidence that preparation and ALL possible writers have ended. Not a lease timeout. */
    public void confirmStopped(String id) throws IOException { marker(id, "stopped"); }

    /** Retry publication off RPC/SDK threads. The future acknowledges durable evidence, not deletion. */
    public CompletableFuture<Void> confirmStoppedAsync(String id) { return publish(id, "stopped"); }

    public CompletableFuture<Void> releaseAsync(String id) { return publish(id, "released"); }

    private CompletableFuture<Void> publish(String id, String kind)
    {
        validateId(id);
        CompletableFuture<Void> future = publications.computeIfAbsent(id + "." + kind, ignored -> new CompletableFuture<>());
        start();
        return future;
    }

    private void marker(String id, String kind) throws IOException
    {
        validateId(id);
        if (!journal.exists(id + ".resources.json")) { throw new IOException("unknown cleanup scope: " + id); }
        JSONObject marker = new JSONObject();
        marker.put("recordedAtMs", System.currentTimeMillis());
        journal.write(id + "." + kind + ".json", marker);
    }

    /** Returns independent compute/release/GC evidence, including the last error and next retry. */
    public JSONObject snapshot(String id) throws IOException
    {
        validateId(id);
        JSONObject intent = journal.read(id + ".resources.json");
        if (intent == null) { throw new IOException("unknown cleanup scope: " + id); }
        if (intent.getIntValue("version") != 1) { throw new IOException("unsupported cleanup record version"); }
        JSONObject state = journal.read(id + ".status.json");
        if (state == null) { state = new JSONObject(true); }
        boolean released = journal.exists(id + ".released.json");
        boolean stopped = journal.exists(id + ".stopped.json");
        state.put("id", id);
        state.put("owner", intent.getString("owner"));
        state.put("released", released);
        state.put("writersStopped", stopped);
        state.put("resourceCount", intent.getJSONArray("resources").size());
        state.put("createdAtMs", intent.getLongValue("createdAtMs"));
        if (!released) { state.put("status", Status.ACTIVE.name()); }
        else if (!stopped) { state.put("status", Status.WAITING_FOR_STOP.name()); }
        else if (!state.containsKey("status") || Status.NEEDS_ATTENTION.name().equals(state.getString("status")))
        { state.put("status", Status.CLEANING.name()); }
        if ((!released || !stopped) && System.currentTimeMillis() - intent.getLongValue("createdAtMs") >=
                TimeUnit.DAYS.toMillis(1))
        {
            state.put("status", Status.NEEDS_ATTENTION.name());
            state.put("lastError", "missing " + (!released ? "reader/admission release" : "writer stop evidence"));
        }
        return state;
    }

    public CompletableFuture<Void> completion(String id)
    {
        validateId(id);
        CompletableFuture<Void> completion = completions.computeIfAbsent(id, ignored -> new CompletableFuture<>());
        try
        {
            if (Status.CLEANED.name().equals(snapshot(id).getString("status")))
            {
                completions.remove(id, completion);
                completion.complete(null);
            }
        }
        catch (IOException failure)
        {
            // A transient journal read failure is not a terminal cleanup outcome.
            logger.warn("Unable to read cleanup scope {}; reconciliation will retry", id, failure);
        }
        return completion;
    }

    /** Each task is isolated; a broken resource or driver must not stop the next query's recovery. */
    public void runOnce() throws IOException
    {
        if (!sweeping.compareAndSet(false, true)) { return; }
        try
        {
            for (Map.Entry<String, CompletableFuture<Void>> entry : publications.entrySet())
            {
                int separator = entry.getKey().lastIndexOf('.');
                try
                {
                    marker(entry.getKey().substring(0, separator), entry.getKey().substring(separator + 1));
                    publications.remove(entry.getKey(), entry.getValue());
                    entry.getValue().complete(null);
                }
                catch (IOException | RuntimeException failure)
                { logger.warn("Cleanup evidence {} will be republished", entry.getKey(), failure); }
            }
            for (String name : journal.tasks())
            {
                String id = name.substring(0, name.length() - ".resources.json".length());
                try { reap(id); }
                catch (Exception failure) { logger.warn("Unable to reconcile cleanup scope {}", id, failure); }
            }
        }
        finally { sweeping.set(false); }
    }

    private void reap(String id) throws IOException
    {
        JSONObject state = snapshot(id);
        if (Status.NEEDS_ATTENTION.name().equals(state.getString("status")))
        {
            JSONObject previous = journal.read(id + ".status.json");
            if (previous == null || !Status.NEEDS_ATTENTION.name().equals(previous.getString("status")))
            {
                journal.write(id + ".status.json", state);
                logger.warn("Cleanup scope {} requires backend reconciliation: {}", id, state.getString("lastError"));
            }
            return;
        }
        if (Status.CLEANED.name().equals(state.getString("status")))
        {
            CompletableFuture<Void> completion = completions.remove(id);
            if (completion != null) { completion.complete(null); }
            return;
        }
        if (!state.getBooleanValue("released") || !state.getBooleanValue("writersStopped") ||
                state.getLongValue("nextRetryAtMs") > System.currentTimeMillis()) { return; }
        JSONArray resources = journal.read(id + ".resources.json").getJSONArray("resources");
        try
        {
            state.put("status", Status.CLEANING.name());
            journal.write(id + ".status.json", state);
            IOException failure = null;
            JSONArray remaining = new JSONArray();
            for (int i = 0; i < resources.size(); i++)
            {
                JSONObject resource = resources.getJSONObject(i);
                try
                {
                    CleanupDriver driver = driver(resource.getString("type"));
                    String location = resource.getString("location");
                    driver.validate(location);
                    if (driver.exists(location)) { driver.delete(location); }
                    if (driver.exists(location)) { throw new IOException("resource still exists: " + location); }
                }
                catch (IOException | RuntimeException error)
                {
                    remaining.add(resource);
                    if (failure == null) { failure = new IOException("resource cleanup incomplete"); }
                    failure.addSuppressed(error);
                }
            }
            state.put("remainingResources", remaining);
            if (failure != null) { throw failure; }
            state.put("status", Status.CLEANED.name());
            state.remove("lastError");
            state.remove("nextRetryAtMs");
        }
        catch (IOException | RuntimeException failure)
        {
            state.put("status", Status.RETRY_WAIT.name());
            state.put("lastError", failure.toString() + Arrays.toString(failure.getSuppressed()));
            state.put("nextRetryAtMs", System.currentTimeMillis() + RETRY_MS);
            logger.warn("Cleanup scope {} will be retried", id, failure);
        }
        state.put("updatedAtMs", System.currentTimeMillis());
        journal.write(id + ".status.json", state);
        if (Status.CLEANED.name().equals(state.getString("status")))
        {
            CompletableFuture<Void> completion = completions.remove(id);
            if (completion != null) { completion.complete(null); }
        }
    }

    private CleanupDriver driver(String type) throws IOException
    {
        CleanupDriver driver = drivers.get(type);
        if (driver == null) { throw new IOException("no cleanup driver for " + type); }
        return driver;
    }

    private static void validateId(String id)
    {
        if (id == null || !UUID.fromString(id).toString().equals(id))
        { throw new IllegalArgumentException("invalid cleanup scope id"); }
    }

    public String getJournalLocation() { return journal.getLocation(); }

    /** Optional local accelerator. The SAME journal must also be swept independently by the backend. */
    public synchronized void start()
    {
        if (scheduler != null) { return; }
        scheduler = Executors.newSingleThreadScheduledExecutor(task ->
        {
            Thread thread = new Thread(task, "pixels-resource-cleanup");
            thread.setDaemon(true);
            return thread;
        });
        scheduler.scheduleWithFixedDelay(() ->
        {
            try { runOnce(); }
            catch (Exception failure) { logger.warn("Cleanup journal scan failed", failure); }
        }, 0, 1, TimeUnit.SECONDS);
    }

    @Override
    public synchronized void close()
    {
        if (scheduler != null) { scheduler.shutdownNow(); scheduler = null; }
    }

    /** Backend control processes can sweep periodically without a query engine or a planner dependency. */
    public static void main(String[] args) throws Exception
    {
        try (TemporaryResourceManager manager = configured())
        {
            if (args.length == 0) { manager.runOnce(); }
            else if (args.length == 2 && args[0].equals("--status"))
            { System.out.println(manager.snapshot(args[1]).toJSONString()); }
            else { throw new IllegalArgumentException("usage: TemporaryResourceManager [--status scope-id]"); }
        }
    }
}
