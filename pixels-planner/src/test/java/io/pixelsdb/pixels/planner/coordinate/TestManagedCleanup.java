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
package io.pixelsdb.pixels.planner.coordinate;

import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.common.turbo.cleanup.*;
import io.pixelsdb.pixels.planner.plan.physical.Operator;
import io.pixelsdb.pixels.planner.plan.physical.ScanOperator.ScanOutputCollection;
import io.pixelsdb.pixels.planner.plan.physical.domain.ShuffleInfo;
import org.junit.Test;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.Assert.*;

public class TestManagedCleanup
{
    private static final AtomicLong IDS = new AtomicLong(1277280000000L);

    @Test(timeout = 5000)
    public void logicalSuccessDoesNotWaitForManagedDeletion() throws Exception
    {
        CountDownLatch deleting = new CountDownLatch(1), resume = new CountDownLatch(1);
        Driver driver = new Driver()
        {
            public void delete(String location) throws IOException
            {
                deleting.countDown();
                try { resume.await(); } catch (InterruptedException failure) { throw new IOException(failure); }
                super.delete(location);
            }
        };
        try (Harness h = new Harness(driver))
        {
            h.execution.execute().get(1, TimeUnit.SECONDS);
            assertNotNull(h.execution.collectOutputs());
            assertTrue(deleting.await(2, TimeUnit.SECONDS));
            assertFalse(h.execution.getCleanupCompletion().isDone());
            h.execution.getComputeCompletion().get(1, TimeUnit.SECONDS);
            assertNull(PlanCoordinatorFactory.Instance().getPlanCoordinator(h.id));
            assertEquals("SUCCEEDED", h.state().getStatus().name());
            resume.countDown();
            h.execution.getCleanupCompletion().get(2, TimeUnit.SECONDS);
            assertTrue(driver.live.isEmpty());
        }
        finally { resume.countDown(); }
    }

    @Test(timeout = 5000)
    public void lateStopEvidenceStillCompletesCleanupInsteadOfBecomingTerminalDeferred() throws Exception
    {
        Driver driver = new Driver();
        try (Harness h = new Harness(driver))
        {
            String invocation = h.state().reserveInvocation(1);
            assertTrue(h.state().registerInvocation(invocation, 1, 17));
            h.execution.execute().get(1, TimeUnit.SECONDS);
            assertNotNull(h.execution.collectOutputs());
            h.manager.runOnce();
            assertEquals("WAITING_FOR_STOP", h.manager.snapshot(h.execution.getCleanupScope()).getString("status"));
            assertTrue(driver.live.contains("shuffle"));
            assertFalse(h.execution.getComputeCompletion().isDone());
            h.state().workerStopped(invocation, 17);
            h.execution.getCleanupCompletion().get(2, TimeUnit.SECONDS);
            assertFalse(driver.live.contains("shuffle"));
        }
    }

    @Test(timeout = 5000)
    public void outputReaderReleaseIsIndependentOfShuffleCleanup() throws Exception
    {
        Driver driver = new Driver();
        try (Harness h = new Harness(driver))
        {
            String resultScope = h.execution.registerResultResources(
                    Collections.singletonList(new CleanupResource("test", "result")));
            driver.live.add("result");
            h.execution.execute().get(1, TimeUnit.SECONDS);
            h.execution.collectOutputs();
            h.execution.getCleanupCompletion().get(2, TimeUnit.SECONDS);
            assertTrue(driver.live.contains("result"));
            h.execution.releaseResultResources();
            h.manager.completion(resultScope).get(2, TimeUnit.SECONDS);
            assertFalse(driver.live.contains("result"));
        }
    }

    @Test(timeout = 5000)
    public void failedPreparationRetainsFullIntentAndIsRecovered() throws Exception
    {
        Driver driver = new Driver();
        try (Harness h = new Harness(driver))
        {
            h.lifecycle.failPreparation = true;
            try { h.execution.execute().get(1, TimeUnit.SECONDS); fail("expected preparation failure"); }
            catch (ExecutionException expected) { assertEquals("create response lost", expected.getCause().getMessage()); }
            h.execution.getCleanupCompletion().get(2, TimeUnit.SECONDS);
            assertTrue(driver.live.isEmpty());
            assertEquals("ABORTED", h.state().getStatus().name());
        }
    }

    @Test(timeout = 10000)
    public void independentJvmSweepsTheSameJournalWithoutPlannerOrQueryCallbacks() throws Exception
    {
        Path root = Files.createTempDirectory("cleanup-process-");
        Path data = Files.createDirectories(root.resolve("query-data"));
        Path records = root.resolve("journal");
        CleanupJournal journal = new CleanupJournal(records);
        String id;
        try (TemporaryResourceManager origin = new TemporaryResourceManager(journal,
                Collections.singletonList(new StoragePrefixCleanupDriver())))
        {
            id = origin.register("query", Collections.singletonList(new CleanupResource("storage-prefix",
                    "file://" + data + "/")));
            Files.write(data.resolve("output"), new byte[]{1});
            origin.release(id);
            origin.confirmStopped(id);
        }
        Path config = root.resolve("pixels.properties"), log = root.resolve("reaper.log");
        Files.write(config, ("enabled.storage.schemes=file\nexecutor.cleanup.journal=" + records + "\n")
                .getBytes(StandardCharsets.UTF_8));
        ProcessBuilder builder = new ProcessBuilder(Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                TemporaryResourceManager.class.getName());
        builder.environment().put("PIXELS_CONFIG", config.toString());
        Process process = builder.redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try
        {
            assertTrue("reaper did not exit", process.waitFor(5, TimeUnit.SECONDS));
            assertEquals(new String(Files.readAllBytes(log), StandardCharsets.UTF_8), 0, process.exitValue());
            assertFalse(Files.exists(data));
            assertEquals("CLEANED", journal.read(id + ".status.json").getString("status"));
        }
        finally { process.destroyForcibly(); }
    }

    private static class Driver implements CleanupDriver
    {
        final Set<String> live = ConcurrentHashMap.newKeySet();
        public String getType() { return "test"; }
        public boolean exists(String location) { return live.contains(location); }
        public void delete(String location) throws IOException { live.remove(location); }
    }

    private static class Harness implements AutoCloseable
    {
        final long id = IDS.getAndIncrement();
        final Path directory;
        final TemporaryResourceManager manager;
        final CoordinatedPlanExecution execution;
        final Lifecycle lifecycle;
        Harness(Driver driver) throws Exception
        {
            directory = Files.createTempDirectory("managed-cleanup-");
            manager = new TemporaryResourceManager(new CleanupJournal(directory), Collections.singletonList(driver));
            lifecycle = new Lifecycle(driver, new CleanupJournal(directory));
            Operator root = new StubOperator();
            PlanCoordinatorFactory factory = PlanCoordinatorFactory.Instance();
            PlanCoordinator plan = factory.createPlanCoordinator(id, root, new CoordinatorEndpoint("localhost", 18894));
            execution = new CoordinatedPlanExecution(id, root, plan, factory, lifecycle,
                    new QueryExecutionConfig(3000, 1, 1, directory.resolve("manifests")), () -> {}, manager);
        }
        QueryExecutionState state() { return execution.getPlanCoordinator().getQueryState(); }
        public void close() { execution.close(); manager.close(); }
    }

    private static class Lifecycle implements ShuffleResourceLifecycle
    {
        final Driver driver;
        final CleanupJournal journal;
        boolean failPreparation;
        Lifecycle(Driver driver, CleanupJournal journal) { this.driver = driver; this.journal = journal; }
        public List<CleanupResource> describeResources(Collection<ShuffleInfo> infos)
        { return Collections.singletonList(new CleanupResource("test", "shuffle")); }
        public void prepare(Collection<ShuffleInfo> infos) throws IOException
        {
            assertFalse("intent must precede creation", journal.tasks().isEmpty());
            driver.live.add("shuffle");
            if (failPreparation) { throw new IOException("create response lost"); }
        }
        public void cleanup() { fail("physical deletion belongs to the resource manager"); }
    }

    private static class StubOperator extends Operator
    {
        StubOperator() { super("managed-cleanup-test"); }
        public void initPlanCoordinator(PlanCoordinator plan, int parent, boolean wide) {}
        public CompletableFuture<CompletableFuture<? extends Output>[]> execute()
        { return CompletableFuture.completedFuture(new CompletableFuture[0]); }
        public CompletableFuture<Void> executePrev() { return CompletableFuture.completedFuture(null); }
        public OutputCollection collectOutputs() { return new ScanOutputCollection(); }
    }
}
