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
 * You should have received a copy of the GNU Affero General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.daemon;

import io.pixelsdb.pixels.common.server.Server;
import io.pixelsdb.pixels.daemon.exception.NoSuchServerException;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestServerContainer
{
    private static final long WAIT_TIMEOUT_SECONDS = 2;
    private static final long NO_EVENT_TIMEOUT_MILLIS = 100;

    @Test
    public void testAddServerRegistersStartsAndShutsDownServer() throws Exception
    {
        BlockingServer server = new BlockingServer(true);
        ServerContainer container = new ServerContainer();
        try
        {
            container.addServer("managed", server);

            assertTrue(server.started.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertEquals(Collections.singletonList("managed"), container.getServerNames());
            assertTrue(container.checkServer("managed"));
        }
        finally
        {
            stopBlockingServer(container, "managed", server);
        }
        assertEquals(1, server.shutdownCount.get());
        assertFalse(container.checkServer("managed"));
    }

    @Test
    public void testStartServerDoesNotDuplicateActiveLifecycleThread() throws Exception
    {
        BlockingServer server = new BlockingServer(false);
        ServerContainer container = new ServerContainer();
        try
        {
            container.addServer("slow", server);
            assertTrue(server.started.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));

            container.startServer("slow");

            assertFalse(server.secondRun.await(
                    NO_EVENT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
            assertEquals(1, server.runCount.get());
        }
        finally
        {
            stopBlockingServer(container, "slow", server);
        }
    }

    @Test
    public void testStartupChecksRunInOrderBeforeServer() throws Exception
    {
        BlockingServer server = new BlockingServer(false);
        BlockingStartupCheck firstCheck = new BlockingStartupCheck("first check");
        BlockingStartupCheck secondCheck = new BlockingStartupCheck("second check");
        ServerContainer container = new ServerContainer();
        try
        {
            container.addServer("checked", server, firstCheck, secondCheck);

            assertTrue(firstCheck.entered.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertFalse(secondCheck.entered.await(
                    NO_EVENT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));

            container.startServer("checked");
            firstCheck.allowReady();

            assertTrue(secondCheck.entered.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertFalse(server.started.await(NO_EVENT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));

            container.startServer("checked");
            secondCheck.allowReady();

            assertTrue(server.started.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
            assertEquals(1, server.runCount.get());
            assertFalse(server.secondRun.await(
                    NO_EVENT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
        }
        finally
        {
            firstCheck.allowReady();
            secondCheck.allowReady();
            stopBlockingServer(container, "checked", server);
        }
    }

    @Test
    public void testStartupCheckFailurePreventsServerStartup() throws Exception
    {
        BlockingServer server = new BlockingServer(false);
        FailingStartupCheck startupCheck = new FailingStartupCheck();
        ServerContainer container = new ServerContainer();
        container.addServer("failing", server, startupCheck);

        assertTrue(startupCheck.executed.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        awaitLifecycleStopped(container, "failing");

        assertEquals(0, server.runCount.get());
        assertEquals(1L, server.started.getCount());
    }

    @Test
    public void testShutdownInterruptsPendingStartupCheck() throws Exception
    {
        BlockingServer server = new BlockingServer(false);
        BlockingStartupCheck startupCheck = new BlockingStartupCheck("blocking check");
        ServerContainer container = new ServerContainer();
        try
        {
            container.addServer("waiting", server, startupCheck);
            assertTrue(startupCheck.entered.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));

            container.shutdownServer("waiting");

            assertTrue(startupCheck.interrupted.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
            awaitLifecycleStopped(container, "waiting");
            assertEquals(0, server.runCount.get());
        }
        finally
        {
            container.shutdownServer("waiting");
            startupCheck.allowReady();
            server.release();
            awaitLifecycleStopped(container, "waiting");
        }
    }

    @Test
    public void testRunningServerIsNotRestartedAfterRunReturns() throws Exception
    {
        AsyncServer server = new AsyncServer();
        ServerContainer container = new ServerContainer();
        try
        {
            container.addServer("async", server);
            assertTrue(server.started.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
            awaitLifecycleStopped(container, "async");

            container.startServer("async");

            assertFalse(server.restarted.await(
                    NO_EVENT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
            assertEquals(1, server.runCount.get());
            assertTrue(server.isRunning());
        }
        finally
        {
            container.shutdownServer("async");
        }
        assertEquals(1, server.shutdownCount.get());
        assertFalse(server.isRunning());
    }

    @Test
    public void testStoppedServerCanBeRestarted() throws Exception
    {
        RestartableServer server = new RestartableServer();
        ServerContainer container = new ServerContainer();

        container.addServer("restartable", server);
        assertTrue(server.firstRun.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        awaitLifecycleStopped(container, "restartable");

        container.startServer("restartable");

        assertTrue(server.secondRun.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        awaitLifecycleStopped(container, "restartable");
        assertEquals(2, server.runCount.get());
    }

    @Test
    public void testUnknownServerOperationsFail() throws Exception
    {
        ServerContainer container = new ServerContainer();

        assertNoSuchServer(() -> container.startServer("missing"));
        assertNoSuchServer(() -> container.checkServer("missing"));
        assertNoSuchServer(() -> container.shutdownServer("missing"));
    }

    @Test
    public void testDuplicateServerRegistrationIsRejected() throws Exception
    {
        BlockingServer first = new BlockingServer(false);
        BlockingServer duplicate = new BlockingServer(false);
        ServerContainer container = new ServerContainer();
        try
        {
            container.addServer("duplicate", first);
            assertTrue(first.started.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));

            try
            {
                container.addServer("duplicate", duplicate);
                fail("duplicate server registration should fail");
            }
            catch (IllegalArgumentException expected)
            {
                assertEquals("server is already registered: duplicate", expected.getMessage());
            }

            assertEquals(1, first.runCount.get());
            assertEquals(0, duplicate.runCount.get());
        }
        finally
        {
            stopBlockingServer(container, "duplicate", first);
        }
    }

    private static void stopBlockingServer(
            ServerContainer container, String name, BlockingServer server) throws Exception
    {
        try
        {
            container.shutdownServer(name);
        }
        finally
        {
            server.release();
        }
        if (server.started.getCount() == 0)
        {
            assertTrue(server.stopped.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }
        awaitLifecycleStopped(container, name);
    }

    private static void awaitLifecycleStopped(ServerContainer container, String name)
            throws Exception
    {
        long deadline = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(WAIT_TIMEOUT_SECONDS);
        while (container.checkServer(name) && System.nanoTime() < deadline)
        {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        assertFalse("server lifecycle thread did not stop", container.checkServer(name));
    }

    private static void assertNoSuchServer(ServerOperation operation) throws Exception
    {
        try
        {
            operation.run();
            fail("operation on an unknown server should fail");
        }
        catch (NoSuchServerException expected)
        {
            // Expected.
        }
    }

    private interface ServerOperation
    {
        void run() throws NoSuchServerException;
    }

    private static final class BlockingStartupCheck implements StartupCheck
    {
        private final String description;
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch ready = new CountDownLatch(1);
        private final CountDownLatch interrupted = new CountDownLatch(1);

        private BlockingStartupCheck(String description)
        {
            this.description = description;
        }

        @Override
        public String getDescription()
        {
            return description;
        }

        @Override
        public void awaitReady(long deadlineNanos) throws InterruptedException
        {
            entered.countDown();
            try
            {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0
                        || !ready.await(remainingNanos, TimeUnit.NANOSECONDS))
                {
                    throw new IllegalStateException(description + " timed out");
                }
            }
            catch (InterruptedException e)
            {
                interrupted.countDown();
                throw e;
            }
        }

        private void allowReady()
        {
            ready.countDown();
        }
    }

    private static final class FailingStartupCheck implements StartupCheck
    {
        private final CountDownLatch executed = new CountDownLatch(1);

        @Override
        public String getDescription()
        {
            return "failing startup check";
        }

        @Override
        public void awaitReady(long deadlineNanos)
        {
            executed.countDown();
            throw new IllegalStateException("test startup check failed");
        }
    }

    private static final class BlockingServer implements Server
    {
        private final boolean reportRunning;
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch stopped = new CountDownLatch(1);
        private final CountDownLatch stop = new CountDownLatch(1);
        private final CountDownLatch secondRun = new CountDownLatch(1);
        private final AtomicInteger runCount = new AtomicInteger();
        private final AtomicInteger shutdownCount = new AtomicInteger();
        private volatile boolean running;

        private BlockingServer(boolean reportRunning)
        {
            this.reportRunning = reportRunning;
        }

        @Override
        public boolean isRunning()
        {
            return reportRunning && running;
        }

        @Override
        public void run()
        {
            if (runCount.incrementAndGet() > 1)
            {
                secondRun.countDown();
            }
            running = true;
            started.countDown();
            try
            {
                stop.await();
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
            finally
            {
                running = false;
                stopped.countDown();
            }
        }

        @Override
        public void shutdown()
        {
            shutdownCount.incrementAndGet();
            stop.countDown();
        }

        private void release()
        {
            stop.countDown();
        }
    }

    private static final class AsyncServer implements Server
    {
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch restarted = new CountDownLatch(1);
        private final AtomicInteger runCount = new AtomicInteger();
        private final AtomicInteger shutdownCount = new AtomicInteger();
        private volatile boolean running;

        @Override
        public boolean isRunning()
        {
            return running;
        }

        @Override
        public void run()
        {
            if (runCount.incrementAndGet() > 1)
            {
                restarted.countDown();
            }
            running = true;
            started.countDown();
        }

        @Override
        public void shutdown()
        {
            shutdownCount.incrementAndGet();
            running = false;
        }
    }

    private static final class RestartableServer implements Server
    {
        private final CountDownLatch firstRun = new CountDownLatch(1);
        private final CountDownLatch secondRun = new CountDownLatch(1);
        private final AtomicInteger runCount = new AtomicInteger();

        @Override
        public boolean isRunning()
        {
            return false;
        }

        @Override
        public void run()
        {
            int currentRun = runCount.incrementAndGet();
            if (currentRun == 1)
            {
                firstRun.countDown();
            }
            else if (currentRun == 2)
            {
                secondRun.countDown();
            }
        }

        @Override
        public void shutdown()
        {
        }
    }
}
