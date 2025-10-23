package io.pixelsdb.pixels.index.rocksdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.rocksdb.ReadOptions;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

public class ReadOptionsThreadFactoryTest {

    @Test
    @Timeout(10)
    public void testThreadLocalCleanupOnThreadExit() throws InterruptedException, ExecutionException {
        ReadOptionsThreadFactory factory = new ReadOptionsThreadFactory();
        AtomicInteger readOptionsCreated = new AtomicInteger(0);
        AtomicInteger readOptionsClosed = new AtomicInteger(0);

        // 使用单个线程执行任务
        ExecutorService executor = Executors.newSingleThreadExecutor(factory);

        Future<?> future = executor.submit(() -> {
            ReadOptions opt = factory.getReadOptions();
            readOptionsCreated.incrementAndGet();
            // 模拟工作
            assertNotNull(opt);
            // 线程结束后应该自动清理
        });

        future.get(); // 等待任务完成
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        // 强制调用清理
        factory.shutdownAllThreads();

        // 验证资源统计
        assertEquals(readOptionsCreated.get(), readOptionsClosed.get(),
                "创建的ReadOptions数量应该等于关闭的数量");
    }

    @Test
    public void testMultipleThreadsResourceCleanup() throws InterruptedException {
        ReadOptionsThreadFactory factory = new ReadOptionsThreadFactory();
        int threadCount = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount, factory);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    ReadOptions opt = factory.getReadOptions();
                    assertNotNull(opt);
                    // 模拟一些工作
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        endLatch.await(5, TimeUnit.SECONDS);

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        // 调用清理并验证没有资源泄漏
        factory.shutdownAllThreads();
    }
}
