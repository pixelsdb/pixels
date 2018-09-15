package cn.edu.ruc.iir.pixels.daemon.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.lock
 * @ClassName: ZkDistributedLock
 * @Description: copyright[https://blog.csdn.net/u012152619/article/details/53053634]
 * @author: tao
 * @date: Create in 2018-09-15 10:41
 **/
public class ZkDistributedLock {
    // 以一个静态变量来模拟公共资源
    private static int counter = 0;

    public static void plus() {

        // 计数器加一
        counter++;

        // 线程随机休眠数毫秒，模拟现实中的费时操作
        int sleepMillis = (int) (Math.random() * 100);
        try {
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // 线程实现类
    static class CountPlus extends Thread {

        private static final String LOCK_ROOT_PATH = "/Locks";
        private static final String LOCK_NODE_NAME = "Lock_";

        // 每个线程持有一个zk客户端，负责获取锁与释放锁
        ZooKeeper zkClient;

        @Override
        public void run() {

            for (int i = 0; i < 20; i++) {

                // 访问计数器之前需要先获取锁
                String path = getLock();

                // 执行任务
                plus();

                // 执行完任务后释放锁
                releaseLock(path);
            }

            closeZkClient();
            System.out.println(Thread.currentThread().getName() + "执行完毕：" + counter);
        }

        /**
         * 获取锁，即创建子节点，当该节点成为序号最小的节点时则获取锁
         */
        private String getLock() {
            try {
                // 创建EPHEMERAL_SEQUENTIAL类型节点
                String lockPath = zkClient.create(LOCK_ROOT_PATH + "/" + LOCK_NODE_NAME,
                        Thread.currentThread().getName().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL_SEQUENTIAL);
                System.out.println(Thread.currentThread().getName() + " create path : " + lockPath);

                // 尝试获取锁
                tryLock(lockPath);

                return lockPath;
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        /**
         * 该函数是一个递归函数 如果获得锁，直接返回；否则，阻塞线程，等待上一个节点释放锁的消息，然后重新tryLock
         */
        private boolean tryLock(String lockPath) throws KeeperException, InterruptedException {

            // 获取LOCK_ROOT_PATH下所有的子节点，并按照节点序号排序
            List<String> lockPaths = zkClient.getChildren(LOCK_ROOT_PATH, false);
            Collections.sort(lockPaths);

            int index = lockPaths.indexOf(lockPath.substring(LOCK_ROOT_PATH.length() + 1));
            if (index == 0) { // lockPath是序号最小的节点，则获取锁
                System.out.println(Thread.currentThread().getName() + " get lock, lockPath: " + lockPath);
                return true;
            } else { // lockPath不是序号最小的节点

                // 创建Watcher，监控lockPath的前一个节点
                Watcher watcher = new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        System.out.println(event.getPath() + " has been deleted");
                        synchronized (this) {
                            notifyAll();
                        }
                    }
                };
                String preLockPath = lockPaths.get(index - 1);
                Stat stat = zkClient.exists(LOCK_ROOT_PATH + "/" + preLockPath, watcher);

                if (stat == null) { // 由于某种原因，前一个节点不存在了（比如连接断开），重新tryLock
                    return tryLock(lockPath);
                } else { // 阻塞当前进程，直到preLockPath释放锁，重新tryLock
                    System.out.println(Thread.currentThread().getName() + " wait for " + preLockPath);
                    synchronized (watcher) {
                        watcher.wait();
                    }
                    return tryLock(lockPath);
                }
            }

        }

        /**
         * 释放锁，即删除lockPath节点
         */
        private void releaseLock(String lockPath) {
            try {
                zkClient.delete(lockPath, -1);
            } catch (InterruptedException | KeeperException e) {
                e.printStackTrace();
            }
        }

        public void setZkClient(ZooKeeper zkClient) {
            this.zkClient = zkClient;
        }

        public void closeZkClient() {
            try {
                zkClient.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public CountPlus(String threadName) {
            super(threadName);
        }
    }

    public static void main(String[] args) throws Exception {
        ZooKeeper zkClient = new ZooKeeper("dbiir02:2181", 3000, null);

        //  创建根节点　-> create　/root rootData
//        String rootNode = "/Locks";
//        String lockPath = zkClient.create(rootNode,
//                rootNode.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
//                CreateMode.PERSISTENT);
//        System.out.println(Thread.currentThread().getName() + " create path : " + lockPath);

        // 开启五个线程
        CountPlus threadA = new CountPlus("threadA");
        setZkClient(threadA);
        threadA.start();

        CountPlus threadB = new CountPlus("threadB");
        setZkClient(threadB);
        threadB.start();

        CountPlus threadC = new CountPlus("threadC");
        setZkClient(threadC);
        threadC.start();

        CountPlus threadD = new CountPlus("threadD");
        setZkClient(threadD);
        threadD.start();

        CountPlus threadE = new CountPlus("threadE");
        setZkClient(threadE);
        threadE.start();
    }

    public static void setZkClient(CountPlus thread) throws Exception {
        ZooKeeper zkClient = new ZooKeeper("dbiir02:2181", 3000, null);
        thread.setZkClient(zkClient);
    }
}
