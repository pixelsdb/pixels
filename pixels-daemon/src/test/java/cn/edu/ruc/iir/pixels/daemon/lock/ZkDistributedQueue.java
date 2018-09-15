package cn.edu.ruc.iir.pixels.daemon.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.lock
 * @ClassName: ZkDistributedQueue
 * @Description: copyright[https://blog.csdn.net/u012152619/article/details/53053634]
 * @author: tao
 * @date: Create in 2018-09-15 13:24
 **/
public class ZkDistributedQueue {
    // 邮箱上限为10封信
    private static final int MAILBOX_MAX_SIZE = 10;

    // 邮箱路径
    private static final String MAILBOX_ROOT_PATH = "/mailBox";

    // 信件节点
    private static final String LETTER_NODE_NAME = "letter_";

    // 生产者线程，负责接受信件
    static class Producer extends Thread {

        ZooKeeper zkClient;

        @Override
        public void run() {
            while (true) {
                try {
                    if (getLetterNum() == MAILBOX_MAX_SIZE) { // 信箱已满
                        System.out.println("mailBox has been full");
                        // 创建Watcher，监控子节点的变化
                        Watcher watcher = new Watcher() {
                            @Override
                            public void process(WatchedEvent event) {
                                // 生产者已停止，只有消费者在活动，所以只可能出现发送信件的动作
                                System.out.println("mailBox has been not full");
                                synchronized (this) {
                                    notify(); // 唤醒生产者
                                }
                            }
                        };
                        zkClient.getChildren(MAILBOX_ROOT_PATH, watcher);

                        synchronized (watcher) {
                            watcher.wait(); // 阻塞生产者
                        }
                    } else {
                        // 线程随机休眠数毫秒，模拟现实中的费时操作
                        int sleepMillis = (int) (Math.random() * 1000);
                        Thread.sleep(sleepMillis);

                        // 接收信件，创建新的子节点
                        String newLetterPath = zkClient.create(
                                MAILBOX_ROOT_PATH + "/" + LETTER_NODE_NAME,
                                "letter".getBytes(),
                                Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT_SEQUENTIAL);
                        System.out.println("a new letter has been received: "
                                + newLetterPath.substring(MAILBOX_ROOT_PATH.length() + 1)
                                + ", letter num: " + getLetterNum());
                    }
                } catch (Exception e) {
                    System.out.println("producer equit task becouse of exception !");
                    e.printStackTrace();
                    break;
                }
            }
        }

        private int getLetterNum() throws KeeperException, InterruptedException {
            Stat stat = zkClient.exists(MAILBOX_ROOT_PATH, null);
            int letterNum = stat.getNumChildren();
            return letterNum;
        }

        public void setZkClient(ZooKeeper zkClient) {
            this.zkClient = zkClient;
        }
    }

    // 消费者线程，负责发送信件
    static class Consumer extends Thread {

        ZooKeeper zkClient;

        @Override
        public void run() {
            while (true) {
                try {
                    if (getLetterNum() == 0) { // 信箱已空
                        System.out.println("mailBox has been empty");
                        // 创建Watcher，监控子节点的变化
                        Watcher watcher = new Watcher() {
                            @Override
                            public void process(WatchedEvent event) {
                                // 消费者已停止，只有生产者在活动，所以只可能出现收取信件的动作
                                System.out.println("mailBox has been not empty");
                                synchronized (this) {
                                    notify(); // 唤醒消费者
                                }
                            }
                        };
                        zkClient.getChildren(MAILBOX_ROOT_PATH, watcher);

                        synchronized (watcher) {
                            watcher.wait(); // 阻塞消费者
                        }
                    } else {
                        // 线程随机休眠数毫秒，模拟现实中的费时操作
                        int sleepMillis = (int) (Math.random() * 1000);
                        Thread.sleep(sleepMillis);

                        // 发送信件，删除序号最小的子节点
                        String firstLetter = getFirstLetter();
                        zkClient.delete(MAILBOX_ROOT_PATH + "/" + firstLetter, -1);
                        System.out.println("a letter has been delivered: " + firstLetter
                                + ", letter num: " + getLetterNum());
                    }
                } catch (Exception e) {
                    System.out.println("consumer equit task becouse of exception !");
                    e.printStackTrace();
                    break;
                }
            }
        }

        private int getLetterNum() throws KeeperException, InterruptedException {
            Stat stat = zkClient.exists(MAILBOX_ROOT_PATH, false);
            int letterNum = stat.getNumChildren();
            return letterNum;
        }

        private String getFirstLetter() throws KeeperException, InterruptedException {
            List<String> letterPaths = zkClient.getChildren(MAILBOX_ROOT_PATH, false);
            Collections.sort(letterPaths);
            return letterPaths.get(0);
        }

        public void setZkClient(ZooKeeper zkClient) {
            this.zkClient = zkClient;
        }
    }

    public static void main(String[] args) throws IOException {
        // 开启生产者线程
        Producer producer = new Producer();
        ZooKeeper zkClientA = new ZooKeeper("dbiir02:2181", 3000, null);
        producer.setZkClient(zkClientA);
        producer.start();

        // 开启消费者线程
        Consumer consumer = new Consumer();
        ZooKeeper zkClientB = new ZooKeeper("dbiir02:2181", 3000, null);
        consumer.setZkClient(zkClientB);
        consumer.start();
    }
}
