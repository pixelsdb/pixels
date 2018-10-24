package cn.edu.ruc.iir.pixels.daemon.etcd;

import cn.edu.ruc.iir.pixels.common.physical.FSFactory;
import cn.edu.ruc.iir.pixels.common.utils.EtcdUtil;
import cn.edu.ruc.iir.pixels.presto.impl.PixelsPrestoConfig;
import com.coreos.jetcd.Watch;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.etcd
 * @ClassName: EtcdQueue
 * @Description:
 * @author: tao
 * @date: Create in 2018-10-22 12:59
 **/
public class EtcdQueue {
    static PixelsPrestoConfig config = new PixelsPrestoConfig().setPixelsHome("");
    static FSFactory fsFactory = config.getFsFactory();
    static String dirPath = "hdfs://dbiir01:9000/pixels/pixels/test_105/v_2_order/";

    // 文件数目下限为8个
    private static final int FILE_MIN_SIZE = 8;

    // 文件路径
    private static final String FILE_ROOT_PATH = "/fileBox";

    // 文件节点
    private static final String FILE_NODE_NAME = "file_";

    // start, end, compacting
    static String compactStatus = "CompactStatus";
    static AtomicInteger count = new AtomicInteger(0);
    // 计数器
    private static int counter = 0;

    // 生产者线程，负责Generate
    static class Producer extends Thread {

        EtcdUtil etcdUtil;

        @Override
        public void run() {
            while (true) {
                try {
                    if (getFileNum() > FILE_MIN_SIZE) { // 文件已够
                        System.out.println("fileBox has been enough");
                        this.etcdUtil.putKeyValue(compactStatus, "start");
                    }
                    // 线程随机休眠数毫秒，模拟现实中的费时操作
                    int sleepMillis = (int) (Math.random() * 1000);
                    Thread.sleep(sleepMillis);

                    // Generate文件
                    count.getAndIncrement();
                    System.out.println("a new file has been received: "
                            + ", file num: " + getFileNum());
                } catch (Exception e) {
                    System.out.println("producer equit task becouse of exception !");
                    e.printStackTrace();
                    break;
                }
            }
        }

        public void setEtcdUtil(EtcdUtil etcdUtil) {
            this.etcdUtil = etcdUtil;
        }

    }

    private static int getFileNum() {
//        List<Path> fileList = null;
//        try {
//            fileList = fsFactory.listFiles(dirPath);
//        } catch (FSException e) {
//            e.printStackTrace();
//        }
//        return fileList.size();
        return count.get();
    }

    // 消费者线程，负责Compact
    static class Consumer extends Thread {

        EtcdUtil etcdUtil;

        @Override
        public void run() {
            Watch.Watcher watcher = etcdUtil.getClient().getWatchClient().watch(ByteSequence.fromString(compactStatus));

            // 创建Watcher，监控子节点的变化
            new Thread(() -> {
                try {
                    watcher.listen().getEvents().stream().forEach(watchEvent -> {
                        System.out.println("**********" + (counter++));
                        KeyValue kv = watchEvent.getKeyValue();
                        // get event type
                        System.out.println(watchEvent.getEventType());
                        // changed key
                        System.out.println(kv.getKey().toStringUtf8());
                        // changed value
                        System.out.println(kv.getValue().toStringUtf8());
//                    System.out.println("Producer.... lessThan");
                        System.out.println("**********");
                    });

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();

            while (true) {
                try {
                    if (getFileNum() <= FILE_MIN_SIZE) {
                        this.etcdUtil.putKeyValue(compactStatus, "end");

                    } else {
                        this.etcdUtil.putKeyValue(compactStatus, "start");
                        System.out.println("fileBox needed to be compacted");

                        // 线程随机休眠数毫秒，模拟现实中的费时操作
                        int sleepMillis = (int) (Math.random() * 1000);
                        Thread.sleep(sleepMillis);

                        // Compact，删除前8个文件
                        int curFileNum = startCompact();

                        System.out.println("file has been compacted: " + curFileNum
                                + ", file num: " + getFileNum());
                        this.etcdUtil.putKeyValue(compactStatus, "end");
                    }
                } catch (Exception e) {
                    System.out.println("consumer equit task becouse of exception !");
                    e.printStackTrace();
                    break;
                }
            }
        }

        private int startCompact() {
            this.etcdUtil.putKeyValue(compactStatus, "compacting");
            int curNum = count.get();
            System.out.println("startCompact: " + curNum);
            count.getAndSet(curNum - 8);
            return count.get();
        }

        public void setEtcdUtil(EtcdUtil etcdUtil) {
            this.etcdUtil = etcdUtil;
        }
    }

    public static void main(String[] args) throws IOException {
        // 开启生产者线程
        Producer producer = new Producer();
        EtcdUtil etcdUtilA = EtcdUtil.Instance();
        producer.setEtcdUtil(etcdUtilA);
        producer.start();

        // 开启消费者线程
        Consumer consumer = new Consumer();
        EtcdUtil etcdUtilB = EtcdUtil.Instance();
        consumer.setEtcdUtil(etcdUtilB);
        consumer.start();
    }

}
