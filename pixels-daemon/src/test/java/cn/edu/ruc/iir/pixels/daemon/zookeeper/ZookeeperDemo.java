package cn.edu.ruc.iir.pixels.daemon.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.zookeeper
 * @ClassName: ZookeeperDemo
 * @Description:
 * @author: tao
 * @date: Create in 2018-09-14 13:23
 **/
public class ZookeeperDemo implements Watcher {

    Logger logger = Logger.getLogger(ZookeeperDemo.class);

    protected CountDownLatch countDownLatch = new CountDownLatch(1);

    //缓存时间
    private static final int SESSION_TIME = 2000;

    public static ZooKeeper zooKeeper = null;
    private String hosts = null;

    /**
     * 监控所有被触发的事件
     *
     * @param watchedEvent
     */
    public void process(WatchedEvent watchedEvent) {
        logger.info("收到事件通知：" + watchedEvent.getState());
        if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
            countDownLatch.countDown();
        } else if (watchedEvent.getState() == Event.KeeperState.Disconnected) {
            //　
            close();
            connect(hosts);
        }
    }

    public void connect(String hosts) {
        try {
            this.hosts = hosts;
            if (zooKeeper == null) {
                //zk客户端允许我们将ZK服务的所有地址进行配置
                zooKeeper = new ZooKeeper(hosts, SESSION_TIME, this);
                //使用countDownLatch的await
                countDownLatch.await();
            }

        } catch (IOException e) {
            logger.error("连接创建失败，发生 IOException :" + e.getMessage());
        } catch (InterruptedException e) {
            logger.error("连接创建失败，发生 InterruptedException :" + e.getMessage());
        }
    }

    /**
     * 关闭连接
     */
    public void close() {
        try {
            if (zooKeeper != null) {
                zooKeeper.close();
            }
        } catch (InterruptedException e) {
            logger.error("释放连接错误 ：" + e.getMessage());
        }
    }
}
