package cn.edu.ruc.iir.pixels.daemon.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.zookeeper
 * @ClassName: ZNodeOperation
 * @Description:
 * @author: tao
 * @date: Create in 2018-09-14 14:02
 **/
public class ZNodeOperation {

    Logger logger = Logger.getLogger(ZNodeOperation.class);
    ZookeeperDemo zookeeperDemo = new ZookeeperDemo();

    /**
     * 创建节点
     *
     * @param path 节点路径
     * @param data 节点内容
     * @return
     */
    public boolean createZNode(String path, String data) {
        try {
            String zkPath = zookeeperDemo.zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            logger.info("Zookeeper创建节点成功，节点地址：" + zkPath);
            return true;
        } catch (KeeperException e) {

            logger.error("创建节点失败：" + e.getMessage() + "，path:" + path, e);
        } catch (InterruptedException e) {
            logger.error("创建节点失败：" + e.getMessage() + "，path:" + path, e);
        }
        return false;

    }

    /**
     * 删除一个节点
     *
     * @param path 节点路径
     * @return
     */
    public boolean deteleZKNode(String path) {
        try {
            zookeeperDemo.zooKeeper.delete(path, -1);
            logger.info("Zookeeper删除节点1成功，节点地址：" + path);
            return true;
        } catch (InterruptedException e) {
            logger.error("删除节点失败：" + e.getMessage() + ",path:" + path, e);
        } catch (KeeperException e) {
            logger.error("删除节点失败：" + e.getMessage() + ",path:" + path, e);
        }
        return false;
    }

    /**
     * 更新节点内容
     *
     * @param path 节点路径
     * @param data 节点数据
     * @return
     */
    public boolean updateZKNodeData(String path, String data) {
        try {
            Stat stat = zookeeperDemo.zooKeeper.setData(path, data.getBytes(), -1);
            logger.info("更新节点数据成功，path:" + path + ", stat:" + stat);
            return true;
        } catch (KeeperException e) {
            logger.error("更新节点数据失败：" + e.getMessage() + "，path:" + path, e);
        } catch (InterruptedException e) {
            logger.error("更新节点数据失败：" + e.getMessage() + "，path:" + path, e);
        }
        return false;
    }

    /**
     * 读取指定节点的内容
     *
     * @param path 指定的路径
     * @return
     */
    public String readData(String path) {
        String data = null;
        try {
            data = new String(zookeeperDemo.zooKeeper.getData(path, false, null));
            logger.info("读取数据成功，其中path:" + path + ", data-content:" + data);
        } catch (KeeperException e) {
            logger.error("读取数据失败,发生KeeperException! path: " + path + ", errMsg:" + e.getMessage(), e);
        } catch (InterruptedException e) {
            logger.error("读取数据失败,InterruptedException! path: " + path + ", errMsg:" + e.getMessage(), e);
        }
        return data;
    }

    /**
     * 获取某个节点下的所有节点
     *
     * @param path 节点路径
     * @return
     */
    public List<String> getChild(String path) {
        try {
            List<String> list = zookeeperDemo.zooKeeper.getChildren(path, false);
            if (list.isEmpty()) {
                logger.info(path + "的路径下没有节点");
            }
            return list;
        } catch (KeeperException e) {
            logger.error("读取子节点数据失败,发生KeeperException! path: " + path
                    + ", errMsg:" + e.getMessage(), e);
        } catch (InterruptedException e) {
            logger.error("读取子节点数据失败,InterruptedException! path: " + path
                    + ", errMsg:" + e.getMessage(), e);
        }
        return null;
    }

    public boolean isExists(String path) {
        try {
            Stat stat = zookeeperDemo.zooKeeper.exists(path, false);
            return null != stat;
        } catch (KeeperException e) {
            logger.error("读取数据失败,发生KeeperException! path: " + path
                    + ", errMsg:" + e.getMessage(), e);
        } catch (InterruptedException e) {
            logger.error("读取数据失败,发生InterruptedException! path: " + path
                    + ", errMsg:" + e.getMessage(), e);
        }
        return false;
    }

    public void deteleZKNodes(String rootPath) {
        List<String> childPaths = getChild(rootPath);
        if (null != childPaths) {
            System.out.println("节点[" + rootPath + "]下的子节点数[" + childPaths.size() + "]");
            for (String childPath : childPaths) {
                System.out.println(" |--节点名[" + childPath + "] 刪除");
                deteleZKNode(rootPath + "/" + childPath);
            }
        }
        deteleZKNode(rootPath);
    }
}
