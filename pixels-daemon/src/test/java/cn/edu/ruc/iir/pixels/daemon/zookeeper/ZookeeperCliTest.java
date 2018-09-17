package cn.edu.ruc.iir.pixels.daemon.zookeeper;

import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.zookeeper
 * @ClassName: ZookeeperCliTest
 * @Description:
 * @author: tao
 * @date: Create in 2018-09-14 14:11
 **/
public class ZookeeperCliTest {
    public static void main(String[] args) {
        //定义父子类节点路径
        String rootPath = "/ZookeeperRoot00";
        String childPath1 = rootPath + "/child101";
        String childPath2 = rootPath + "/child201";
        String hosts = "dbiir02:2181";

        //ZNodeOperation操作API
        ZNodeOperation zNodeOperation = new ZNodeOperation();

        //连接Zookeeper服务器
        ZookeeperDemo zookeeperDemo = new ZookeeperDemo();
        zookeeperDemo.connect(hosts);

        if (zNodeOperation.isExists(rootPath)) {
            zNodeOperation.deteleZKNodes(rootPath);
        }

        //创建节点
        if (zNodeOperation.createZNode(rootPath, "<父>父节点数据")) {
            System.out.println("节点 [ " + rootPath + " ],数据 [ " + zNodeOperation.readData(rootPath) + " ]");
        }

        // 创建子节点, 读取 + 删除
        if (zNodeOperation.createZNode(childPath1, "<父-子(1)>节点数据")) {
            System.out.println("节点[" + childPath1 + "]数据内容[" + zNodeOperation.readData(childPath1) + "]");
            zNodeOperation.deteleZKNode(childPath1);
            if (zNodeOperation.isExists(childPath1)) {
                System.out.println("节点[" + childPath1 + "]删除值后[" + zNodeOperation.readData(childPath1) + "]");
            }else{
                System.out.println("节点[" + childPath1 + "]已经删除");
            }
        }

        // 创建子节点, 读取 + 修改
        if (zNodeOperation.createZNode(childPath2, "<父-子(2)>节点数据")) {
            System.out.println("节点[" + childPath2 + "]数据内容[" + zNodeOperation.readData(childPath2) + "]");
            zNodeOperation.updateZKNodeData(childPath2, "<父-子(2)>节点数据,更新后的数据");
            System.out.println("节点[" + childPath2 + "]数据内容更新后[" + zNodeOperation.readData(childPath2) + "]");
        }

        // 获取子节点
        List<String> childPaths = zNodeOperation.getChild(rootPath);
        if (null != childPaths) {
            System.out.println("节点[" + rootPath + "]下的子节点数[" + childPaths.size() + "]");
            for (String childPath : childPaths) {
                System.out.println(" |--节点名[" + childPath + "]");
            }
        }
        // 判断节点是否存在
        System.out.println("检测节点[" + rootPath + "]是否存在:" + zNodeOperation.isExists(rootPath));
        System.out.println("检测节点[" + childPath1 + "]是否存在:" + zNodeOperation.isExists(childPath1));
        System.out.println("检测节点[" + childPath2 + "]是否存在:" + zNodeOperation.isExists(childPath2));


        zookeeperDemo.close();
    }
}
