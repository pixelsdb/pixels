package cn.edu.ruc.iir.pixels.presto.client;

import cn.edu.ruc.iir.pixels.common.DateUtil;
import cn.edu.ruc.iir.pixels.metadata.domain.Schema;
import com.alibaba.fastjson.JSON;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.presto.client
 * @ClassName: TimeClient
 * @Description: 时间服务器 客户端
 * @author: taoyouxian
 * @date: Create in 2018-01-26 15:13
 **/
public class TimeClient {

    private String action;
    private ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<String>();

    public TimeClient(String action) {
        this.action = action;
    }

    public ConcurrentLinkedQueue<String> getQueue() {
        return queue;
    }

    public void connect(int port, String host) throws Exception {
        //配置客户端NIO 线程组
        EventLoopGroup group = new NioEventLoopGroup();

        Bootstrap client = new Bootstrap();

        try {
            client.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(
                                    new MetadataCLientHandler(action, queue));
                        }
                    });

            //绑定端口, 异步连接操作
            ChannelFuture future = client.connect(host, port).sync();

            //等待客户端连接端口关闭
            future.channel().closeFuture().sync();
        } finally {
            //优雅关闭 线程组
            group.shutdownGracefully();
        }
    }

    public static void main(String[] args) {
        String action = "getSchemaNames";
        Scanner sc = new Scanner(System.in);
        System.out.print("Input your action: ");
        while (sc.hasNext()) {
            action = sc.next();
            System.out.println();
            TimeClient client = new TimeClient(action);
            try {
                new Thread(() -> {
                    try {
                        client.connect(18888, "127.0.0.1");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }).start();
                System.out.println(DateUtil.formatTime(new Date()));
                while (true) {
                    int count = client.getQueue().size();
                    if (count > 0) {
                        String res = client.getQueue().poll();
                        if (action.equals("Time")) {
                            System.out.println(res);
                        } else if (action.equals("getSchemaNames")) {
                            List<Schema> schemas = JSON.parseArray(res, Schema.class);
                            System.out.println(schemas.size());
                        }
                        break;
                    }
                    Thread.sleep(1000);
                }
                System.out.println(DateUtil.formatTime(new Date()));
                System.out.print("Input your action: ");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
