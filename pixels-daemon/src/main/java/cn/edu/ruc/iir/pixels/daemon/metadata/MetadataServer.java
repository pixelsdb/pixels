package cn.edu.ruc.iir.pixels.daemon.metadata;

import cn.edu.ruc.iir.pixels.common.LogFactory;
import cn.edu.ruc.iir.pixels.daemon.Server;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.metadata.server
 * @ClassName: MetadataServer
 * @Description: 时间服务器 服务端
 * @author: taoyouxian
 * @date: Create in 2018-01-26 15:09
 **/
public class MetadataServer implements Server {
    private boolean running = false;
    private int port;
    private EventLoopGroup boss = null;
    private EventLoopGroup worker = null;

    public MetadataServer(int port)
    {
        this.port = port;
    }

    @Override
    public boolean isRunning()
    {
        return this.running;
    }

    @Override
    public void shutdown()
    {
        // close the netty server here.
        this.running = false;
        boss.shutdownGracefully();
        worker.shutdownGracefully();
    }

    @Override
    public void run()
    {
        //配置服务端NIO 线程组
        this.boss = new NioEventLoopGroup();
        this.worker = new NioEventLoopGroup();

        ServerBootstrap server = new ServerBootstrap();

        try {
            server.group(boss, worker)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .childHandler(new ChildChannelInitializer());

            //绑定端口, 同步等待成功
            ChannelFuture future = server.bind(port).sync();

            //等待服务端监听端口关闭
            future.channel().closeFuture().sync();
            this.running = true;
        } catch (InterruptedException e)
        {
            LogFactory.Instance().getLog().error("error while binding port in metadata server.", e);
        } finally {
            //优雅关闭 线程组
            this.running = false;
            boss.shutdownGracefully();
            worker.shutdownGracefully();
        }
    }
}
