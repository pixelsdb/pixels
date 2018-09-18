package cn.edu.ruc.iir.pixels.daemon.metadata;

import cn.edu.ruc.iir.pixels.common.serialize.KryoDecoder;
import cn.edu.ruc.iir.pixels.common.serialize.KryoEncoder;
import cn.edu.ruc.iir.pixels.common.utils.DBUtil;
import cn.edu.ruc.iir.pixels.common.utils.LogFactory;
import cn.edu.ruc.iir.pixels.daemon.Server;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

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

    public MetadataServer(int port) {
        this.port = port;
    }

    @Override
    public boolean isRunning() {
        return this.running;
    }

    @Override
    public void shutdown() {
        // close the netty server here.
        this.running = false;
        boss.shutdownGracefully();
        worker.shutdownGracefully();
    }

    @Override
    public void run() {
        //配置服务端NIO 线程组
        this.boss = new NioEventLoopGroup();
        this.worker = new NioEventLoopGroup();

        ServerBootstrap server = new ServerBootstrap();

        try {
            server.group(boss, worker)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    //TODO: currently, the message received by server can not be longer than this fixed buffer size.
//                    .childOption(ChannelOption.RCVBUF_ALLOCATOR, new FixedRecvByteBufAllocator(1024*128))
                    .childHandler(new ChannelInitializer<SocketChannel>()
                    {
                        @Override
                        protected void initChannel(SocketChannel channel) throws Exception
                        {
//                            channel.pipeline().addLast(
//                                    new ObjectEncoder(),
//                                    // 线程安全的类加载器进行缓存，支持多线程并发访问
//                                    new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.weakCachingConcurrentResolver(this.getClass().getClassLoader())),
//                                    new MetadataServerHandler());
                            channel.pipeline().addLast("decoder", new KryoDecoder());
                            channel.pipeline().addLast("encoder", new KryoEncoder());
                            channel.pipeline().addLast(new MetadataServerHandler());
                        }
                    });

            //绑定端口, 同步等待成功
            //System.out.println("port: " + port);
            this.running = true;
            ChannelFuture future = server.bind(port).sync();

            //等待服务端监听端口关闭
            future.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            LogFactory.Instance().getLog().error("error while binding port in metadata server.", e);
        } finally {
            //优雅关闭 线程组
            this.running = false;
            boss.shutdownGracefully();
            worker.shutdownGracefully();
            DBUtil.Instance().close();
        }
    }
}
