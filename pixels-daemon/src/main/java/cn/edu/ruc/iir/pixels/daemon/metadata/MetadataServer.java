package cn.edu.ruc.iir.pixels.daemon.metadata;

import cn.edu.ruc.iir.pixels.common.utils.DBUtil;
import cn.edu.ruc.iir.pixels.daemon.Server;
import io.grpc.ServerBuilder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.metadata.server
 * @ClassName: MetadataServer
 * @author: taoyouxian
 * @date: Create in 2018-01-26 15:09
 **/
public class MetadataServer implements Server {
    private static Logger log = LogManager.getLogger(MetadataServer.class);

    private boolean running = false;
    private final int port;
    private final io.grpc.Server rpcServer;
    private EventLoopGroup boss = null;
    private EventLoopGroup worker = null;

    public MetadataServer(int port) {
        assert (port > 0 && port <= 65535);
        this.port = port;
        this.rpcServer = ServerBuilder.forPort(port)
                .addService(new MetadataServiceImpl())
                .build();
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
        DBUtil.Instance().close();
    }

    @Override
    public void run() {
        // configure server-end NIO thread group.
        this.boss = new NioEventLoopGroup();
        this.worker = new NioEventLoopGroup();
        Connection conn = DBUtil.Instance().getConnection();
        if(conn != null)
            log.info("mysql connected.");
        ServerBootstrap server = new ServerBootstrap();

        try {
            server.group(boss, worker)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>()
                    {
                        @Override
                        protected void initChannel(SocketChannel channel) throws Exception
                        {
                            channel.pipeline().addLast(
                                    new ObjectEncoder(),
                                    // thread-safe class loader cache
                                    new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.weakCachingConcurrentResolver(this.getClass().getClassLoader())),
                                    new MetadataServerHandler());
//                            channel.pipeline().addLast("decoder", new KryoDecoder());
//                            channel.pipeline().addLast("encoder", new KryoEncoder());
//                            channel.pipeline().addLast(new MetadataServerHandler());
                        }
                    });

            // bind port, synchronously
            this.running = true;
            ChannelFuture future = server.bind(port).sync();

            // waiting for closing the server-end port.
            future.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            log.error("error while binding port in metadata server.", e);
        } finally {
            // close thread group.
            this.running = false;
            boss.shutdownGracefully();
            worker.shutdownGracefully();
            DBUtil.Instance().close();
        }
    }
}
