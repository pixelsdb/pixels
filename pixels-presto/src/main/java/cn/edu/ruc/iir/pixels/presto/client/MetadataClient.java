package cn.edu.ruc.iir.pixels.presto.client;

import cn.edu.ruc.iir.pixels.common.metadata.ReqParams;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by hank on 18-6-17.
 */
public class MetadataClient
{

    private final ReqParams params;
    private final String token;
    private final Map<String, String> response = new HashMap<String, String>();

    public MetadataClient(ReqParams params, String token)
    {
        this.params = params;
        this.token = token;
    }

    public Map<String, String> getResponse()
    {
        return response;
    }

    public void connect(int port, String host) throws Exception
    {
        //配置客户端NIO线程组
        EventLoopGroup group = new NioEventLoopGroup(1);

        Bootstrap client = new Bootstrap();

        try
        {
            client.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>()
                    {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception
                        {
                            ch.pipeline().addLast(
                                    new MetadataClientHandler(params, token, response));
                        }
                    });

            //绑定端口, 异步连接操作
            ChannelFuture future = client.connect(host, port).sync();

            //等待客户端连接端口关闭
            future.channel().closeFuture().sync();
        } finally
        {
            //优雅关闭线程组
            group.shutdownGracefully();
        }
    }

}
