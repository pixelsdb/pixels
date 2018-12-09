package cn.edu.ruc.iir.pixels.common.metadata;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by hank on 18-6-17.
 */
public class MetadataClient
{

    private final ReqParams params;
    private final String token;
    // getResponse and setResponse are synchronized to ensure atomic read/write.
    private final Map<String, Object> response = new HashMap<>();

    public MetadataClient(ReqParams params, String token)
    {
        this.params = params;
        this.token = token;
    }

    public synchronized Map<String, Object> getResponse()
    {
        return response;
    }

    public synchronized void setResponse(String token, Object res)
    {
        this.response.put(token, res);
    }

    public void connect(int port, String host) throws Exception
    {
        // configure NIO thread group in client
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
                        protected void initChannel(SocketChannel channel) throws Exception
                        {
                            channel.pipeline().addLast(
                                    new ObjectEncoder(),
                                    // disable cache
                                    new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(this.getClass().getClassLoader())),
                                    new MetadataClientHandler(params, token, MetadataClient.this));
//                            channel.pipeline().addLast("decoder", new KryoDecoder());
//                            channel.pipeline().addLast("encoder", new KryoEncoder());
//                            channel.pipeline().addLast(new MetadataClientHandler(params, token, MetadataClient.this));
                        }
                    });

            // bind to the port asynchronously
            ChannelFuture future = client.connect(host, port).sync();

            // wait for closing the port.
            future.channel().closeFuture().sync();
        } finally
        {
            // close the thread group
            group.shutdownGracefully();
        }
    }
}
