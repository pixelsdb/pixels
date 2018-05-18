package cn.edu.ruc.iir.pixels.daemon.metadata;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.metadata.server
 * @ClassName: ChildChannelInitializer
 * @Description:
 * @author: taoyouxian
 * @date: Create in 2018-01-26 15:09
 **/
public class ChildChannelInitializer extends ChannelInitializer<SocketChannel> {
    @Override
    protected void initChannel(SocketChannel channel) throws Exception {
        channel.pipeline().addLast("MetadataServerHandler", new MetadataServerHandler());
    }
}
