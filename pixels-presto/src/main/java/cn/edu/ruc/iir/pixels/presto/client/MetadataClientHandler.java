package cn.edu.ruc.iir.pixels.presto.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.presto.client
 * @ClassName: MetadataClientHandler
 * @Description:
 * @author: taoyouxian
 * @date: Create in 2018-01-26 15:13
 **/
public class MetadataClientHandler extends ChannelInboundHandlerAdapter {
    private final ByteBuf firstMSG;

    private ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<String>();

    public MetadataClientHandler(String action, ConcurrentLinkedQueue queue, String paras) {
        this.queue = queue;
        String param = action + "==" + (paras != null ? paras : "");
        byte[] req = param.getBytes();
        firstMSG = Unpooled.buffer(req.length);
        firstMSG.writeBytes(req);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
//        System.out.println("client -> action -> active");
        ctx.writeAndFlush(firstMSG);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//        System.out.println("client -> action -> read");
        ByteBuf buf = (ByteBuf) msg;
        byte[] req = new byte[buf.readableBytes()];
        buf.readBytes(req);
        String body = new String(req, "UTF-8");
//        System.out.println("NOW is: " + body);
        queue.add(body);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.close();
    }
}
