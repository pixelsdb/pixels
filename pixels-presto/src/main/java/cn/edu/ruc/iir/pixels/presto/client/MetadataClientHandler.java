package cn.edu.ruc.iir.pixels.presto.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.HashMap;
import java.util.Map;

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

    private boolean isRead;
    private String token;
    private Map<String, String> map = new HashMap<String, String>();
    private StringBuilder sb = new StringBuilder();

    public MetadataClientHandler(String action, String token, Map<String, String> map, String paras) {
        this.token = token;
        this.map = map;
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
        sb.append(body);
        isRead = true;
//        map.put(token, body);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
        map.put(token, sb.toString());
        if(isRead){
            ctx.close();
        } else {
            ctx.read();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}
