package cn.edu.ruc.iir.pixels.presto.client;

import cn.edu.ruc.iir.pixels.common.metadata.ReqParams;
import com.facebook.presto.spi.PrestoException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.Map;

import static cn.edu.ruc.iir.pixels.presto.exception.PixelsErrorCode.PIXELS_CLIENT_ERROR;

/**
 * Created by hank on 18-6-17.
 */
public class MetadataClientHandler extends ChannelInboundHandlerAdapter
{
    private final ByteBuf firstMSG;

    private boolean isRead;
    private final String token;
    private final Map<String, String> response;
    private StringBuilder builder = new StringBuilder();

    public MetadataClientHandler(ReqParams params, String token, Map<String, String> response)
    {
        this.token = token;
        this.response = response;
        byte[] req = params.toString().getBytes();
        firstMSG = Unpooled.buffer(req.length);
        firstMSG.writeBytes(req);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception
    {
        ctx.writeAndFlush(firstMSG);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
    {
        ByteBuf buf = (ByteBuf) msg;
        if (buf.readableBytes() <= 0)
        {
            isRead = true;
            return;
        }
        byte[] req = new byte[buf.readableBytes()];
        buf.readBytes(req);
        String body = new String(req, "UTF-8");
        builder.append(body);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception
    {
        ctx.flush();
        response.put(token, builder.toString());
        if (isRead)
        {
            ctx.close();
        } else
        {
            ctx.read();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
    {
        cause.printStackTrace();
        ctx.close();
        throw new PrestoException(PIXELS_CLIENT_ERROR, cause);
    }
}
