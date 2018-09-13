package cn.edu.ruc.iir.pixels.common.metadata;

import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.utils.LogFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;


/**
 * instance of this class should not be reused.
 */
public class MetadataClientHandler extends ChannelInboundHandlerAdapter
{
    private boolean complete = false;
    private final ByteBuf request;
    private final String token;
    private final MetadataClient client;
    private StringBuilder builder = new StringBuilder();

    public MetadataClientHandler(ReqParams params, String token, MetadataClient client)
    {
        this.token = token;
        this.client = client;
        byte[] req = params.toString().getBytes();
        request = Unpooled.buffer(req.length);
        request.writeBytes(req);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception
    {
        ctx.writeAndFlush(request);
    }

    @SuppressWarnings("Duplicates")
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
    {
        ByteBuf buf = (ByteBuf) msg;
        try
        {
            byte[] req = new byte[buf.readableBytes()];
            buf.readBytes(req);
            String body = new String(req, "UTF-8");
            builder.append(body);
        }
        finally
        {
            ReferenceCountUtil.release(msg);
        }

    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception
    {
        ctx.flush();
        if (this.complete == false)
        {
            this.client.setResponse(token, builder.toString());
        }
        this.complete = true;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) throws MetadataException
    {
        LogFactory.Instance().getLog().error("error caught in metadata client.", e);
        ctx.close();
        throw new MetadataException("exception caught in MetadataClientHandler", e);
    }
}
