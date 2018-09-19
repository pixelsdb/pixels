package cn.edu.ruc.iir.pixels.common.serialize;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.common.serialize
 * @ClassName: KryoEncoder
 * @Description:
 * @author: tao
 * @date: Create in 2018-09-18 19:49
 **/
public class KryoEncoder extends MessageToByteEncoder<Object> {

    // Object may not be replaced by 'ReqParams', because the response is object
    @Override
    protected void encode(ChannelHandlerContext ctx, Object message, ByteBuf out) throws Exception {
        KryoSerializer.serialize(message, out);
        ctx.flush();
    }
}
