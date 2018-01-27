package cn.edu.ruc.iir.pixels.metadata.server;

import cn.edu.ruc.iir.pixels.metadata.dao.BaseDao;
import cn.edu.ruc.iir.pixels.metadata.dao.SchemaDao;
import cn.edu.ruc.iir.pixels.metadata.dao.TblDao;
import cn.edu.ruc.iir.pixels.metadata.domain.Schema;
import cn.edu.ruc.iir.pixels.metadata.domain.Table;
import com.alibaba.fastjson.JSON;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.metadata.server
 * @ClassName: TimeServerHandler
 * @Description:
 * @author: taoyouxian
 * @date: Create in 2018-01-26 15:11
 **/
public class TimeServerHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//        System.out.println("Server -> read");
        ByteBuf buf = (ByteBuf) msg;
        byte[] req = new byte[buf.readableBytes()];
        buf.readBytes(req);
        String body = new String(req, "UTF-8");
        String currentTime = null;
        if (body.equals("Time")) {
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            currentTime = df.format(new Date());
        } else if (body.equals("getSchemaNames")) {
            BaseDao baseDao = new SchemaDao();
            String sql = "select * from DBS";
            List<Schema> schemas = baseDao.loadT(sql, null);
            currentTime = JSON.toJSONString(schemas);
        } else if (body.equals("getTableNames")) {
            BaseDao baseDao = new TblDao();
            String sql = "select * from TBLS";
            List<Table> schemas = baseDao.loadT(sql, null);
            currentTime = JSON.toJSONString(schemas);
        } else {
            currentTime = "action default";
        }
        //response
        ByteBuf resp = Unpooled.copiedBuffer(currentTime.getBytes());
        //异步发送应答消息给客户端: 这里并没有把消息直接写入SocketChannel,而是放入发送缓冲数组中
        ChannelFuture future = ctx.writeAndFlush(resp);
        // Thread close
        future.addListener(
                (ChannelFutureListener) channelFuture -> ctx.close()
        );
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
//        System.out.println("Server -> read complete");
        //将发送缓冲区中数据全部写入SocketChannel
        //ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        //释放资源
        ctx.close();
    }
}
