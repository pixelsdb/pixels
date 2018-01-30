package cn.edu.ruc.iir.pixels.daemon.metadata;

import cn.edu.ruc.iir.pixels.daemon.metadata.dao.BaseDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.CatalogDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.SchemaDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.TableDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Catalog;
import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Schema;
import cn.edu.ruc.iir.pixels.daemon.metadata.domain.Table;
import com.alibaba.fastjson.JSON;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.List;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.daemon.metadata.server
 * @ClassName: MetadataServerHandler
 * @Description:
 * @author: taoyouxian
 * @date: Create in 2018-01-26 15:11
 **/
public class MetadataServerHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//        System.out.println("Server -> read");
        ByteBuf buf = (ByteBuf) msg;
        byte[] req = new byte[buf.readableBytes()];
        buf.readBytes(req);
        String body = new String(req, "UTF-8").trim();
        String curResponse = null;
        String sql = null;
        String split[] = body.split("==");
        String action = split[0];
        String params[] = new String[]{};
        System.out.println("Action: " + action);
        if (split.length > 1)
            params = split[1].split("&");
        if (action.equals("getSchemas")) {
            BaseDao baseDao = new SchemaDao();
            sql = "select * from DBS";
            List<Schema> schemaList = baseDao.loadAll(sql, params);
            curResponse = JSON.toJSONString(schemaList);
        } else if (action.equals("getTables")) {
            BaseDao baseDao = new TableDao();
            sql = "select * from TBLS " + (params.length > 0 ? "where DBS_DB_ID in (select DB_ID from DBS where DB_NAME = ? )" : "");
            List<Table> tableList = baseDao.loadAll(sql, params);
            curResponse = JSON.toJSONString(tableList);
        } else if (action.equals("getCatalogs")) {
            BaseDao baseDao = new CatalogDao();
            sql = "select * from CATALOG " + (params.length > 0 ? "where LAYOUTS_LAYOUT_ID in (select LAYOUT_ID from LAYOUTS where TBLS_TBL_ID in (select TBL_ID from TBLS where TBL_NAME = ? ) )" : "");
            List<Catalog> catalogList = baseDao.loadAll(sql, params);
            curResponse = JSON.toJSONString(catalogList);
        } else {
            curResponse = "action default";
        }

        //response
        ByteBuf resp = Unpooled.copiedBuffer(curResponse.getBytes());
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
