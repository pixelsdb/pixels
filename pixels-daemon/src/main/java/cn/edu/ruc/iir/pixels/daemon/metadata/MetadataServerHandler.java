package cn.edu.ruc.iir.pixels.daemon.metadata;

import cn.edu.ruc.iir.pixels.common.metadata.*;
import cn.edu.ruc.iir.pixels.common.utils.LogFactory;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.ColumnDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.LayoutDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.SchemaDao;
import cn.edu.ruc.iir.pixels.daemon.metadata.dao.TableDao;
import com.alibaba.fastjson.JSON;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.List;

/**
 * Created by hank on 18-6-17.
 */
public class MetadataServerHandler extends ChannelInboundHandlerAdapter
{
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
    {
        ByteBuf buf = (ByteBuf) msg;
        byte[] req = new byte[buf.readableBytes()];
        buf.readBytes(req);
        String body = new String(req, "UTF-8").trim();
        ReqParams params = ReqParams.parse(body);

        String res;

        SchemaDao schemaDao = new SchemaDao();
        TableDao tableDao = new TableDao();
        LayoutDao layoutDao = new LayoutDao();
        ColumnDao columnDao = new ColumnDao();

        switch (params.getAction())
        {
            case "getSchemas":
            {
                List<Schema> schemaList = schemaDao.getAll();
                res = JSON.toJSONString(schemaList);
                break;
            }
            case "getTables":
            {
                Schema schema = schemaDao.getByName(params.getParam("schemaName"));
                List<Table> tableList = tableDao.getBySchema(schema);
                res = JSON.toJSONString(tableList);
                break;
            }
            case "getLayouts":
            {
                Schema schema = schemaDao.getByName(params.getParam("schemaName"));
                Table table = tableDao.getByNameAndSchema(params.getParam("tableName"), schema);
                List<Layout> layoutList = layoutDao.getReadableByTable(table);
                res = JSON.toJSONString(layoutList);
                break;
            }
            case "getColumns":
            {
                Schema schema = schemaDao.getByName(params.getParam("schemaName"));
                Table table = tableDao.getByNameAndSchema(params.getParam("tableName"), schema);
                List<Column> columnList = columnDao.getByTable(table);
                res = JSON.toJSONString(columnList);
                break;
            }
            default:
            {
                res = "action default";
                break;
            }
        }

        //response
        //异步发送应答消息给客户端: 这里并没有把消息直接写入SocketChannel,而是放入发送缓冲数组中
        ChannelFuture future = ctx.writeAndFlush(Unpooled.copiedBuffer(res.getBytes()));
        // Thread close
        future.addListener(
                (ChannelFutureListener) channelFuture -> ctx.close()
        );
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception
    {
        //将发送缓冲区中数据全部写入SocketChannel
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e)
    {
        LogFactory.Instance().getLog().error("error caught in metadata server.", e);
        //释放资源
        ctx.close();
    }
}
