package cn.edu.ruc.iir.pixels.daemon.metadata;

import cn.edu.ruc.iir.pixels.common.metadata.*;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Column;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Layout;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Schema;
import cn.edu.ruc.iir.pixels.common.metadata.domain.Table;
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
import io.netty.util.ReferenceCountUtil;

import java.util.List;

/**
 * instance of this class should not be reused.
 */
public class MetadataServerHandler extends ChannelInboundHandlerAdapter
{
    private boolean complete = false;
    private StringBuilder builder = new StringBuilder();

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
        //将发送缓冲区中数据全部写入SocketChannel
        ctx.flush();
        if (this.complete == false)
        {
            ReqParams params = ReqParams.parse(builder.toString());

            // log the received params.
            LogFactory.Instance().getLog().info(builder.toString());

            String res = this.executeRequest(params);
            //response
            //异步发送应答消息给客户端: 这里并没有把消息直接写入SocketChannel,而是放入发送缓冲数组中
            ChannelFuture future = ctx.writeAndFlush(Unpooled.copiedBuffer(res.getBytes()));
            // Thread close
            future.addListener(
                    (ChannelFutureListener) channelFuture -> ctx.close()
            );
        }
        this.complete = true;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e)
    {
        LogFactory.Instance().getLog().error("error caught in metadata server.", e);
        //释放资源
        ctx.close();
    }

    private String executeRequest (ReqParams params)
    {
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
            case "createSchema":
            {
                Schema schema = new Schema();
                schema.setName(params.getParam("schemaName"));
                schema.setDesc("This schema is created by pixels-daemon");
                if (schemaDao.exists(schema))
                {
                    res = "exists";
                }
                else
                {
                    schemaDao.insert(schema);
                    res = "success";
                }
                break;
            }
            case "dropSchema":
            {
                if (schemaDao.deleteByName(params.getParam("schemaName")))
                {
                    res = "success";
                }
                else
                {
                    res = "no-such";
                }
                break;
            }
            case "createTable":
            {
                Schema schema = schemaDao.getByName(params.getParam("schemaName"));
                Table table = new Table();
                table.setName(params.getParam("tableName"));
                table.setSchema(schema);
                table.setType("user");
                if (tableDao.exists(table))
                {
                    res = "exists";
                }
                else
                {
                    tableDao.insert(table);
                    String columnsJson = params.getParam("columns");
                    List<Column> columns = JSON.parseArray(columnsJson, Column.class);
                    table = tableDao.getByNameAndSchema(table.getName(), schema);
                    if (columns.size() == columnDao.insertBatch(table, columns))
                    {
                        res = "success";
                    }
                    else
                    {
                        tableDao.deleteByNameAndSchema(table.getName(), schema);
                        res = "failed";
                    }
                }
                break;
            }
            case "dropTable":
            {
                Schema schema = schemaDao.getByName(params.getParam("schemaName"));
                if (tableDao.deleteByNameAndSchema(params.getParam("tableName"), schema))
                {
                    res = "success";
                }
                else
                {
                    res = "no-such";
                }
                break;
            }
            case "existTable":
            {
                Schema schema = schemaDao.getByName(params.getParam("schemaName"));
                Table table = new Table();
                table.setId(-1);
                table.setName(params.getParam("tableName"));
                table.setSchema(schema);
                if (tableDao.exists(table))
                {
                    res = "true";
                }
                else
                {
                    res = "false";
                }
                break;
            }
            default:
            {
                res = "default";
                break;
            }
        }

        return res;
    }
}
