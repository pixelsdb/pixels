/*
 * Copyright 2023 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.daemon.rest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.*;
import io.pixelsdb.pixels.common.exception.InvalidArgumentException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.common.utils.QueryEngineConns;
import io.pixelsdb.pixels.daemon.rest.request.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static java.util.Objects.requireNonNull;

/**
 * The request handler for Pixels REST server.
 * Created at: 3/13/23
 * Author: hank
 */
public class RestServerHandler extends SimpleChannelInboundHandler<FullHttpRequest>
{
    private static final Logger log = LogManager.getLogger(RestServerHandler.class);
    private static final String URI_METADATA_GET_SCHEMAS = "/metadata/get_schemas";
    private static final String URI_METADATA_GET_TABLES = "/metadata/get_tables";
    private static final String URI_METADATA_GET_VIEWS = "/metadata/get_views";
    private static final String URI_METADATA_GET_COLUMNS = "/metadata/get_columns";
    private static final String URI_QUERY_OPEN_ENGINE_CONN = "/query/open_engine_conn";
    private static final String URI_QUERY_EXECUTE_QUERY = "/query/execute_query";
    private static final String URI_QUERY_CLOSE_ENGINE_CONN = "/query/close_engine_conn";

    private static final MetadataService metadataService;

    static
    {
        String host = ConfigFactory.Instance().getProperty("metadata.server.host");
        int port = Integer.parseInt(ConfigFactory.Instance().getProperty("metadata.server.port"));
        metadataService = new MetadataService(host, port);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx)
    {
        ctx.flush();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request)
    {
        FullHttpResponse response;
        try
        {
            response = new DefaultFullHttpResponse(request.protocolVersion(),
                    HttpResponseStatus.OK, processRequest(request.uri(), request.content()));
        } catch (JSONException e)
        {
            log.error("failed to process request content", e);
            response = new DefaultFullHttpResponse(request.protocolVersion(),
                    HttpResponseStatus.BAD_REQUEST, error("invalid request content"));
        } catch (InvalidArgumentException e)
        {
            log.error("failed to process request content", e);
            response = new DefaultFullHttpResponse(request.protocolVersion(),
                    HttpResponseStatus.BAD_REQUEST, error("invalid request uri"));
        } catch (MetadataException e)
        {
            log.error("failed get metadata", e);
            response = new DefaultFullHttpResponse(request.protocolVersion(),
                    HttpResponseStatus.INTERNAL_SERVER_ERROR, error("metadata error"));
        } catch (Throwable e)
        {
            log.error("failed get metadata", e);
            response = new DefaultFullHttpResponse(request.protocolVersion(),
                    HttpResponseStatus.INTERNAL_SERVER_ERROR, error("unknown server error"));
        }

        response.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                .setInt(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());

        boolean keepAlive = HttpUtil.isKeepAlive(request);
        if (keepAlive)
        {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
        }
        else
        {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        }

        ChannelFuture f = ctx.writeAndFlush(response);

        if (!keepAlive)
        {
            f.addListener(ChannelFutureListener.CLOSE);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        cause.printStackTrace();
        ctx.close();
    }

    private static final String EMPTY_CONTENT = "{}";
    private static ByteBuf error(String message)
    {
        StringBuilder builder = new StringBuilder("{error:\"").append(message).append("\"}");
        return Unpooled.wrappedBuffer(builder.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static ByteBuf processRequest(String uri, ByteBuf requestContent)
            throws InvalidArgumentException, MetadataException, SQLException
    {
        requireNonNull(uri, "uri is null");
        requireNonNull(requestContent, "requestContent is null");
        String content = requestContent.toString(StandardCharsets.UTF_8);
        String response;
        if (uri.startsWith(URI_METADATA_GET_SCHEMAS))
        {
            GetSchemas request = JSON.parseObject(content, GetSchemas.class);
            requireNonNull(request, "failed to parse request content");
            response = JSON.toJSONString(metadataService.getSchemas());
        } else if (uri.startsWith(URI_METADATA_GET_TABLES))
        {
            GetTables request = JSON.parseObject(content, GetTables.class);
            requireNonNull(request, "failed to parse request content");
            response = JSON.toJSONString(metadataService.getTables(request.getSchemaName()));
        } else if (uri.startsWith(URI_METADATA_GET_VIEWS))
        {
            GetViews request = JSON.parseObject(content, GetViews.class);
            requireNonNull(request, "failed to parse request content");
            response = JSON.toJSONString(metadataService.getViews(request.getSchemaName()));
        } else if (uri.startsWith(URI_METADATA_GET_COLUMNS))
        {
            GetColumns request = JSON.parseObject(content, GetColumns.class);
            requireNonNull(request, "failed to parse request content");
            response = JSON.toJSONString(metadataService.getColumns(
                    request.getSchemaName(), request.getTableName(), false));
        } else if (uri.startsWith(URI_QUERY_OPEN_ENGINE_CONN))
        {
            OpenEngineConn request = JSON.parseObject(content, OpenEngineConn.class);
            requireNonNull(request, "failed to parse request content");
            QueryEngineConns.Instance().openConn(request.getConnName(), request.getDriver(),
                    request.getUrl(), request.getProperties());
            response = EMPTY_CONTENT;
        } else if (uri.startsWith(URI_QUERY_EXECUTE_QUERY))
        {
            ExecuteQuery request = JSON.parseObject(content, ExecuteQuery.class);
            requireNonNull(request, "failed to parse request content");
            Connection connection = QueryEngineConns.Instance().getConnection(request.getConnName());
            Statement statement = connection.createStatement();
            statement.executeQuery(request.getSql());
            // TODO: return query result.
            response = EMPTY_CONTENT;
        } else if (uri.startsWith(URI_QUERY_CLOSE_ENGINE_CONN))
        {
            CloseEngineConn request = JSON.parseObject(content, CloseEngineConn.class);
            requireNonNull(request, "failed to parse request content");
            QueryEngineConns.Instance().closeConn(request.getConnName());
            response = EMPTY_CONTENT;
        } else
        {
            throw new InvalidArgumentException("invalid uri: " + uri);
        }
        return Unpooled.wrappedBuffer(response.getBytes(StandardCharsets.UTF_8));
    }
}
