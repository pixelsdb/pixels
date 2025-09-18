/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.storage.http.io;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import io.pixelsdb.pixels.common.utils.HttpServerHandler;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.pixelsdb.pixels.storage.http.io.HttpContentQueue.PART_ID;

/**
 * @author hank
 * @create 2025-09-18
 */
@ChannelHandler.Sharable
public class HttpServerHandlerImpl extends SimpleChannelInboundHandler<HttpObject> implements HttpServerHandler
{
    private Runnable serverCloser;
    private final HttpContentQueue contentQueue;

    public HttpServerHandlerImpl(HttpContentQueue contentQueue)
    {
        this.contentQueue = contentQueue;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx)
    {
        ctx.flush();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg)
    {
        if (!(msg instanceof HttpRequest))
        {
            return;
        }
        FullHttpRequest req = (FullHttpRequest) msg;
        if (req.method() != HttpMethod.POST)
        {
            sendResponse(ctx, req, HttpResponseStatus.OK);
            return;
        }
        if (!req.headers().get(HttpHeaderNames.CONTENT_TYPE).equals("application/x-protobuf"))
        {
            return;
        }
        String partId = req.headers().get(PART_ID);
        if (partId != null)
        {
            ByteBuf content = req.content();
            if (content.isReadable())
            {
                content.retain();
                // FIXME: exception might be caught on a previous content that is processed by this method, causing
                //  the client to retry sending the content, leading to the content being added to the queue again.
                this.contentQueue.add(new HttpContent(Integer.parseInt(partId), content));
            }
        }
        sendResponse(ctx, req, HttpResponseStatus.OK);
    }

    /**
     * Response with 500 Internal Server Error and close the connection.
     * @param ctx
     * @param cause
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, INTERNAL_SERVER_ERROR,
                cause != null ? Unpooled.copiedBuffer(cause.toString(), CharsetUtil.UTF_8) : Unpooled.buffer(0));
        response.headers()
                .set(HttpHeaderNames.CONTENT_TYPE, "text/plain")
                .set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());

        ChannelFuture f = ctx.writeAndFlush(response);
        f.addListener(ChannelFutureListener.CLOSE);
        ctx.close();
    }

    private void sendResponse(ChannelHandlerContext ctx, FullHttpRequest req, HttpResponseStatus status)
    {
        FullHttpResponse response = new DefaultFullHttpResponse(req.protocolVersion(), status);
        response.headers()
                .set(HttpHeaderNames.CONTENT_TYPE, "text/plain")
                .set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());

        if (req.headers().get(HttpHeaderNames.CONNECTION).equals(HttpHeaderValues.CLOSE.toString()))
        {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
            ctx.writeAndFlush(response);
            this.serverCloser.run();
        } else
        {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            ctx.writeAndFlush(response);
        }
    }

    @Override
    public void setServerCloser(Runnable serverCloser)
    {
        this.serverCloser = serverCloser;
    }
}
