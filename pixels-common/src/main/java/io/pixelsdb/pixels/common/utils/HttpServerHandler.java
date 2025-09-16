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
package io.pixelsdb.pixels.common.utils;

import io.netty.channel.*;
import io.netty.handler.codec.http.*;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;

/**
 * @author jasha64
 * @create 2023-07-27
 */
@ChannelHandler.Sharable
public abstract class HttpServerHandler extends SimpleChannelInboundHandler<HttpObject>
{
    protected Runnable serverCloser;

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx)
    {
        ctx.flush();
    }

    // By default, response with 500 Internal Server Error and close the connection on exception.
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, INTERNAL_SERVER_ERROR);
        response.headers()
                .set(HttpHeaderNames.CONTENT_TYPE, "text/plain")
                .set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());

        ChannelFuture f = ctx.writeAndFlush(response);
        f.addListener(ChannelFutureListener.CLOSE);
        ctx.close();
    }

    public void setServerCloser(Runnable serverCloser) {
        this.serverCloser = serverCloser;
    }
}
