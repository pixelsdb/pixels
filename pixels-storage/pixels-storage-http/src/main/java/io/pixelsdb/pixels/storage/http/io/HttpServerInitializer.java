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
package io.pixelsdb.pixels.storage.http.io;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;
import io.netty.handler.ssl.SslContext;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

/**
 * @author jasha64
 * @create 2023-07-27
 */
public class HttpServerInitializer extends ChannelInitializer<SocketChannel>
{
    private final SslContext sslCtx;
    private final ChannelHandler handler;

    public HttpServerInitializer(SslContext sslCtx, ChannelHandler handler)
    {
        this.sslCtx = sslCtx;
        this.handler = handler;
    }

    @Override
    public void initChannel(SocketChannel ch)
    {
        ChannelPipeline p = ch.pipeline();
        if (sslCtx != null)
        {
            p.addLast(sslCtx.newHandler(ch.alloc()));
        }
        p.addLast(new HttpServerCodec());
        p.addLast(new HttpObjectAggregator(2 * Integer.parseInt(
                ConfigFactory.Instance().getProperty("row.group.size"))));
        // Currently the pipelining component sends intermediate tables in row groups, so we must have the HTTP server
        //  allow for a content length equal to the row group size specified in pixels.properties.
        // XXX: the row group size may be up to 2GB, which might not be a good value as the max content length here
        //  in a production environment.
        p.addLast(new HttpContentCompressor());
        p.addLast(new HttpServerExpectContinueHandler());
        p.addLast(this.handler);
    }
}
