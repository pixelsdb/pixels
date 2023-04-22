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
package io.pixelsdb.pixels.example.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.compression.CompressionOptions;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;
import io.netty.handler.ssl.SslContext;

/**
 * @create 2023-03-13
 * @author hank
 */
public class RestServerInitializer extends ChannelInitializer<SocketChannel>
{
    private final SslContext sslContext;

    public RestServerInitializer(SslContext sslContext)
    {
        this.sslContext = sslContext;
    }

    @Override
    protected void initChannel(SocketChannel ch)
    {
        ChannelPipeline p = ch.pipeline();
        if (sslContext != null)
        {
            p.addLast(sslContext.newHandler(ch.alloc()));
        }
        p.addLast("codec", new HttpServerCodec());
        p.addLast("aggregator", new HttpObjectAggregator(Short.MAX_VALUE));
        p.addLast("compressor", new HttpContentCompressor((CompressionOptions[]) null));
        p.addLast("exception-handler", new HttpServerExpectContinueHandler());
        p.addLast("request-handler", new RestServerHandler());
    }
}
