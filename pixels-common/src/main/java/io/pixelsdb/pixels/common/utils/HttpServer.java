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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import javax.net.ssl.SSLException;
import java.security.cert.CertificateException;

/**
 * Basic HTTP server using the netty library.
 *
 * @author jasha64
 * @create 2023-07-27
 */
public final class HttpServer
{
    final HttpServerInitializer initializer;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel channel;

    public HttpServer(HttpServerHandler handler) throws CertificateException, SSLException
    {
        this.initializer = new HttpServerInitializer(HttpServerUtil.buildSslContext(), handler);
        handler.setServerCloser(this::close);
    }

    public void serve(int port) throws InterruptedException
    {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        try
        {
            ServerBootstrap b = new ServerBootstrap();
            b.option(ChannelOption.SO_BACKLOG, 1024)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(this.initializer);

            channel = b.bind(port).sync().channel();
            channel.closeFuture().sync();
        }
        finally
        {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public void close()
    {
        if (channel != null)
        {
            channel.close();
        }
        if (bossGroup != null)
        {
            bossGroup.shutdownGracefully();
        }
        if (workerGroup != null)
        {
            workerGroup.shutdownGracefully();
        }
    }
}
