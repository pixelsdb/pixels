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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.pixelsdb.pixels.daemon.Server;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.SSLException;
import java.security.cert.CertificateException;

/**
 * This is the server for the REST API of Pixels.
 * @create 2023-03-13
 * @author hank
 */
public class RestServer implements Server
{
    private static final Logger log = LogManager.getLogger(RestServer.class);

    private boolean running = false;
    private boolean enableSsl = true;

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private Channel channel;

    public RestServer(int port)
    {
        this.bossGroup = new NioEventLoopGroup();
        this.workerGroup = new NioEventLoopGroup();

        try
        {
            SslContext sslContext = null;
            if (enableSsl)
            {
                SelfSignedCertificate ssc = new SelfSignedCertificate();
                sslContext = SslContextBuilder
                        .forServer(ssc.certificate(), ssc.privateKey()).build();
            }
            ServerBootstrap b = new ServerBootstrap();
            b.option(ChannelOption.SO_BACKLOG, 1024);
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new RestServerInitializer(sslContext));
            this.channel = b.bind(port).sync().channel();
            log.info("Pixels REST server is running on " + (enableSsl ? "https" : "http") +
                    "://localhost:" + port + "/");
        } catch (InterruptedException | CertificateException | SSLException e)
        {
            log.error("failed to start REST server", e);
        }
    }

    @Override
    public boolean isRunning()
    {
        return this.running;
    }

    @Override
    public void shutdown()
    {
        this.running = false;
        this.bossGroup.shutdownGracefully();
        this.workerGroup.shutdownGracefully();
    }

    @Override
    public void run()
    {
        try
        {
            if (this.channel != null)
            {
                this.channel.closeFuture().sync();
            }
        } catch (InterruptedException e)
        {
            log.error("REST server channel is terminated exceptionally", e);
        }
    }
}
