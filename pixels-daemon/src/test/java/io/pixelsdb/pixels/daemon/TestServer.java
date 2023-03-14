/*
 * Copyright 2018 PixelsDB.
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
package io.pixelsdb.pixels.daemon;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.compression.CompressionOptions;
import io.netty.handler.codec.http.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.pixelsdb.pixels.daemon.metadata.MetadataServer;
import io.pixelsdb.pixels.daemon.rest.RestServer;
import org.junit.Test;

import javax.net.ssl.SSLException;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;

import static io.netty.handler.codec.http.HttpHeaderNames.*;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpHeaderValues.TEXT_PLAIN;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

/**
 * @author: tao
 * @date: Create in 2018-01-27 10:46
 **/
public class TestServer {

    @Test
    public void testMetadataServer()
    {
        MetadataServer server = new MetadataServer(18888);
        server.run();
    }

    @Test
    public void testRestServer()
    {
        RestServer server = new RestServer(18890);
        server.run();
    }

    @Test
    public void testHttp()
    {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try
        {
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            SslContext sslContext = SslContextBuilder
                    .forServer(ssc.certificate(), ssc.privateKey()).build();

            ServerBootstrap b = new ServerBootstrap();
            b.option(ChannelOption.SO_BACKLOG, 1024);
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new HttpHelloWorldServerInitializer(sslContext));
            ChannelFuture channelFuture = b.bind(18890).sync();
            System.err.println("Open your web browser and navigate to https://127.0.0.1:18890/");
            channelFuture.channel().closeFuture().sync();
        } catch (InterruptedException | CertificateException | SSLException e)
        {
            throw new RuntimeException(e);
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public static class HttpHelloWorldServerInitializer extends ChannelInitializer<SocketChannel>
    {
        private final SslContext sslCtx;

        public HttpHelloWorldServerInitializer(SslContext sslCtx)
        {
            this.sslCtx = sslCtx;
        }

        @Override
        public void initChannel(SocketChannel ch)
        {
            ChannelPipeline p = ch.pipeline();
            if (sslCtx != null)
            {
                p.addLast(sslCtx.newHandler(ch.alloc()));
            }
            p.addLast("codec", new HttpServerCodec());
            p.addLast("aggregator", new HttpObjectAggregator(Short.MAX_VALUE));
            p.addLast("compressor", new HttpContentCompressor((CompressionOptions[]) null));
            p.addLast("exception-handler", new HttpServerExpectContinueHandler());
            p.addLast("request-handler", new HttpHelloWorldServerHandler());
        }
    }

    public static class HttpHelloWorldServerHandler extends SimpleChannelInboundHandler<FullHttpRequest>
    {
        private static final byte[] CONTENT = {'H', 'e', 'l', 'l', 'o', ' ', 'W', 'o', 'r', 'l', 'd'};

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx)
        {
            ctx.flush();
        }

        @Override
        public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request)
        {
            FullHttpResponse response = new DefaultFullHttpResponse(request.protocolVersion(),
                    OK, Unpooled.wrappedBuffer(CONTENT));

            boolean keepAlive = HttpUtil.isKeepAlive(request);

            ByteBuf buffer = request.content();
            System.out.println(request.uri());
            System.out.println(buffer.toString(StandardCharsets.UTF_8));

            response.headers().set(CONTENT_TYPE, TEXT_PLAIN)
                    .setInt(CONTENT_LENGTH, response.content().readableBytes());

            if (keepAlive)
            {
                response.headers().set(CONNECTION, KEEP_ALIVE);
            }
            else
            {
                response.headers().set(CONNECTION, CLOSE);
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
    }
}
