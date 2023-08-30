package io.pixelsdb.pixels.common.utils;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;
import io.netty.handler.ssl.SslContext;

public class HttpServerInitializer extends ChannelInitializer<SocketChannel> {

    private final SslContext sslCtx;
    private final HttpServerHandler handler;

    public HttpServerInitializer(SslContext sslCtx) {
        this.sslCtx = sslCtx;
        this.handler = new HttpServerHandler();
    }
    public HttpServerInitializer(SslContext sslCtx, HttpServerHandler handler) {
        this.sslCtx = sslCtx;
        this.handler = handler;
    }

    @Override
    public void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        if (sslCtx != null) {
            p.addLast(sslCtx.newHandler(ch.alloc()));
        }
        p.addLast(new HttpServerCodec());
        p.addLast(new HttpContentCompressor());
        p.addLast(new HttpServerExpectContinueHandler());
        p.addLast(this.handler);
    }
}
