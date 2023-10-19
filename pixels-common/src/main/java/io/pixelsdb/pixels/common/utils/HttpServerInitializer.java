package io.pixelsdb.pixels.common.utils;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
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
        p.addLast(new HttpObjectAggregator(2 * Integer.parseInt(ConfigFactory.Instance().getProperty("row.group.size"))));
        // Currently the pipelining component sends intermediate tables in row groups, so we must have the HTTP server
        //  allow for a content length equal to the row group size specified in pixels.properties.
        // XXX: the row group size may be up to 2GB, which might not be a good value as the max content length here
        //  in a production environment.
        p.addLast(new HttpContentCompressor());
        p.addLast(new HttpServerExpectContinueHandler());
        p.addLast(this.handler);
    }
}
