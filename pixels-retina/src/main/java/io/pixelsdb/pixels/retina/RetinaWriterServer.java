package io.pixelsdb.pixels.retina;

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.retina.RetinaWriterServiceGrpc.RetinaWriterServiceImplBase;
import io.pixelsdb.pixels.retina.RetinaWriterProto.*;

import java.io.IOException;


import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Server that manages startup/shutdown of a {@code Greeter} server.
 */
public class RetinaWriterServer {
    private static final Logger logger = Logger.getLogger(RetinaWriterServer.class.getName());

    private Server server;

    private void start() throws IOException {
        /* The port on which the server should run */
        int port = 50052;
        server = ServerBuilder.forPort(port)
                .addService(new RetinaWriterServiceImpl())
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    RetinaWriterServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        final RetinaWriterServer server = new RetinaWriterServer();
        server.start();
        server.blockUntilShutdown();
    }

    static class RetinaWriterServiceImpl extends RetinaWriterServiceImplBase {
        RetinaWriter writer = new RetinaWriter();

        @Override
        public void flush(FlushRequest request, StreamObserver<FlushResponse> responseObserver) {
            FlushResponse flushResponse = null;
            try {
                writer.readAndWrite(request.getSchemaStr(),
                        request.getPos(), request.getFilePath());
                flushResponse = FlushResponse.newBuilder().setErrorCode(0).setPos(request.getPos()).build();
            } catch (IOException e) {
                flushResponse = FlushResponse.newBuilder().setErrorCode(-1).build();
            } finally {
                responseObserver.onNext(flushResponse);
                responseObserver.onCompleted();
            }
        }
    }
}

