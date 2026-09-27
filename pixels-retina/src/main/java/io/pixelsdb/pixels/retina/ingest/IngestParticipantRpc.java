/*
 * Copyright 2026 PixelsDB.
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels. If not, see <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.retina.ingest;

import com.google.protobuf.Empty;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.ingest.wire.IngestWire;
import io.pixelsdb.pixels.ingest.*;
import io.pixelsdb.pixels.ingest.IngestProto.*;

/** Typed transport over the durable participant. */
public final class IngestParticipantRpc
        extends IngestParticipantServiceGrpc.IngestParticipantServiceImplBase {
    private final RetinaIngestParticipant target;

    public IngestParticipantRpc(RetinaIngestParticipant target) {
        this.target = target;
    }

    @Override
    public void append(AppendRequest r, StreamObserver<Empty> o) {
        try {
            target.append(IngestWire.decode(r));
            o.onNext(Empty.getDefaultInstance());
            o.onCompleted();
        } catch (Exception e) {
            o.onError(
                    Status.FAILED_PRECONDITION
                            .withDescription(e.getMessage())
                            .withCause(e)
                            .asRuntimeException());
        }
    }

    @Override
    public void seal(StreamSeal r, StreamObserver<StreamSeal> o) {
        try {
            o.onNext(IngestWire.encode(target.seal(IngestWire.decode(r))));
            o.onCompleted();
        } catch (Exception e) {
            o.onError(
                    Status.FAILED_PRECONDITION
                            .withDescription(e.getMessage())
                            .withCause(e)
                            .asRuntimeException());
        }
    }

    @Override
    public void prepare(ParticipantRequest r, StreamObserver<PrepareToken> o) {
        try {
            o.onNext(target.prepare(r.getTransaction()));
            o.onCompleted();
        } catch (Exception e) {
            o.onError(
                    Status.FAILED_PRECONDITION
                            .withDescription(e.getMessage())
                            .withCause(e)
                            .asRuntimeException());
        }
    }

    @Override
    public void install(ParticipantRequest r, StreamObserver<InstallResult> o) {
        try {
            o.onNext(InstallResult.newBuilder()
                    .setReady(target.install(
                            r.getTransaction(), r.getForceFileTail()))
                    .build());
            o.onCompleted();
        } catch (Exception e) {
            o.onError(
                    Status.FAILED_PRECONDITION
                            .withDescription(e.getMessage())
                            .withCause(e)
                            .asRuntimeException());
        }
    }

    @Override
    public void checkpoint(TransactionId r, StreamObserver<Empty> o) {
        try {
            target.checkpoint(r.getTransactionId());
            o.onNext(Empty.getDefaultInstance());
            o.onCompleted();
        } catch (Exception e) {
            o.onError(
                    Status.FAILED_PRECONDITION
                            .withDescription(e.getMessage())
                            .withCause(e)
                            .asRuntimeException());
        }
    }

    @Override
    public void discard(TransactionId r, StreamObserver<Empty> o) {
        try {
            target.discard(r.getTransactionId());
            o.onNext(Empty.getDefaultInstance());
            o.onCompleted();
        } catch (Exception e) {
            o.onError(
                    Status.FAILED_PRECONDITION
                            .withDescription(e.getMessage())
                            .withCause(e)
                            .asRuntimeException());
        }
    }

    @Override
    public void pinRead(ReadPin r, StreamObserver<ReadPin> o) {
        try {
            o.onNext(target.pinRead(r));
            o.onCompleted();
        } catch (Exception e) {
            o.onError(
                    Status.FAILED_PRECONDITION
                            .withDescription(e.getMessage())
                            .withCause(e)
                            .asRuntimeException());
        }
    }

    @Override
    public void renewRead(ReadPin r, StreamObserver<ReadPin> o) {
        try {
            o.onNext(target.readPins().renew(r));
            o.onCompleted();
        } catch (Exception e) {
            o.onError(
                    Status.FAILED_PRECONDITION
                            .withDescription(e.getMessage())
                            .withCause(e)
                            .asRuntimeException());
        }
    }

    @Override
    public void releaseRead(ReadPin r, StreamObserver<Empty> o) {
        try {
            target.readPins().release(r);
            o.onNext(Empty.getDefaultInstance());
            o.onCompleted();
        } catch (Exception e) {
            o.onError(
                    Status.FAILED_PRECONDITION
                            .withDescription(e.getMessage())
                            .withCause(e)
                            .asRuntimeException());
        }
    }

    @Override
    public void readPrivate(PrivateReadRequest r, StreamObserver<PrivateReadPage> o) {
        try {
            o.onNext(target.readPrivate(r));
            o.onCompleted();
        } catch (Exception e) {
            o.onError(
                    Status.FAILED_PRECONDITION
                            .withDescription(e.getMessage())
                            .withCause(e)
                            .asRuntimeException());
        }
    }
}
