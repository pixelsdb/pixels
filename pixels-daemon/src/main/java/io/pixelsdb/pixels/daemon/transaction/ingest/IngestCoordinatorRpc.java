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
package io.pixelsdb.pixels.daemon.transaction.ingest;

import com.google.protobuf.Empty;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.ingest.*;
import io.pixelsdb.pixels.ingest.IngestProto.*;

/** Typed transport over the durable transaction state machine. */
public final class IngestCoordinatorRpc
        extends IngestCoordinatorServiceGrpc.IngestCoordinatorServiceImplBase {
    private final DurableIngestCoordinator target;

    public IngestCoordinatorRpc(DurableIngestCoordinator target) {
        this.target = target;
    }

    @Override
    public void beginWrite(BeginWriteRequest r, StreamObserver<Transaction> o) {
        try {
            o.onNext(target.begin(r));
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
    public void beginStatement(BeginStatementRequest r, StreamObserver<Transaction> o) {
        try {
            o.onNext(target.beginStatement(r));
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
    public void allocateWriter(AllocateWriterRequest r, StreamObserver<WriterAssignment> o) {
        try {
            o.onNext(target.allocateWriter(r));
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
    public void registerStream(RegisterStreamRequest r, StreamObserver<Transaction> o) {
        try {
            o.onNext(target.register(r.getStream()));
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
    public void completeStatement(CompleteStatementRequest r, StreamObserver<Transaction> o) {
        try {
            o.onNext(target.completeStatement(r));
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
    public void prepareWrite(PrepareWriteRequest r, StreamObserver<Transaction> o) {
        try {
            o.onNext(target.prepare(r));
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
    public void commitWrite(TransactionId r, StreamObserver<Transaction> o) {
        try {
            o.onNext(target.commit(r.getTransactionId()));
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
    public void abortWrite(TransactionId r, StreamObserver<Transaction> o) {
        try {
            o.onNext(target.abort(r.getTransactionId()));
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
    public void getWrite(TransactionId r, StreamObserver<Transaction> o) {
        try {
            o.onNext(target.get(r.getTransactionId()));
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
    public void touchWrite(TransactionId r, StreamObserver<Transaction> o) {
        try {
            o.onNext(target.touch(r.getTransactionId()));
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
    public void listWrites(OwnerRequest r, StreamObserver<TransactionList> o) {
        try {
            o.onNext(target.list(r.getOwner()));
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
    public void getPublication(Empty r, StreamObserver<Publication> o) {
        try {
            o.onNext(target.publication());
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
    public void awaitVisible(VisibilityRequest r, StreamObserver<Transaction> o) {
        try {
            o.onNext(target.awaitVisible(r));
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
    public void flushVisibleBarrier(
            VisibleBarrierRequest r, StreamObserver<VisibleBarrier> o) {
        try {
            o.onNext(target.flushVisibleBarrier(r));
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
