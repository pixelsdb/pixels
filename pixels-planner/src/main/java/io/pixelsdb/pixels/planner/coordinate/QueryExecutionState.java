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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.planner.coordinate;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Query admission and first failure share this monitor. Only short control-plane
 * operations may hold it; invocation, storage I/O and blocking waits must not.
 * Client invocation failures are not evidence that a remote handler has stopped.
 */
public final class QueryExecutionState
{
    public enum Status { RUNNING, SUCCEEDED, ABORTED }
    public enum InvocationStatus { RESERVED, REGISTERED, UNKNOWN, STOPPED }

    private Status status = Status.RUNNING;
    private Throwable firstFailure;
    private final CompletableFuture<Void> failureFuture = new CompletableFuture<>();
    private final CompletableFuture<Void> quiescenceFuture = new CompletableFuture<>();
    private final Map<String, Invocation> invocations = new LinkedHashMap<>();
    private volatile Runnable invocationRecorder = () -> {};

    public void setInvocationRecorder(Runnable recorder)
    {
        this.invocationRecorder = java.util.Objects.requireNonNull(recorder, "recorder is null");
    }

    public boolean abort(Throwable cause)
    {
        Throwable failure = unwrap(cause);
        synchronized (this)
        {
            if (status != Status.RUNNING)
            {
                return false;
            }
            firstFailure = failure;
            status = Status.ABORTED;
        }
        // Completing futures can call arbitrary listeners. Never do this while
        // holding the admission monitor (or a stage/lifecycle lock).
        failureFuture.completeExceptionally(failure);
        signalQuiescence();
        return true;
    }

    public boolean succeed()
    {
        synchronized (this)
        {
            if (status != Status.RUNNING)
            {
                return false;
            }
            status = Status.SUCCEEDED;
        }
        signalQuiescence();
        return true;
    }

    public synchronized void requireRunning()
    {
        if (status != Status.RUNNING)
        {
            CancellationException failure = new CancellationException("query is " + status);
            if (firstFailure != null)
            {
                failure.initCause(firstFailure);
            }
            throw failure;
        }
    }

    public String reserveInvocation(int stageId)
    {
        String id = UUID.randomUUID().toString();
        synchronized (this)
        {
            requireRunning();
            invocations.put(id, new Invocation(stageId));
        }
        try
        {
            // Persist the reservation before allowing any remote launch. The
            // recorder must run outside admission so abort cannot wait on disk.
            invocationRecorder.run();
        }
        catch (RuntimeException | Error failure)
        {
            invocationReturned(id, true); // No launch has been attempted.
            abort(failure);
            throw failure;
        }
        return id;
    }

    /** Caller holds the same monitor while adding the worker to its stage. */
    public synchronized boolean registerInvocation(String id, int stageId, long workerId)
    {
        Invocation invocation = invocations.get(id);
        if (status != Status.RUNNING || invocation == null || invocation.stageId != stageId ||
                invocation.status != InvocationStatus.RESERVED)
        {
            return false;
        }
        invocation.workerId = workerId;
        invocation.status = InvocationStatus.REGISTERED;
        return true;
    }

    public void invocationReturned(String id, boolean successful)
    {
        synchronized (this)
        {
            Invocation invocation = invocations.get(id);
            if (invocation != null && invocation.status != InvocationStatus.STOPPED)
            {
                invocation.status = successful ? InvocationStatus.STOPPED : InvocationStatus.UNKNOWN;
            }
        }
        signalQuiescence();
    }

    /** A matching, cooperative worker confirms that all its owned work exited. */
    public void workerStopped(String id, long workerId)
    {
        synchronized (this)
        {
            Invocation invocation = invocations.get(id);
            if (invocation != null && invocation.workerId == workerId)
            {
                invocation.status = InvocationStatus.STOPPED;
            }
        }
        signalQuiescence();
    }

    public synchronized Status getStatus() { return status; }
    public synchronized Throwable getFirstFailure() { return firstFailure; }
    public CompletableFuture<Void> getFailureFuture() { return failureFuture; }
    public CompletableFuture<Void> getQuiescenceFuture() { return quiescenceFuture; }

    public synchronized boolean isQuiescent()
    {
        return invocations.values().stream().allMatch(invocation -> invocation.status == InvocationStatus.STOPPED);
    }

    public synchronized Map<String, String> snapshotInvocations()
    {
        Map<String, String> snapshot = new LinkedHashMap<>();
        invocations.forEach((id, invocation) -> snapshot.put(id,
                "stage=" + invocation.stageId + ",worker=" + invocation.workerId + ",state=" + invocation.status));
        return snapshot;
    }

    private void signalQuiescence()
    {
        boolean done;
        synchronized (this)
        {
            done = status != Status.RUNNING && isQuiescent();
        }
        if (done)
        {
            quiescenceFuture.complete(null);
        }
    }

    public static Throwable unwrap(Throwable failure)
    {
        if (failure == null)
        {
            return new IllegalStateException("query failed without a cause");
        }
        while ((failure instanceof CompletionException || failure instanceof ExecutionException) &&
                failure.getCause() != null)
        {
            failure = failure.getCause();
        }
        return failure;
    }

    private static final class Invocation
    {
        private final int stageId;
        private long workerId = -1;
        private InvocationStatus status = InvocationStatus.RESERVED;
        private Invocation(int stageId) { this.stageId = stageId; }
    }
}
