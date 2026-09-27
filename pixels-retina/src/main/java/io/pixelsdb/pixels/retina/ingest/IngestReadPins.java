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

import io.pixelsdb.pixels.ingest.IngestProto.ReadPin;

import java.io.IOException;
import java.time.Clock;
import java.util.*;

/** Leases pin a physical coverage selection while logical versions continue to arrive. */
public final class IngestReadPins {
    public interface CheckedAction {
        void run() throws Exception;
    }

    private final long leaseMillis;
    private final Clock clock;
    private final Map<String, ReadPin> pins = new HashMap<>();
    private boolean ready;

    public IngestReadPins(long leaseMillis) {
        this(leaseMillis, Clock.systemUTC());
    }

    public IngestReadPins(long leaseMillis, Clock clock) {
        if (leaseMillis <= 0) throw new IllegalArgumentException("leaseMillis");
        this.leaseMillis = leaseMillis;
        this.clock = clock;
    }

    public synchronized void ready() {
        ready = true;
    }

    /** Reject new/renewed reads and release every process-local physical coverage lease. */
    public synchronized void stop() {
        ready = false;
        pins.clear();
    }

    private void expire() {
        pins.values().removeIf(p -> p.getExpiresAtMillis() <= clock.millis());
    }

    public synchronized ReadPin pin(ReadPin request) throws IOException {
        if (!ready) throw new IOException("Read owner not ready");
        expire();
        ReadPin p =
                request.toBuilder()
                        .setToken(UUID.randomUUID().toString())
                        .setExpiresAtMillis(clock.millis() + leaseMillis)
                        .build();
        pins.put(p.getToken(), p);
        return p;
    }

    public synchronized ReadPin renew(ReadPin request) throws IOException {
        validate(request.getToken(), request.getReadTimestamp());
        ReadPin p =
                pins.get(request.getToken()).toBuilder()
                        .setExpiresAtMillis(clock.millis() + leaseMillis)
                        .build();
        pins.put(p.getToken(), p);
        return p;
    }

    public synchronized void release(ReadPin request) {
        pins.remove(request.getToken());
    }

    public synchronized void validate(String token, long timestamp) throws IOException {
        validate(token, timestamp, null);
    }

    public synchronized void validate(String token, long timestamp, Long transactionId)
            throws IOException {
        expire();
        ReadPin p = pins.get(token);
        if (!ready || p == null || p.getReadTimestamp() != timestamp
                || (transactionId != null && p.getTransactionId() != transactionId))
            throw new IOException("Expired or inconsistent read view");
    }

    public synchronized boolean publish(CheckedAction action) throws Exception {
        expire();
        if (!pins.isEmpty()) return false;
        action.run();
        return true;
    }

    public synchronized int active() {
        expire();
        return pins.size();
    }
}
