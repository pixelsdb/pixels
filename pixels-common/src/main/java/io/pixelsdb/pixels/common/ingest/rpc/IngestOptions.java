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
package io.pixelsdb.pixels.common.ingest.rpc;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.ingest.IngestProto.CommitAckMode;
import io.pixelsdb.pixels.ingest.IngestProto.WriteRepresentation;

import java.nio.file.Path;
import java.nio.file.Paths;

/** Bounded admission settings shared by the connector and ingestion endpoints. */
public final class IngestOptions {
    private static final int KIBIBYTE = 1024;
    private static final int MEBIBYTE = KIBIBYTE * KIBIBYTE;
    private static final long GIBIBYTE = (long) KIBIBYTE * MEBIBYTE;
    private static final int RETINA_TIMESTAMP_BITS = 48;
    private static final long MAX_RETINA_TIMESTAMP = 1L << RETINA_TIMESTAMP_BITS;
    private static final int DEFAULT_MAX_BATCH_ROWS = 4096;
    private static final int DEFAULT_MAX_BATCH_BYTES = 4 * MEBIBYTE;
    private static final int DEFAULT_MAX_STREAMS = 4096;
    private static final int DEFAULT_MAX_TRANSACTIONS = 10_000;
    private static final int DEFAULT_INSTALLATION_THREADS = 16;
    private static final int DEFAULT_FILE_TARGET_ROWS = 1_000_000;
    private static final int DEFAULT_FILE_PIXEL_STRIDE = 10_000;
    private static final long DEFAULT_FILE_MAX_BYTES = 512L * MEBIBYTE;
    private static final long DEFAULT_FILE_MAX_DELAY_MILLIS = 30_000L;
    private static final long DEFAULT_FILE_POLL_MILLIS = 25L;
    private static final int DEFAULT_PLAN_COMPACTION_BYTES = 16 * MEBIBYTE;
    private static final int DEFAULT_COORDINATOR_COMPACTION_BYTES = 16 * MEBIBYTE;
    private static final int DEFAULT_MAX_STATE_BYTES = 64 * MEBIBYTE;
    private static final int DEFAULT_MAX_PREPARED_ROWS = 1_000_000;
    private static final long DEFAULT_READ_LEASE_MILLIS = 120_000L;
    private static final long DEFAULT_TRANSACTION_LEASE_MILLIS = 300_000L;
    private static final long DEFAULT_INSTALL_RPC_TIMEOUT_MILLIS = 3_600_000L;
    private static final long DEFAULT_TERMINAL_RETENTION_MILLIS = 86_400_000L;
    private static final int DEFAULT_MAX_TERMINAL_TRANSACTIONS = 100_000;
    private static final int DEFAULT_WAL_SEGMENT_BYTES = 64 * MEBIBYTE;
    private static final long DEFAULT_WAL_MAX_BYTES = 4L * GIBIBYTE;
    private static final int DEFAULT_WAL_MAX_RECORDS = 10_000_000;
    private static final long DEFAULT_WAL_GROUP_COMMIT_DELAY_MICROS = 200L;
    private static final long DEFAULT_WAL_READ_CACHE_MAX_BYTES = 64L * MEBIBYTE;
    private static final long DEFAULT_COORDINATOR_GROUP_COMMIT_DELAY_MICROS = 200L;
    private static final int DEFAULT_PRIVATE_READ_MAX_BATCHES = 128;
    private static final int DEFAULT_PRIVATE_READ_MAX_BYTES = 4 * MEBIBYTE;

    public final boolean enabled = Boolean.parseBoolean(property("retina.ingest.enabled", "false"));
    public final int maxBatchRows = number("retina.ingest.max.batch.rows", DEFAULT_MAX_BATCH_ROWS);
    public final int maxBatchBytes = number("retina.ingest.max.batch.bytes", DEFAULT_MAX_BATCH_BYTES);
    public final int maxStreams = number("retina.ingest.max.streams", DEFAULT_MAX_STREAMS);
    public final int maxTransactions = number(
            "retina.ingest.max.transactions", DEFAULT_MAX_TRANSACTIONS);
    public final int installationThreads = number(
            "retina.ingest.install.threads", DEFAULT_INSTALLATION_THREADS);
    public final int fileTargetRows = number(
            "retina.ingest.file.target.rows", DEFAULT_FILE_TARGET_ROWS);
    public final int filePixelStride = number(
            "retina.ingest.file.pixel.stride", DEFAULT_FILE_PIXEL_STRIDE);
    public final long fileMaxBytes = longNumber(
            "retina.ingest.file.max.bytes", DEFAULT_FILE_MAX_BYTES);
    public final long fileMaxDelayMillis = longNumber(
            "retina.ingest.file.max.delay.ms", DEFAULT_FILE_MAX_DELAY_MILLIS);
    public final long filePollMillis = longNumber(
            "retina.ingest.file.poll.ms", DEFAULT_FILE_POLL_MILLIS);
    public final int planCompactionBytes = number(
            "retina.ingest.plan.compaction.bytes", DEFAULT_PLAN_COMPACTION_BYTES);
    public final int coordinatorCompactionBytes = number(
            "retina.ingest.coordinator.compaction.bytes",
            DEFAULT_COORDINATOR_COMPACTION_BYTES);
    public final int maxStateBytes = number("retina.ingest.max.state.bytes", DEFAULT_MAX_STATE_BYTES);
    public final int maxPreparedRows = number(
            "retina.ingest.max.prepared.rows", DEFAULT_MAX_PREPARED_ROWS);
    public final long readLeaseMillis = longNumber(
            "retina.ingest.read.lease.ms", DEFAULT_READ_LEASE_MILLIS);
    public final int privateReadMaxBatches = number(
            "retina.ingest.private.read.max.batches", DEFAULT_PRIVATE_READ_MAX_BATCHES);
    public final int privateReadMaxBytes = number(
            "retina.ingest.private.read.max.bytes", DEFAULT_PRIVATE_READ_MAX_BYTES);
    public final WriteRepresentation writeRepresentation = representation(
            property("retina.ingest.write.representation",
                    WriteRepresentation.BUFFERED.name()));
    public final CommitAckMode commitAckMode = ackMode(
            property("retina.ingest.commit.ack", CommitAckMode.VISIBLE.name()));
    public final long transactionLeaseMillis = longNumber(
            "retina.ingest.transaction.lease.ms", DEFAULT_TRANSACTION_LEASE_MILLIS);
    public final long installRpcTimeoutMillis = longNumber(
            "retina.ingest.install.rpc.timeout.ms", DEFAULT_INSTALL_RPC_TIMEOUT_MILLIS);
    public final long terminalRetentionMillis =
            longNumber("retina.ingest.terminal.retention.ms",
                    DEFAULT_TERMINAL_RETENTION_MILLIS);
    public final int maxTerminalTransactions =
            number("retina.ingest.terminal.max.transactions",
                    DEFAULT_MAX_TERMINAL_TRANSACTIONS);
    public final String coordinatorStateDirectory =
            requiredPath("retina.ingest.coordinator.state.dir");
    public final String participantPlanDirectory =
            requiredPath("retina.ingest.participant.plan.dir");
    public final String participantWalDirectory =
            requiredPath("retina.ingest.participant.wal.dir");
    public final int walSegmentBytes = number(
            "retina.ingest.wal.segment.bytes", DEFAULT_WAL_SEGMENT_BYTES);
    public final long walMaxBytes =
            longNumber("retina.ingest.wal.max.bytes", DEFAULT_WAL_MAX_BYTES);
    public final int walMaxRecords =
            number("retina.ingest.wal.max.records", DEFAULT_WAL_MAX_RECORDS);
    public final long walGroupCommitDelayMicros = nonNegativeLong(
            "retina.ingest.wal.group.commit.delay.micros",
            DEFAULT_WAL_GROUP_COMMIT_DELAY_MICROS);
    public final long walReadCacheMaxBytes = nonNegativeLong(
            "retina.ingest.wal.read.cache.max.bytes",
            DEFAULT_WAL_READ_CACHE_MAX_BYTES);
    public final long coordinatorGroupCommitDelayMicros = nonNegativeLong(
            "retina.ingest.coordinator.group.commit.delay.micros",
            DEFAULT_COORDINATOR_GROUP_COMMIT_DELAY_MICROS);
    public final long cutoverBaselineTimestamp =
            nonNegative("retina.ingest.cutover.baseline.timestamp", 0L);

    public IngestOptions() {
        if (terminalRetentionMillis < transactionLeaseMillis) {
            throw new IllegalArgumentException(
                    "retina.ingest.terminal.retention.ms must not be shorter than the transaction lease");
        }
        if (enabled) {
            validateStateDirectories();
        }
    }

    public static String property(String key, String fallback) {
        String v = ConfigFactory.Instance().getProperty(key);
        return v == null ? fallback : v;
    }

    private static int number(String key, int fallback) {
        int n = Integer.parseInt(property(key, Integer.toString(fallback)));
        if (n <= 0) throw new IllegalArgumentException(key + " must be positive");
        return n;
    }

    private static long longNumber(String key, long fallback) {
        long n = Long.parseLong(property(key, Long.toString(fallback)));
        if (n <= 0) throw new IllegalArgumentException(key + " must be positive");
        return n;
    }

    private static long nonNegative(String key, long fallback) {
        long n = Long.parseLong(property(key, Long.toString(fallback)));
        if (n < 0 || n >= MAX_RETINA_TIMESTAMP) {
            throw new IllegalArgumentException(
                    key + " must fit Retina's non-negative 48-bit timestamp domain");
        }
        return n;
    }

    private static long nonNegativeLong(String key, long fallback) {
        long value = Long.parseLong(property(key, Long.toString(fallback)));
        if (value < 0) {
            throw new IllegalArgumentException(key + " must be non-negative");
        }
        return value;
    }

    private static WriteRepresentation representation(String value) {
        try {
            WriteRepresentation representation =
                    WriteRepresentation.valueOf(value.trim().toUpperCase(java.util.Locale.ROOT));
            if (representation == WriteRepresentation.UNRECOGNIZED) {
                throw new IllegalArgumentException();
            }
            return representation;
        }
        catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException(
                    "retina.ingest.write.representation must be BUFFERED or FILE", failure);
        }
    }

    private static CommitAckMode ackMode(String value) {
        try {
            CommitAckMode ackMode =
                    CommitAckMode.valueOf(value.trim().toUpperCase(java.util.Locale.ROOT));
            if (ackMode == CommitAckMode.UNRECOGNIZED) {
                throw new IllegalArgumentException();
            }
            return ackMode;
        }
        catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException(
                    "retina.ingest.commit.ack must be VISIBLE or DURABLE", failure);
        }
    }

    private String requiredPath(String key) {
        String value = property(key, "").trim();
        if (enabled && value.isEmpty()) {
            throw new IllegalArgumentException(
                    key + " is required when transactional ingestion is enabled");
        }
        return value;
    }

    private void validateStateDirectories() {
        Path coordinator = absoluteStatePath(
                "retina.ingest.coordinator.state.dir", coordinatorStateDirectory);
        Path plans = absoluteStatePath(
                "retina.ingest.participant.plan.dir", participantPlanDirectory);
        Path wal = absoluteStatePath(
                "retina.ingest.participant.wal.dir", participantWalDirectory);
        requireDisjoint(coordinator, plans);
        requireDisjoint(coordinator, wal);
        requireDisjoint(plans, wal);
    }

    private static Path absoluteStatePath(String key, String value) {
        Path path = Paths.get(value);
        if (!path.isAbsolute()) {
            throw new IllegalArgumentException(key + " must be an absolute path");
        }
        return path.normalize();
    }

    private static void requireDisjoint(Path first, Path second) {
        if (first.equals(second) || first.startsWith(second) || second.startsWith(first)) {
            throw new IllegalArgumentException(
                    "transactional ingestion state directories must not overlap: "
                            + first + " and " + second);
        }
    }
}
