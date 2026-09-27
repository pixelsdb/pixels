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
package io.pixelsdb.pixels.common.ingest;

import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * Immutable opaque batch. Row count and schema are validated again by Prepare;
 * persisting these bytes does not validate rows or make them query-visible.
 */
public final class MutationBatch
{
    public static final int PROTOCOL_VERSION = 2;
    public static final int DIGEST_BYTES = 32;
    private static final int DIGEST_HEADER_BYTES =
            Integer.BYTES
                    + Long.BYTES
                    + Long.BYTES
                    + Long.BYTES
                    + Long.BYTES
                    + Integer.BYTES
                    + Integer.BYTES
                    + Long.BYTES
                    + Long.BYTES
                    + Integer.BYTES
                    + Integer.BYTES
                    + Integer.BYTES;

    private final MutationStreamId streamId;
    private final long sequence;
    private final long schemaVersion;
    private final int payloadFormat;
    private final int rowCount;
    private final ByteString payload;
    private final byte[] digest;

    public MutationBatch(MutationStreamId streamId, long sequence, long schemaVersion,
                         int payloadFormat, int rowCount, byte[] payload)
    {
        this(streamId, sequence, schemaVersion, payloadFormat, rowCount,
                ByteString.copyFrom(Objects.requireNonNull(payload, "payload")));
    }

    /** Retains immutable RPC payload bytes without an intermediate mutable array. */
    public MutationBatch(MutationStreamId streamId, long sequence, long schemaVersion,
                         int payloadFormat, int rowCount, ByteString payload)
    {
        this.streamId = Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(payload, "payload");
        if (sequence < 0 || schemaVersion < 0 || payloadFormat <= 0 || rowCount <= 0
                || payload.isEmpty())
        {
            throw new IllegalArgumentException("Invalid mutation batch metadata or empty payload");
        }
        this.sequence = sequence;
        this.schemaVersion = schemaVersion;
        this.payloadFormat = payloadFormat;
        this.rowCount = rowCount;
        this.payload = payload;

        // Fixed-width big-endian encoding; bind identity, metadata, and payload.
        ByteBuffer header = ByteBuffer.allocate(DIGEST_HEADER_BYTES);
        header.putInt(PROTOCOL_VERSION)
                .putLong(streamId.getTransactionId())
                .putLong(streamId.getStatementId())
                .putLong(streamId.getWriterId())
                .putLong(streamId.getTableId()).putInt(streamId.getShardId())
                .putInt(streamId.getKind().getCode()).putLong(sequence).putLong(schemaVersion)
                .putInt(payloadFormat).putInt(rowCount).putInt(payload.size());
        MessageDigest sha = sha256();
        sha.update(header.array());
        sha.update(this.payload.asReadOnlyByteBuffer());
        this.digest = sha.digest();
    }

    public MutationStreamId getStreamId() { return streamId; }
    public long getSequence() { return sequence; }
    public long getSchemaVersion() { return schemaVersion; }
    public int getPayloadFormat() { return payloadFormat; }
    public int getRowCount() { return rowCount; }
    public int getPayloadBytes() { return payload.size(); }
    public byte[] getPayload() { return payload.toByteArray(); }
    public ByteString getPayloadByteString() { return payload; }
    public byte[] getDigest() { return digest.clone(); }

    static MessageDigest sha256()
    {
        try
        {
            return MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new IllegalStateException("SHA-256 is required by the Java runtime", e);
        }
    }
}
