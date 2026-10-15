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

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Objects;

/** Exact completion boundary for one stream, never for an entire transaction. */
public final class MutationStreamSeal
{
    private final MutationStreamId streamId;
    private final long batchCount;
    private final long rowCount;
    private final long payloadBytes;
    private final byte[] digest;

    public MutationStreamSeal(MutationStreamId streamId, long batchCount, long rowCount,
                              long payloadBytes, byte[] digest)
    {
        this.streamId = Objects.requireNonNull(streamId, "streamId");
        Objects.requireNonNull(digest, "digest");
        if (batchCount <= 0 || rowCount < batchCount || payloadBytes < batchCount
                || digest.length != MutationBatch.DIGEST_BYTES)
        {
            throw new IllegalArgumentException("Invalid stream seal");
        }
        this.batchCount = batchCount;
        this.rowCount = rowCount;
        this.payloadBytes = payloadBytes;
        this.digest = digest.clone();
    }

    public static byte[] emptyDigest()
    {
        return MutationBatch.sha256().digest(new byte[0]);
    }

    public static byte[] extendDigest(byte[] previous, byte[] batchDigest)
    {
        if (previous.length != MutationBatch.DIGEST_BYTES || batchDigest.length != MutationBatch.DIGEST_BYTES)
        {
            throw new IllegalArgumentException("Expected SHA-256 digests");
        }
        MessageDigest sha = MutationBatch.sha256();
        sha.update(previous);
        return sha.digest(batchDigest);
    }

    public MutationStreamId getStreamId() { return streamId; }
    public long getBatchCount() { return batchCount; }
    public long getRowCount() { return rowCount; }
    public long getPayloadBytes() { return payloadBytes; }
    public byte[] getDigest() { return digest.clone(); }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) { return true; }
        if (!(other instanceof MutationStreamSeal)) { return false; }
        MutationStreamSeal that = (MutationStreamSeal) other;
        return streamId.equals(that.streamId) && batchCount == that.batchCount
                && rowCount == that.rowCount && payloadBytes == that.payloadBytes
                && Arrays.equals(digest, that.digest);
    }

    @Override
    public int hashCode()
    {
        return 31 * Objects.hash(streamId, batchCount, rowCount, payloadBytes) + Arrays.hashCode(digest);
    }
}
