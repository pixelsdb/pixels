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
import io.pixelsdb.pixels.common.ingest.wire.IngestWire;
import io.pixelsdb.pixels.ingest.IngestProto.AppendRequest;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ReadOnlyBufferException;
import java.security.MessageDigest;

import static org.junit.jupiter.api.Assertions.*;

class TestMutationBatch {
    private static final MutationStreamId STREAM = new MutationStreamId(
            11, 2, 3, 17, 1, MutationStreamId.Kind.APPEND_ROWS);
    private static final long SEQUENCE = 0;
    private static final long SCHEMA = 7;
    private static final int FORMAT = 1;
    private static final int ROWS = 2;

    private MutationBatch batch(byte[] payload) {
        return new MutationBatch(STREAM, SEQUENCE, SCHEMA, FORMAT, ROWS, payload);
    }

    @Test
    void mutableArraysRemainIsolated() throws Exception {
        byte[] input = {1, 2, 3};
        MutationBatch batch = batch(input);
        byte[] digest = batch.getDigest();
        input[0] = 9;
        byte[] copy = batch.getPayload();
        copy[1] = 9;
        assertArrayEquals(new byte[] {1, 2, 3}, batch.getPayload());
        assertArrayEquals(digest, batch.getDigest());
        assertThrows(ReadOnlyBufferException.class,
                () -> batch.getPayloadByteString().asReadOnlyByteBuffer().put(0, (byte) 9));
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        batch.getPayloadByteString().writeTo(output);
        assertArrayEquals(batch.getPayload(), output.toByteArray());
    }

    @Test
    void rpcRetainsImmutablePayloadAndStillRejectsDigestMismatch() throws Exception {
        MutationBatch batch = batch(new byte[] {1, 2, 3});
        AppendRequest request = IngestWire.encode(batch);
        assertSame(batch.getPayloadByteString(), request.getPayload());
        MutationBatch decoded = IngestWire.decode(request);
        assertSame(request.getPayload(), decoded.getPayloadByteString());
        assertArrayEquals(batch.getDigest(), decoded.getDigest());
        assertThrows(IOException.class, () -> IngestWire.decode(request.toBuilder()
                .setPayload(ByteString.copyFrom(new byte[] {3, 2, 1})).build()));
    }

    @Test
    void digestBindsTheSameWireIdentityAndPayload() throws Exception {
        byte[] payload = new byte[4096];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) i;
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeInt(MutationBatch.PROTOCOL_VERSION);
        out.writeLong(STREAM.getTransactionId());
        out.writeLong(STREAM.getStatementId());
        out.writeLong(STREAM.getWriterId());
        out.writeLong(STREAM.getTableId());
        out.writeInt(STREAM.getShardId());
        out.writeInt(STREAM.getKind().getCode());
        out.writeLong(SEQUENCE);
        out.writeLong(SCHEMA);
        out.writeInt(FORMAT);
        out.writeInt(ROWS);
        out.writeInt(payload.length);
        out.write(payload);
        byte[] expected = MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray());
        assertArrayEquals(expected, batch(payload).getDigest());
        int split = payload.length / 2;
        ByteString segmented = ByteString.copyFrom(payload, 0, split)
                .concat(ByteString.copyFrom(payload, split, payload.length - split));
        assertArrayEquals(expected, new MutationBatch(STREAM, SEQUENCE, SCHEMA, FORMAT,
                ROWS, segmented).getDigest());
    }
}
