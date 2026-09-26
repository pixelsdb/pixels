/*
 * Copyright 2026 PixelsDB.
 *
 * This file is part of Pixels and is licensed under the GNU Affero General
 * Public License, version 3 or (at your option) any later version.
 */
package io.pixelsdb.pixels.common.ingest.rpc;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestIngestClient
{
    @Test
    public void testParticipantRpcWaitsForDaemonStartupWithinDeadline()
    {
        try (IngestClient client = new IngestClient(
                "127.0.0.1", 1, "012345678901234567890123", 1024, 250))
        {
            assertTrue(client.participant("127.0.0.1:2")
                    .getCallOptions().isWaitForReady());
            assertFalse(client.coordinator().getCallOptions().isWaitForReady());
            assertTrue(client.coordinator(2, TimeUnit.SECONDS)
                    .getCallOptions().getDeadline().timeRemaining(TimeUnit.MILLISECONDS) > 1_000);
            assertTrue(client.participant("127.0.0.1:2", 2, TimeUnit.SECONDS)
                    .getCallOptions().getDeadline().timeRemaining(TimeUnit.MILLISECONDS) > 1_000);
        }
    }
}
