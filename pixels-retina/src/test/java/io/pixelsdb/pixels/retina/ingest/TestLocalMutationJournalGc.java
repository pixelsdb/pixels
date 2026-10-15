/*
 * Copyright 2026 PixelsDB.
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package io.pixelsdb.pixels.retina.ingest;

import org.junit.Test;

public class TestLocalMutationJournalGc
{
    @Test public void checkpointedPayload() throws Exception
    { LocalMutationJournalGcContract.collectsPayloadButKeepsIdentityFence(); }
    @Test public void abortedPayload() throws Exception
    { LocalMutationJournalGcContract.abortOnlyReclamation(); }
    @Test public void unfinishedStreams() throws Exception
    { LocalMutationJournalGcContract.openStreamIsPreserved(); }
    @Test public void crashBoundaries() throws Exception
    { LocalMutationJournalGcContract.crashAtEveryPublicationBoundary(); }
    @Test public void exclusiveWriter() throws Exception
    { LocalMutationJournalGcContract.lockSurvivesGenerationSwitch(); }
    @Test public void corruptCurrentGeneration() throws Exception
    { LocalMutationJournalGcContract.corruptSelectedGenerationNeverFallsBack(); }
    @Test public void boundedPayloadAfterRepeatedGc() throws Exception
    { LocalMutationJournalGcContract.multipleCyclesAndAdmissionRecovery(); }
    @Test public void preserveDeletesAndDifferentTables() throws Exception
    { LocalMutationJournalGcContract.differentTablesAndKindsArePreserved(); }
}
