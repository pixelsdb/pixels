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

import org.junit.Test;

public class TestLocalMutationJournal
{
    @Test
    public void roundTripAndStreamIsolation() throws Exception
    {
        LocalMutationJournalContract.roundTripAndStreamIsolation();
    }

    @Test
    public void boundedPrivateCacheFallsBackToWal() throws Exception
    {
        LocalMutationJournalContract.boundedPrivateCacheFallsBackToWal();
    }

    @Test
    public void duplicatesAndDefensiveCopies() throws Exception
    {
        LocalMutationJournalContract.duplicatesAndDefensiveCopies();
    }

    @Test
    public void gapsSchemaAndSealValidation() throws Exception
    {
        LocalMutationJournalContract.gapsSchemaAndSealValidation();
    }

    @Test
    public void abortFencesOldAndNewStreams() throws Exception
    {
        LocalMutationJournalContract.abortFencesOldAndNewStreams();
    }

    @Test
    public void unacknowledgedSuffixIsDiscarded() throws Exception
    {
        LocalMutationJournalContract.unacknowledgedSuffixIsDiscarded();
    }

    @Test
    public void acknowledgedTruncationFailsClosed() throws Exception
    {
        LocalMutationJournalContract.acknowledgedTruncationFailsClosed();
    }

    @Test
    public void acknowledgedCorruptionFailsClosed() throws Exception
    {
        LocalMutationJournalContract.acknowledgedCorruptionFailsClosed();
    }

    @Test
    public void corruptMarkerFailsClosed() throws Exception
    {
        LocalMutationJournalContract.corruptMarkerFailsClosed();
    }

    @Test
    public void missingWalFailsClosed() throws Exception
    {
        LocalMutationJournalContract.missingWalFailsClosed();
    }

    @Test
    public void missingMarkerFailsClosed() throws Exception
    {
        LocalMutationJournalContract.missingMarkerFailsClosed();
    }

    @Test
    public void exclusiveOwner() throws Exception
    {
        LocalMutationJournalContract.exclusiveOwner();
    }

    @Test
    public void admissionLimits() throws Exception
    {
        LocalMutationJournalContract.admissionLimits();
    }

    @Test
    public void postOpenCorruptionPoisonsJournal() throws Exception
    {
        LocalMutationJournalContract.postOpenCorruptionPoisonsJournal();
    }

    @Test
    public void manyTransactionsShareOneWal() throws Exception
    {
        LocalMutationJournalContract.manyTransactionsShareOneWal();
    }

    @Test
    public void concurrentSealsShareOneDurabilityBarrier() throws Exception
    {
        LocalMutationJournalContract.concurrentSealsShareOneDurabilityBarrier();
    }

    @Test
    public void syncDoesNotSeal() throws Exception
    {
        LocalMutationJournalContract.syncDoesNotSeal();
    }

    @Test
    public void mutationKindsAreIndependent() throws Exception
    {
        LocalMutationJournalContract.mutationKindsAreIndependent();
    }

    @Test
    public void incompleteFrameInDurablePrefixFailsClosed() throws Exception
    {
        LocalMutationJournalContract.incompleteFrameInDurablePrefixFailsClosed();
    }
}
