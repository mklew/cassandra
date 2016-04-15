/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.mpp.transaction.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.SystemClock;
import org.apache.cassandra.mpp.transaction.DeleteTransactionsDataService;
import org.apache.cassandra.mpp.transaction.TransactionId;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.client.TransactionStateUtils;
import org.apache.cassandra.mpp.transaction.paxos.MpPaxosId;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.mpp.transaction.MppTestingUtilities.newTransactionItem;


/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 07/02/16
 */
public class MpPaxosIndexTest
{
    MpPaxosIndex mpPaxosIndex;

    String ksName = "test_ks";
    String cfName = "cf1";
    long token1 = 1;
    long token2 = 2;
    long token3 = 3;
    long token4 = 4;

    final TransactionItem ti1 = newTransactionItem(ksName, cfName, token1);
    final TransactionItem ti2 = newTransactionItem(ksName, cfName, token2);
    final TransactionItem ti3 = newTransactionItem(ksName, cfName, token3);
    final TransactionItem ti4 = newTransactionItem(ksName, cfName, token4);

    private DeleteTransactionsDataServiceStub deleteTransactionsDataService;


    @Before
    public void setUp()
    {
        mpPaxosIndex = new MpPaxosIndex()
        {
            protected Function<TransactionState, Stream<TransactionItem>> getTransactionItemsOwnedByThisNode()
            {
                return txState -> txState.getTransactionItems().stream();
            }
        };
        deleteTransactionsDataService = new DeleteTransactionsDataServiceStub();
        mpPaxosIndex.setDeleteTransactionsDataService(deleteTransactionsDataService);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testShouldFailIfNoneOfTransactionItemsBelongToThisNode()
    {
        final MpPaxosIndex idx = new MpPaxosIndex()
        {
            protected Function<TransactionState, Stream<TransactionItem>> getTransactionItemsOwnedByThisNode()
            {
                return txState -> Stream.empty();
            }
        };

        final TransactionState tx1 = newTransactionState(ti1, ti2, ti3);
        idx.acquireIndex(tx1, (index, items) -> {
            Assert.fail();
        });
    }

    /**
     * Each transaction that registers is participant of at most 1 paxos.
     * Participant might have other tx registered on its "rollback me list"
     */
    @Test
    public void transactionShouldStartPaxosRoundIfThereAreNoOtherTransactions() {
        final TransactionState tx1 = newTransactionState(ti1, ti2, ti3);

        mpPaxosIndex.acquireIndex(tx1, (index, items) -> {
            final MpPaxosIndex.MppIndexResultActions result = index.addItToIndex(tx1, items);
            Assert.assertTrue("Should proceed because there are no conflicts to be checked", result.canProceed());
        });

        final MpPaxosIndex.MpPaxosParticipant participant = mpPaxosIndex.getIndexUnsafe().get(ti1).getParticipantsUnsafe().iterator().next();
        final MpPaxosIndex.MpPaxosParticipant participantForTi2 = mpPaxosIndex.getIndexUnsafe().get(ti2).getParticipantsUnsafe().iterator().next();
        final MpPaxosIndex.MpPaxosParticipant participantForTi3 = mpPaxosIndex.getIndexUnsafe().get(ti3).getParticipantsUnsafe().iterator().next();

        Assert.assertSame(participant, participantForTi2);
        Assert.assertSame(participant, participantForTi3);

        Assert.assertTrue(participant.isParticipatingInPaxosRound());
        Assert.assertEquals(tx1, participant.getTransactionState());
    }

    /**
     * Tx1: 1,2,3
     * Tx2: 2,4
     *
     * Tx2 joins and it sees Tx1 so it should check for conflict with it.
     */
    @Test
    public void shouldCheckForConflictWhenItTriesToJoin() {
        final TransactionState tx1 = newTransactionState(ti1, ti2, ti3);
        final TransactionState tx2 = newTransactionState(ti2, ti4);

        Assert.assertTrue(mpPaxosIndex.acquireIndexAndAdd(tx1).canProceed());

        final MpPaxosIndex.MppIndexResultActions tx2Results = mpPaxosIndex.acquireIndexAndAdd(tx2);

        Assert.assertFalse(tx2Results.canProceed());
        Assert.assertTrue(tx2Results.hasToCheckForConflicts());

        Assert.assertEquals(1, mpPaxosIndex.getIndexUnsafe().get(ti1).getParticipantsUnsafe().size());
        Assert.assertEquals(2, mpPaxosIndex.getIndexUnsafe().get(ti2).getParticipantsUnsafe().size());
        Assert.assertEquals(1, mpPaxosIndex.getIndexUnsafe().get(ti3).getParticipantsUnsafe().size());
        Assert.assertEquals(1, mpPaxosIndex.getIndexUnsafe().get(ti4).getParticipantsUnsafe().size());

        assertParticipantsInOrder(ti2, tx1, tx2);

        final MpPaxosIndex.MpPaxosParticipant participant = findParticipantForStateAndItem(tx2, ti2);

        Assert.assertFalse(participant.isParticipatingInPaxosRound());

        assertHasToCheckConflictWith(tx2Results, tx1, ti2);
    }

    public void assertParticipantsInOrder(TransactionItem ti, TransactionState ... txs) {
        final MpPaxosIndex.MppPaxosRoundPointers mppPaxosRoundPointers = mpPaxosIndex.getIndexUnsafe().get(ti);
        final LinkedHashSet<MpPaxosIndex.MpPaxosParticipant> participantsUnsafe = (LinkedHashSet<MpPaxosIndex.MpPaxosParticipant>) mppPaxosRoundPointers.getParticipantsUnsafe();
        final Iterator<MpPaxosIndex.MpPaxosParticipant> iterator = participantsUnsafe.iterator();
        for (TransactionState tx : txs)
        {
            final MpPaxosIndex.MpPaxosParticipant next = iterator.next();
            Assert.assertEquals(tx, next.getTransactionState());
        }
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldCreateNewPaxosRoundIfResolutionSaysThereAreNoConflicts() {
        final TransactionState tx1 = newTransactionState(ti1, ti2, ti3);
        final TransactionState tx2 = newTransactionState(ti2, ti4);

        mpPaxosIndex.acquireIndexAndAdd(tx1);
        final MpPaxosIndex.MppIndexResultActions results = mpPaxosIndex.acquireIndexAndAdd(tx2);
        final Map<TransactionItem, List<TransactionState>> conflictsToBeChecked = results.getConflictsToBeChecked();
        assertHasToCheckConflictWith(results, tx1, ti2);
        // for each transaction state, try to resolve conflict. In the meantime index state can change in any way.
        MpPaxosIndex.MppIndexResultActions results2 = mpPaxosIndex.acquireAndMarkTxAndNonConflicting(tx2, ti2, tx1);

        Assert.assertTrue(results2.canProceed());
        Assert.assertFalse(results2.hasToCheckForConflicts());
        final MpPaxosIndex.MpPaxosParticipant participant = findParticipantForStateAndItem(tx2, ti2);

        Assert.assertEquals(0, participant.getParticipantsToRollback().size());

        assertHasNonConflicting(participant, tx1);
    }


    private static void assertHasNonConflicting(MpPaxosIndex.MpPaxosParticipant participant, TransactionState tx1)
    {
        Assert.assertTrue(participant.getNonConflictingUnsafe().contains(tx1.getTransactionId()));
    }

    private static void assertHasToCheckConflictWith(MpPaxosIndex.MppIndexResultActions results, TransactionState conflicting, TransactionItem onItem)
    {
        final List<TransactionState> transactionStates = results.getConflictsToBeChecked().get(onItem);
        Assert.assertTrue(transactionStates.contains(conflicting));
    }

    private MpPaxosIndex.MpPaxosParticipant findParticipantForStateAndItem(TransactionState tx, TransactionItem ti)
    {
        return mpPaxosIndex.getIndexUnsafe().get(ti).getParticipantsUnsafe().stream().filter(p -> p.getTransactionState().equals(tx)).findFirst().get();
    }

    /**
     * Tx1: 1,2,3
     * Tx2: 2,4
     *
     * Tx2 joins so it should have same paxos round id as Tx1
     */
    @Test
    public void shouldInheritPaxosIdIfItJoinsRound() {
        final TransactionState tx1 = newTransactionState(ti1, ti2, ti3);
        final TransactionState tx2 = newTransactionState(ti2, ti4);
        mpPaxosIndex.acquireIndexAndAdd(tx1);
        mpPaxosIndex.acquireIndexAndAdd(tx2);

        final MpPaxosIndex.MppIndexResultActions results = mpPaxosIndex.acquireAndMarkTxAsConflicting(tx2, ti2, tx1);

        Assert.assertTrue(results.canProceed());

        final MpPaxosIndex.MpPaxosParticipant p2 = findParticipantForStateAndItem(tx2, ti2);
        final MpPaxosIndex.MpPaxosParticipant p1 = findParticipantForStateAndItem(tx1, ti2);
        Assert.assertEquals(p1.getPaxosId(), p2.getPaxosId());
        assertHasParticipantToRollback(p1, p2);
        assertHasParticipantToRollback(p2, p1);
    }

    private void assertHasParticipantToRollback(MpPaxosIndex.MpPaxosParticipant p1, MpPaxosIndex.MpPaxosParticipant p2)
    {
        Assert.assertTrue(p1.getParticipantsToRollback().contains(p2));
    }

    /**
     * Tx1: 1,2,3
     * Tx2: 2,4
     * Tx4: 4
     *
     * order of registration: Tx1, Tx2, Tx4
     * order of conflict checking: Tx4, Tx2
     */
    @Test
    public void cannotJoinRoundIfThereAreMoreThan1() {
        final TransactionState tx1 = newTransactionState(ti1, ti2, ti3);
        final TransactionState tx2 = newTransactionState(ti2, ti4);
        final TransactionState tx4 = newTransactionState(ti4);

        final MpPaxosIndex.MppIndexResultActions r1 = mpPaxosIndex.acquireIndexAndAdd(tx1);
        final MpPaxosIndex.MppIndexResultActions r2 = mpPaxosIndex.acquireIndexAndAdd(tx2);
        final MpPaxosIndex.MppIndexResultActions r4 = mpPaxosIndex.acquireIndexAndAdd(tx4);
        assertHasToCheckConflictWith(r4, tx2, ti4);
        assertHasToCheckConflictWith(r2, tx1, ti2);

        final MpPaxosIndex.MppIndexResultActions r4after = mpPaxosIndex.acquireAndMarkTxAsConflicting(tx4, ti4, tx2);
        Assert.assertTrue(r4after.canProceed());

        final MpPaxosIndex.MppIndexResultActions r2AfterConflict = mpPaxosIndex.acquireAndMarkTxAsConflicting(tx2, ti2, tx1);

        // Tx2 cannot proceed
        Assert.assertFalse(r2AfterConflict.canProceed());
    }

    @Test
    public void testThatTransactionKnowsItHasToRollbackIfItFindsThatConflictingTransactionHasAlreadyCommitted() {
        // given
        final TransactionState tx1 = newTransactionState(ti1, ti2, ti3);
        final TransactionState tx2 = newTransactionState(ti2, ti4);

        mpPaxosIndex.acquireIndexAndAdd(tx1);
        MpPaxosIndex.MppIndexResultActions r2Results = mpPaxosIndex.acquireIndexAndAdd(tx2);
        MpPaxosIndex.MpPaxosParticipant tx2Participant = r2Results.getPaxosParticipant();

        // when
        mpPaxosIndex.acquireAndMarkAsCommitted(tx1, System.currentTimeMillis());
        final MpPaxosIndex.MppIndexResultActions r2ResolvedConflict = mpPaxosIndex.acquireAndMarkTxAsConflicting(tx2, ti2, tx1);

        // then
        Assert.assertTrue("Was rolledback", tx2Participant.wasItRolledBack());
        Assert.assertTrue("It can check that it should rollback", tx2Participant.hasToRollback());

        assertTransactionDataDeleted(tx2.id());

    }

    private void assertTransactionDataDeleted(TransactionId id)
    {
        Assert.assertTrue("Transaction " + id + " should be deleted", deleteTransactionsDataService.deletedTransactions.contains(id));
    }

    private void assertTransactionDataNotDeleted(TransactionId id)
    {
        Assert.assertTrue("Transaction " + id + " should NOT be deleted", !deleteTransactionsDataService.deletedTransactions.contains(id));
    }

    private static class DeleteTransactionsDataServiceStub implements DeleteTransactionsDataService {

        List<TransactionId> deletedTransactions = new ArrayList<>();

        public void deleteAllPrivateTransactionData(TransactionId transactionId)
        {
            deletedTransactions.add(transactionId);
        }
    }

    @Test
    public void testThatTransactionIsRolledbackIfItIsConflictingAndOtherTxHasCommitted() {
        // given
        final TransactionState tx1 = newTransactionState(ti1, ti2, ti3);
        final TransactionState tx2 = newTransactionState(ti2, ti4);

        mpPaxosIndex.acquireIndexAndAdd(tx1);
        mpPaxosIndex.acquireIndexAndAdd(tx2);

        // given it registers conflict and joins round
        final MpPaxosIndex.MppIndexResultActions r2ResolvedConflict = mpPaxosIndex.acquireAndMarkTxAsConflicting(tx2, ti2, tx1);
        Assert.assertTrue(r2ResolvedConflict.canProceed());

        // when
        mpPaxosIndex.acquireAndMarkAsCommitted(tx1, System.currentTimeMillis());

        // then paxos participant is checked if rollback happened
        final MpPaxosIndex.MpPaxosParticipant paxosParticipant = r2ResolvedConflict.getPaxosParticipant();
        // this will be true if conflict was registered and then other tx committed.
        Assert.assertTrue(paxosParticipant.wasItRolledBack());

        // this will be true if conflict was registered after other tx committed.
        Assert.assertTrue(paxosParticipant.hasToRollback());

        final MpPaxosIndex.MppIndexResultActions tx2AfterRecheck = mpPaxosIndex.acquireAndReCheck(tx2);

        Assert.assertEquals(MpPaxosIndex.CheckForRollbackResult.result, tx2AfterRecheck);
        Assert.assertTrue("Should check for rollback", tx2AfterRecheck.needsToCheckForRollback());
    }

    private final AtomicLong txStateCount = new AtomicLong(0);
    private final long startingTime = SystemClock.getCurrentTimeMillis();
    private TransactionState newTransactionState(TransactionItem ... items) {
        long timestampForThatTx = txStateCount.incrementAndGet() + startingTime;

        TransactionState transactionState = new TransactionState(UUIDGen.getTimeUUID(timestampForThatTx), Collections.emptyList());
        for (TransactionItem item : items)
        {
            transactionState.addTxItem(item);
        }
        return transactionState;
    }

    private TransactionState newTransactionStateForReadOnly(TransactionItem item) {
        return TransactionStateUtils.createReadOnlyTransaction(item.getToken(), item.getKsName(), item.getCfName());
    }

    @Test
    public void testThatTxCanProceedIfOtherGetsRolledBack() {
        final TransactionState tx1 = newTransactionState(ti1, ti2, ti3);
        final TransactionState tx2 = newTransactionState(ti2, ti4);
        final TransactionState tx3 = newTransactionState(ti1, ti2, ti3);
        final TransactionState tx4 = newTransactionState(ti4);

        final MpPaxosIndex.MppIndexResultActions r1 = mpPaxosIndex.acquireIndexAndAdd(tx1);
        final MpPaxosIndex.MppIndexResultActions r2 = mpPaxosIndex.acquireIndexAndAdd(tx2);
        final MpPaxosIndex.MppIndexResultActions r3 = mpPaxosIndex.acquireIndexAndAdd(tx3);
        final MpPaxosIndex.MppIndexResultActions r4 = mpPaxosIndex.acquireIndexAndAdd(tx4);


        // Order: 1, 4, 3, 2
        // 2 cannot proceed

        // tx4 resolved conflict and started round of its own
        final MpPaxosIndex.MppIndexResultActions r4after = mpPaxosIndex.acquireAndMarkTxAsConflicting(tx4, ti4, tx2);

        // tx3 resolved conflicts and joined round of tx1
        mpPaxosIndex.acquireAndMarkTxAsConflicting(tx3, ti1, tx1);
        mpPaxosIndex.acquireAndMarkTxAsConflicting(tx3, ti2, tx1);
        mpPaxosIndex.acquireAndMarkTxAsConflicting(tx3, ti3, tx1);

        mpPaxosIndex.acquireAndMarkTxAsConflicting(tx2, ti2, tx1);
        mpPaxosIndex.acquireAndMarkTxAndNonConflicting(tx2, ti2, tx3);
        MpPaxosIndex.MppIndexResultActions tx2CannotProceed = mpPaxosIndex.acquireAndMarkTxAsConflicting(tx2, ti4, tx4);

        // Tx2 cannot proceed
        Assert.assertFalse(tx2CannotProceed.canProceed());

        // when TX3 is committed, TX1 is rolledback, and TX2 should be able to proceed. TX2 does not conflict with TX3
        mpPaxosIndex.acquireAndMarkAsCommitted(tx3, System.currentTimeMillis());
        assertTransactionDataDeleted(tx1.id());
        assertTransactionDataNotDeleted(tx2.id());
        MpPaxosIndex.MppIndexResultActions tx2ShouldProceed = mpPaxosIndex.acquireAndReCheck(tx2);

        Assert.assertTrue(tx2ShouldProceed.canProceed());
    }

    @Test
    public void testSimpleAcquireForMppPaxosScenario1() {
        final TransactionState tx1 = newTransactionState(ti1, ti2, ti3);
        final TransactionState tx2 = newTransactionState(ti2, ti4);
        final TransactionState tx3 = newTransactionState(ti1, ti2, ti3);
        final TransactionState tx4 = newTransactionState(ti4);


        final Optional<MpPaxosId> r1 = mpPaxosIndex.acquireForMppPaxos(tx1);
        Assert.assertTrue("Tx1 starts its own round", r1.isPresent());

        final Optional<MpPaxosId> r2 = mpPaxosIndex.acquireForMppPaxos(tx2);
        Assert.assertTrue("Tx2 joins Tx1", r2.isPresent());
        Assert.assertEquals("Tx2, tx1 are in same round", r1, r2);

        final Optional<MpPaxosId> r3 = mpPaxosIndex.acquireForMppPaxos(tx3);
        Assert.assertTrue("Tx3 joins Tx1, Tx2", r3.isPresent());
        Assert.assertEquals(r1, r3);

        final Optional<MpPaxosId> r4 = mpPaxosIndex.acquireForMppPaxos(tx4);
        Assert.assertTrue("Tx4 joins Tx1, Tx2, Tx3 in same round", r4.isPresent());
        Assert.assertEquals(r4, r1);
    }

    @Test
    public void acquireAndFindPaxosIdShouldReturnFirstPaxosIdFound() {
        final TransactionState tx1 = newTransactionState(ti1, ti2, ti3);

        TransactionState readTi1Tx = newTransactionStateForReadOnly(ti1);

        final Optional<MpPaxosId> r1 = mpPaxosIndex.acquireForMppPaxos(tx1);
        Assert.assertTrue("Tx1 starts its own round", r1.isPresent());

        Optional<MpPaxosId> mpPaxosId = mpPaxosIndex.acquireAndFindPaxosId(readTi1Tx);
        Assert.assertEquals(mpPaxosId, r1);

        Assert.assertEquals(mpPaxosId, mpPaxosIndex.acquireAndFindPaxosId(newTransactionStateForReadOnly(ti2)));
        Assert.assertEquals(mpPaxosId, mpPaxosIndex.acquireAndFindPaxosId(newTransactionStateForReadOnly(ti3)));

        Assert.assertFalse(mpPaxosIndex.acquireAndFindPaxosId(newTransactionStateForReadOnly(ti4)).isPresent());
    }

    @Test
    public void testSimpleAcquireForMppPaxosScenario2() {
        final TransactionState tx1 = newTransactionState(ti1, ti2, ti3);
        final TransactionState tx2 = newTransactionState(ti2, ti4);
        final TransactionState tx3 = newTransactionState(ti1, ti2, ti3);
        final TransactionState tx4 = newTransactionState(ti4);


        final Optional<MpPaxosId> r4 = mpPaxosIndex.acquireForMppPaxos(tx4);
        Assert.assertTrue("Tx4 starts its own round", r4.isPresent());
        Assert.assertEquals(r4, mpPaxosIndex.acquireAndFindPaxosId(tx4));

        final Optional<MpPaxosId> r1 = mpPaxosIndex.acquireForMppPaxos(tx1);
        Assert.assertTrue("Tx1 starts its own round", r1.isPresent());
        Assert.assertEquals(r1, mpPaxosIndex.acquireAndFindPaxosId(tx1));

        final Optional<MpPaxosId> r2 = mpPaxosIndex.acquireForMppPaxos(tx2);
        Assert.assertFalse("Tx2 has to rollback because Tx1 and Tx4 have different rounds", r2.isPresent());

        final Optional<MpPaxosId> r3 = mpPaxosIndex.acquireForMppPaxos(tx3);
        Assert.assertTrue("Tx3 joins Tx1", r3.isPresent());
        Assert.assertEquals(r1, r3);
    }

    @Test
    public void testSimpleAcquireForMppPaxosScenario3AndRollback() {
        final TransactionState tx1 = newTransactionState(ti1, ti2, ti3);
        final TransactionState tx2 = newTransactionState(ti2, ti4);
        final TransactionState tx3 = newTransactionState(ti1, ti2, ti3);
        final TransactionState tx4 = newTransactionState(ti4);


        final Optional<MpPaxosId> r4 = mpPaxosIndex.acquireForMppPaxos(tx4);
        Assert.assertTrue("Tx4 starts its own round", r4.isPresent());
        Assert.assertEquals(r4, mpPaxosIndex.acquireAndFindPaxosId(tx4));

        final Optional<MpPaxosId> r1 = mpPaxosIndex.acquireForMppPaxos(tx1);
        Assert.assertTrue("Tx1 starts its own round", r1.isPresent());
        Assert.assertEquals(r1, mpPaxosIndex.acquireAndFindPaxosId(tx1));

        final Optional<MpPaxosId> r3 = mpPaxosIndex.acquireForMppPaxos(tx3);
        Assert.assertTrue("Tx3 joins Tx1", r3.isPresent());
        Assert.assertEquals(r1, r3);

        mpPaxosIndex.acquireAndRollback(tx1);
        Assert.assertFalse("After rollback it should not be found", mpPaxosIndex.acquireAndFindPaxosId(tx1).isPresent());

        // should be idempotent
        mpPaxosIndex.acquireAndRollback(tx1);
        Assert.assertFalse("Should be idempotent operation", mpPaxosIndex.acquireAndFindPaxosId(tx1).isPresent());

    }

    // TODO [MPP]:
        // TODO actually check for conflict
        // TODO:    check for same cells.
       // TODO:     quorum read data
}
