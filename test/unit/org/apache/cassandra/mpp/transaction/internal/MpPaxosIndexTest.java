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

import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.client.TransactionState;

import static org.apache.cassandra.mpp.transaction.MppTestingUtilities.newTransactionItem;
import static org.apache.cassandra.mpp.transaction.MppTestingUtilities.newTransactionState;

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

    @Test
    public void testShouldHoldInvariants()
    {
        final TransactionState tx1 = newTransactionState(ti1, ti2, ti3);
        final TransactionState tx2 = newTransactionState(ti2, ti4);
        final TransactionState tx3 = newTransactionState(ti1, ti2, ti3);
        final TransactionState tx4 = newTransactionState(ti4);

        // Order 2, 4, 3, 1

        mpPaxosIndex.acquireIndex(tx2, (index, items) -> {
            final MpPaxosIndex.MppIndexResultActions result = index.addItToIndex(tx2, items);
            Assert.assertTrue("There should be single paxos instance for tx2", result.hasSingleLogicalPaxosInstanceAtThisNode());
        });

        mpPaxosIndex.acquireIndex(tx4, (index, items) -> {
            final MpPaxosIndex.MppIndexResultActions result = index.addItToIndex(tx4, items);

            Assert.assertFalse("There cannot be paxos instance for tx4 yet because it needs to be checked against conflict with tx2", result.hasSingleLogicalPaxosInstanceAtThisNode());

            Assert.assertEquals(1, result.getResults().size());
            result.getResults().forEach((ti, tiResult) -> {
                // Assert result
                Assert.assertEquals(MpPaxosIndex.IndexResultType.ADDED_TO_CHECK_FOR_CONFLICTS, tiResult.getResultType());

                Assert.assertEquals(1, tiResult.getTransactionsToCheckForConflict().size());
                Assert.assertTrue("Have to check for conflict with transaction tx2", tiResult.getTransactionsToCheckForConflict().contains(tx2));
            });


            // Assert index internal state
            final MpPaxosIndex.MppPaxosRoundPointers pointers = index.getIndexUnsafe().get(ti4);
            Assert.assertEquals(1, pointers.roundsSize());
            final MpPaxosIndex.MpPaxosRoundPointer roundPointer = findPointerByInitiator(tx2, pointers);

            Assert.assertEquals("Has only tx4 in awaiting", 1, roundPointer.getCandidatesToBeCheckedForConflicts().size());
            Assert.assertTrue("Has tx4 in awaiting", roundPointer.getCandidatesToBeCheckedForConflicts().contains(tx4));
        });

        mpPaxosIndex.acquireIndex(tx1, (index, items) -> {
            final MpPaxosIndex.MppIndexResultActions actions = index.addItToIndex(tx1, items);
            Assert.assertTrue("There should be single paxos instance for tx1", actions.hasSingleLogicalPaxosInstanceAtThisNode());


            index.canInitPaxosRoundIfAdded(tx1);

//            index.addToIndex(tx1);
        });
    }

    private MpPaxosIndex.MpPaxosRoundPointer findPointerByInitiator(TransactionState initiator, MpPaxosIndex.MppPaxosRoundPointers pointers)
    {
        return pointers.getPaxosRounds().stream().filter(piRound -> piRound.getRoundInitiatorId().equals(initiator.id())).findFirst().get();
    }
}
