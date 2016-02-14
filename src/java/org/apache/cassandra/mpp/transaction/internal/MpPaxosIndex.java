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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Striped;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.mpp.transaction.TransactionId;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.client.TransactionState;

import static org.apache.cassandra.mpp.transaction.internal.ReadTransactionDataServiceImpl.TRANSACTION_ITEMS_OWNED_BY_THIS_NODE;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 06/02/16
 */
public class MpPaxosIndex
{

    private static final Striped<Lock> LOCKS = Striped.lazyWeakLock(DatabaseDescriptor.getConcurrentWriters() * 1024);

    private Map<TransactionItem, MppPaxosRoundPointers> index = new HashMap<>();

    public Map<TransactionItem, MppPaxosRoundPointers> getIndexUnsafe()
    {
        return index;
    }

    /**
     * Method assumes that locks were acquired
     *
     * @param transactionState
     * @param items
     * @return
     */
    public MppIndexResultActions addItToIndex(TransactionState transactionState, List<TransactionItem> items)
    {
        final Map<TransactionItem, Optional<MppPaxosRoundPointers>> gotIndex = items.stream().collect(Collectors.toMap(Function.identity(), ti -> Optional.ofNullable(getIndexUnsafe().get(ti))));

        if (wholeIndexIsEmpty(gotIndex))
        {
            final UUID paxosId = inititatePaxosState(transactionState.getTransactionId());

            gotIndex.forEach((entry, nullPointer) -> {

                MppPaxosRoundPointers pointers = createEmptyPaxosPointers();
                MpPaxosRoundPointer pointer = createNewPaxosRoundPointer(paxosId, transactionState);

                pointers.addPointer(pointer);

                getIndexUnsafe().put(entry, pointers);
            });

            MppIndexForItemResult sameResultForAll = createIndexForItemResultWithCreatedPointer();

            final Map<TransactionItem, MppIndexForItemResult> resultForItem = gotIndex.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toMap(Function.identity(), ti -> sameResultForAll));

            return createIndexResult(1, resultForItem);
        }
        else
        {
            // These pointers are not defined, but some others are. Need to add itself to awaiting candidates and to "check for conflict" for all other present
            final Stream<Map.Entry<TransactionItem, Optional<MppPaxosRoundPointers>>> pointersToAddForAwaiting = gotIndex.entrySet().stream().filter(p -> !p.getValue().isPresent());

            // Pointers are defined. They can be either:
            // 1) Just awaiting and no rounds
            // 2) Just rounds (at least 1 round)

            // IF transaction can start it's own round, and moves transactions from awaiting

            final Stream<Map.Entry<TransactionItem, Optional<MppPaxosRoundPointers>>> definedPointersToAddToCheckForConflicts = gotIndex.entrySet().stream().filter(p -> p.getValue().isPresent());

            definedPointersToAddToCheckForConflicts.map(entry -> {
                final TransactionItem ti = entry.getKey();
                final MppPaxosRoundPointers pointers = entry.getValue().get();

                Preconditions.checkState(pointers.areEmpty(), "Pointers should not be totally");

                return 5;
            });
        }

        return null;
    }

    private MppIndexResultActions createIndexResult(int numberOfPossiblePaxosRounds, Map<TransactionItem, MppIndexForItemResult> perItemResults)
    {
        return new MppIndexResultActions(numberOfPossiblePaxosRounds, perItemResults);
    }

    private MppIndexForItemResult createIndexForItemResultWithCreatedPointer()
    {
        return new MppIndexForItemResult(IndexResultType.CREATED_ROUND_POINTER, Collections.emptyList());
    }

    private static MpPaxosRoundPointer createNewPaxosRoundPointer(UUID paxosId, TransactionState transactionState)
    {
        return new MpPaxosRoundPointerImpl(paxosId, transactionState);
    }


    private static MppPaxosRoundPointers createEmptyPaxosPointers()
    {
        return new MppPaxosRoundPointers(new ArrayList<>(), new ArrayList<>());
    }

    /**
     * TODO maybe using artificial ID would be better. For this moment it does not matter.
     * If depends whatever I break up pointer into more pointers after rollback on initiator.
     *
     * @param paxosId
     */
    private UUID inititatePaxosState(UUID paxosId)
    {
        // TODO [MPP] Implement it: it should create paxos state with given id
        return paxosId;
    }

    private static boolean wholeIndexIsEmpty(Map<TransactionItem, Optional<MppPaxosRoundPointers>> getIndex)
    {
        return getIndex.entrySet().stream().allMatch(e -> !e.getValue().isPresent());
    }


//    private static Stream<TransactionItem> filterTransactionItemsOnlyOwnedByThisNode(Collection<TransactionItem> transactionItems)
//    {
//        final InetAddress broadcastAddress = FBUtilities.getBroadcastAddress();
//
//        return transactionItems.stream().filter(ti -> {
//            final Keyspace keyspace = Keyspace.open(ti.getKsName());
//            List<InetAddress> allReplicas = StorageProxy.getLiveSortedEndpoints(keyspace, ti.getToken());
//            return allReplicas.contains(broadcastAddress);
//        });
//    }


    enum IndexResultType
    {
        CREATED_ROUND_POINTER, ADDED_TO_AWAITING, ADDED_TO_CONFLICTING, ADDED_TO_CHECK_FOR_CONFLICTS;


    }

    public static class MppIndexForItemResult
    {
        private final IndexResultType resultType;

        private final Collection<TransactionState> transactionsToCheckForConflict;

        public MppIndexForItemResult(IndexResultType resultType, Collection<TransactionState> transactionsToCheckForConflict)
        {
            this.resultType = resultType;
            this.transactionsToCheckForConflict = transactionsToCheckForConflict;
        }

        public IndexResultType getResultType()
        {
            return resultType;
        }

        public Collection<TransactionState> getTransactionsToCheckForConflict()
        {
            return transactionsToCheckForConflict;
        }
    }

    public static class MppIndexResultActions
    {

        private int numberOfRoundsItBelongs;

        private Map<TransactionItem, MppIndexForItemResult> results;

        public MppIndexResultActions(int numberOfRoundsItBelongs, Map<TransactionItem, MppIndexForItemResult> results)
        {
            this.numberOfRoundsItBelongs = numberOfRoundsItBelongs;
            this.results = results;
        }

        boolean hasSingleLogicalPaxosInstanceAtThisNode()
        {
            return numberOfRoundsItBelongs == 1;
        }

        public Map<TransactionItem, MppIndexForItemResult> getResults()
        {
            return results;
        }
    }

    /**
     * Has to always be called before doing any operations on index.
     * <p>
     * TODO [MPP] Have to check if there still is private transaction data before trying to acquire index because if there isn't then it means that transaction got rolled back
     *
     * @param transactionState
     * @param mpPaxosIndex
     */
    public void acquireIndex(TransactionState transactionState, BiConsumer<MpPaxosIndex, List<TransactionItem>> mpPaxosIndex)
    {
        final Stream<TransactionItem> sorted = getTransactionItemsOwnedByThisNode().apply(transactionState)
                                                                                   .sorted();
        final List<TransactionItem> ownedByThisNode = sorted.collect(Collectors.toList());

        final List<Lock> locks = ownedByThisNode.stream()
                                                .map(toLockKey)
                                                .map(LOCKS::get)
                                                .collect(Collectors.toList());
        final ArrayDeque<Lock> reversedLocks = locks.stream().collect(Collectors.toCollection(ArrayDeque::new));
        if (locks.isEmpty())
        {
            throw new IllegalArgumentException("None of items in TransactionState belong to index in this node " + transactionState);
        }
        locks.stream().forEach(Lock::lock);
        try
        {
            mpPaxosIndex.accept(this, ownedByThisNode);
        }
        finally
        {
            reversedLocks.descendingIterator().forEachRemaining(Lock::unlock);
        }
    }

    protected Function<TransactionState, Stream<TransactionItem>> getTransactionItemsOwnedByThisNode()
    {
        return TRANSACTION_ITEMS_OWNED_BY_THIS_NODE;
    }

    private static Function<TransactionItem, LockKey> toLockKey = txItem -> new LockKey(txItem.getKsName(), txItem.getCfName(), (Long) txItem.getToken().getTokenValue());

    public boolean canInitPaxosRoundIfAdded(TransactionState transactionState)
    {


        return false;
    }

    private static class LockKey
    {
        public LockKey(String keyspace, String cf, Long token)
        {
            this.keyspace = keyspace;
            this.cf = cf;
            this.token = token;
        }

        private final String keyspace;

        private final String cf;

        private final Long token;


        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LockKey lockKey = (LockKey) o;

            if (!cf.equals(lockKey.cf)) return false;
            if (!keyspace.equals(lockKey.keyspace)) return false;
            if (!token.equals(lockKey.token)) return false;

            return true;
        }

        public int hashCode()
        {
            int result = keyspace.hashCode();
            result = 31 * result + cf.hashCode();
            result = 31 * result + token.hashCode();
            return result;
        }
    }

    public interface MpPaxosState
    {
        // TODO Need to think whether TransactionState of owner is required here.
        // TODO I need a way of finding all conflictingTransactions from doing commit
        // TODO Proposal is TransactionState
        // MppPaxosInstance can be queried to find pointers to other transactions.
        // For each transaction item
        //    find MppPaxosRoundPointer that has transaction id from proposed TransactionState.transactionId
        //    mark as rolled back every other transaction
    }

    /**
     * Handler used to find mpp paxos state.
     * <p>
     * <p>
     * Pointer can be created only IF
     * <p>
     * 1) locks are acquired
     * 2) It would maintain unique identification of paxos round
     * so if there are tx items 1,2,3
     * and Tx1 uses TI1
     * and Tx2 uses TI1,TI2,TI3 then Tx2 cannot create round pointer at tx item 2 and tx item 3 because it would
     * result in having two paxos instances, one identified by tx1 and other by tx2
     * <p>
     * |
     * tx item 1  |  tx 1
     * tx item 2  |
     * tx item 3  |
     * <p>
     * After:
     * tx item 1  |  tx 1 [ tx 2]
     * tx item 2  |  awaiting: tx2
     * tx item 3  |  awaiting: tx2
     */
    public interface MpPaxosRoundPointer
    {
        /**
         * @return id of transaction that initiated this paxos round
         */
        TransactionId getRoundInitiatorId();

        /**
         * @return transaction state of transaction that initiated paxos round
         */
        TransactionState getRoundInitiator();

        /**
         * @return id to lookup in MppPaxosStorage
         */
        UUID getPaxosRoundId();

        /**
         * If MppPaxosRoundPointer exists then transaction #3 is added to each pointer
         * to its "to be checked for conflicts"
         * <p>
         * If next transaction #4 comes then it sees #3 with which it can check conflict
         * <p>
         * If #3 returns before #4 and has some conflict then it is added conflicting list
         * If #3 returns before #4 and has no conflict then maybe new pointer is created or more conflict checking is required
         * If #4 returns before #3 and has conflict then it can already move Tx3 to conflicting and also itself.
         * Then there is guarantee that #3 won't have to check for conflicts again, transactions that come after it have to do it once.
         */
        Collection<TransactionState> getCandidatesToBeCheckedForConflicts();

        /**
         * Other participating transactions
         *
         * @return ids of transactions that have been checked against conflict and are indeed conflicting
         * <p>
         * TODO Tradeoff between checking new transaction vs all participating OR only vs initiator
         */
        Collection<TransactionId> getConflictingTransactionIds();

        /**
         * @return all conflicting transactions and initiator
         */
        default Stream<TransactionId> getAllParticipatingTransactions()
        {
            return Stream.concat(Stream.of(getRoundInitiatorId()), getConflictingTransactionIds().stream());
        }
    }

    private static class MpPaxosRoundPointerImpl implements MpPaxosRoundPointer
    {

        private final UUID paxosId;

        private final TransactionState initiator;

        private final Set<TransactionState> candidatesToBeCheckedForConflict;

        private final Set<TransactionId> conflictingTransactionIds;

        public MpPaxosRoundPointerImpl(UUID paxosId, TransactionState initiator)
        {
            this.paxosId = paxosId;
            this.initiator = initiator;
            this.candidatesToBeCheckedForConflict = new HashSet<>();
            this.conflictingTransactionIds = new HashSet<>();
        }

        public TransactionId getRoundInitiatorId()
        {
            return initiator.id();
        }

        public TransactionState getRoundInitiator()
        {
            return initiator;
        }

        public UUID getPaxosRoundId()
        {
            return paxosId;
        }

        public Collection<TransactionState> getCandidatesToBeCheckedForConflicts()
        {
            return Collections.unmodifiableCollection(candidatesToBeCheckedForConflict);
        }

        public Collection<TransactionId> getConflictingTransactionIds()
        {
            return Collections.unmodifiableCollection(conflictingTransactionIds);
        }
    }

    public static class MppPaxosRoundPointers
    {

        /**
         * Here are transactions that need to wait for other
         * transactions to complete before they can start paxos round on their own.
         */
        private final Collection<TransactionState> awaitingCandidates;

        private final Collection<MpPaxosRoundPointer> paxosRounds;

        public MppPaxosRoundPointers(Collection<TransactionState> awaitingCandidates, Collection<MpPaxosRoundPointer> paxosRounds)
        {
            this.awaitingCandidates = awaitingCandidates;
            this.paxosRounds = paxosRounds;
        }

        public Collection<TransactionState> getAwaitingCandidates()
        {
            return awaitingCandidates;
        }

        public boolean hasValidState() {
            return (roundsSize() > 0 && awaitingCandidates.isEmpty()) || areEmpty();
        }

        public Collection<MpPaxosRoundPointer> getPaxosRounds()
        {
            return paxosRounds;
        }

        public int roundsSize()
        {
            return paxosRounds.size();
        }

        public void addPointer(MpPaxosRoundPointer pointer)
        {
            Preconditions.checkState(paxosRounds.stream()
                                                .noneMatch(p -> p.getPaxosRoundId().equals(pointer.getPaxosRoundId())),
                                     "Round pointers already have pointer to same paxos round, something is wrong: PaxosId %s",
                                     pointer.getPaxosRoundId());
            paxosRounds.add(pointer);
        }

        public boolean areEmpty()
        {
            return awaitingCandidates.isEmpty() && paxosRounds.isEmpty();
        }
    }
}
