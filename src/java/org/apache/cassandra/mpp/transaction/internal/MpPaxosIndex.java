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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Striped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.mpp.transaction.DeleteTransactionsDataService;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.mpp.transaction.internal.ReadTransactionDataServiceImpl.TRANSACTION_ITEMS_OWNED_BY_THIS_NODE;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 06/02/16
 */
public class MpPaxosIndex
{

    private static final Striped<Lock> LOCKS = Striped.lazyWeakLock(DatabaseDescriptor.getConcurrentWriters() * 1024);

    private static final Logger logger = LoggerFactory.getLogger(MpPaxosIndex.class);

    private Map<TransactionItem, MppPaxosRoundPointers> index = new HashMap<>();

    private DeleteTransactionsDataService deleteTransactionsDataService;

    public DeleteTransactionsDataService getDeleteTransactionsDataService()
    {
        return deleteTransactionsDataService;
    }

    public void setDeleteTransactionsDataService(DeleteTransactionsDataService deleteTransactionsDataService)
    {
        this.deleteTransactionsDataService = deleteTransactionsDataService;
    }

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
        final Map<TransactionItem, Optional<MppPaxosRoundPointers>> gotIndex = getIndexForItems(items);

        if (wholeIndexIsEmpty(gotIndex))
        {
            final UUID paxosId = inititatePaxosState();
            final MpPaxosParticipant paxosParticipant = MpPaxosParticipant.createForNewRound(paxosId, transactionState);

            gotIndex.forEach((entry, nullPointer) -> {
                MppPaxosRoundPointers pointers = createEmptyPaxosPointers();
                pointers.addParticipant(paxosParticipant);
                getIndexUnsafe().put(entry, pointers);
            });
            return createIndexResult(paxosId, paxosParticipant);
        }
        else
        {
            // Register itself in each
            MpPaxosParticipant paxosParticipant = MpPaxosParticipant.createAwaitingForConflictResolution(transactionState);
            gotIndex.forEach((entry, optionalPointers) -> {
                MppPaxosRoundPointers pointers = optionalPointers.orElse(createEmptyPaxosPointers());
                pointers.addParticipant(paxosParticipant);
                // Add to index
                getIndexUnsafe().put(entry, pointers);
            });

            return reCheckTransactionParticipant(paxosParticipant);
        }
    }

    private Map<TransactionItem, List<MpPaxosParticipant>> getParticipantsUntil(Map<TransactionItem, Optional<MppPaxosRoundPointers>> gotIndex, MpPaxosParticipant paxosParticipant)
    {
        return getParticipantsUntilTx(gotIndex, paxosParticipant.getTransactionState());
    }

    private Map<TransactionItem, List<MpPaxosParticipant>> getParticipantsUntilTx(Map<TransactionItem, Optional<MppPaxosRoundPointers>> gotIndex, TransactionState txState)
    {
        return gotIndex.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                                                                     v -> v.getValue()
                                                                           .map(p -> {
                                                                               final Predicate<MpPaxosParticipant> predicate = par -> !txState.equals(par.getTransactionState());
                                                                               final Stream<MpPaxosParticipant> mpPaxosParticipantStream = StreamUtils.takeWhile(p.getParticipantsUnsafe().stream(), predicate);
                                                                               return mpPaxosParticipantStream.collect(Collectors.toList());
                                                                           })
                                                                           .orElse(Collections.emptyList())));
    }


    private Map<TransactionItem, Optional<MppPaxosRoundPointers>> getIndexForItems(List<TransactionItem> items)
    {
        return items.stream().collect(Collectors.toMap(Function.identity(), ti -> Optional.ofNullable(getIndexUnsafe().get(ti))));
    }

    /**
     *
     * PaxosParticipant P_3
     *
     * Participants for some token: P_1, P_2, P_3, P_4, P_5
     *
     * P_3 is responsible to check for conflict with all before him, so with P_1, P_2
     * These conflicts P_1, P_2 have to be resolved.
     * P_4, P_5 is ignored.
     *
     * If P_4 resolves conflict with P_3 and there is a conflict then it will also mark it on P_3
     *
     * So gather all paxos round ids from all conflicting transactions in given token
     *
     *
     * @param paxosParticipant
     * @return
     */
    private MppIndexResultActions reCheckTransactionParticipant(MpPaxosParticipant paxosParticipant)
    {
        final List<TransactionItem> items = getTransactionItemsOwnedByThisNodeSorted(paxosParticipant.getTransactionState());

        final Map<TransactionItem, Optional<MppPaxosRoundPointers>> indexForItems = getIndexForItems(items);


        final Collection<MpPaxosParticipant> conflictingParticipants = paxosParticipant.getParticipantsToRollback();

        final Stream<MpPaxosParticipant> allParticipantsForAllItems = indexForItems.entrySet().stream().map(Map.Entry::getValue).filter(Optional::isPresent).map(Optional::get).flatMap(p -> p.getParticipantsUnsafe().stream());
        final Set<UUID> paxosRoundsFromConflictingTransactions = allParticipantsForAllItems.filter(conflictingParticipants::contains).filter(MpPaxosParticipant::isParticipatingInPaxosRound).map(p -> p.paxosId).filter(Optional::isPresent).map(Optional::get).collect(Collectors.toSet());


        // these are guys with whom it still has to resolve conflict.
        // these are participants until paxosParticipant.
        final Map<TransactionItem, List<MpPaxosParticipant>> participantsUntil = getParticipantsUntil(indexForItems, paxosParticipant);
        final Map<TransactionItem, List<TransactionState>> needToResolveConflictWithThese = filterAndMapParticipants(participantsUntil, par -> !paxosParticipant.hasResolvedConflictWith(par), MpPaxosParticipant::getTransactionState);

        final Set<TransactionState> txToCheckForConflictWith = getSetOfValues(needToResolveConflictWithThese);
        if(paxosRoundsFromConflictingTransactions.isEmpty() && txToCheckForConflictWith.isEmpty()) {
            // TODO start new round or proceed in current? something has happened that all the other transactions were removed
            final UUID paxosId = inititatePaxosState();
            paxosParticipant.joinRound(paxosId);
            return new MppIndexResultActions(Sets.newHashSet(paxosId), needToResolveConflictWithThese, paxosParticipant);
        }
        else if(paxosRoundsFromConflictingTransactions.size() == 1 && txToCheckForConflictWith.isEmpty()) {
            // All conflicts were resolved and there is only single paxos round -> This is expected use case
            // Join that paxos round and proceed
            final UUID paxosId = paxosRoundsFromConflictingTransactions.iterator().next();
            paxosParticipant.joinRound(paxosId);
        }

        if(paxosRoundsFromConflictingTransactions.size() > 1) {
            // Cannot join, because there are ambigous number of possible paxos rounds.
            // TODO log it
        }

        if(!txToCheckForConflictWith.isEmpty()) {
            // There are still conflicts that have to be resolved.
            // TODO log it
        }

        return new MppIndexResultActions(paxosRoundsFromConflictingTransactions, needToResolveConflictWithThese, paxosParticipant);
    }

    private static Map<TransactionItem, List<MpPaxosParticipant>> filterParticipants(Map<TransactionItem, List<MpPaxosParticipant>> participantsMap, Predicate<MpPaxosParticipant> predicate)
    {
        return filterAndMapParticipants(participantsMap, predicate, Function.identity());
    }

    private static <T> Map<TransactionItem, List<T>> filterAndMapParticipants(Map<TransactionItem, List<MpPaxosParticipant>> participantsMap, Predicate<MpPaxosParticipant> predicate, Function<MpPaxosParticipant, T> mapper)
    {
        return participantsMap
               .entrySet()
               .stream()
               .collect(Collectors.toMap(
                                        Map.Entry::getKey,
                                        v -> v.getValue()
                                              .stream()
                                              .filter(predicate)
                                              .map(mapper)
                                              .collect(Collectors.toList())));
    }



    public MppIndexResultActions acquireAndMarkTxAndNonConflicting(TransactionState transactionStateToAcquire, TransactionItem resolvedItem, TransactionState nonConflictingTx)
    {
        final MppIndexResultActions[] result = new MppIndexResultActions[1];
        acquireIndex(transactionStateToAcquire, (index, items) -> {
            Pair<Optional<RemoveParticipantsFromIndex>, MppIndexResultActions> optionalMppIndexResultActionsPair = resolveConflict(transactionStateToAcquire, resolvedItem, nonConflictingTx, false);
            Preconditions.checkState(!optionalMppIndexResultActionsPair.left.isPresent());
            result[0] = optionalMppIndexResultActionsPair.right;
        });
        return result[0];
    }

    public MppIndexResultActions acquireAndMarkTxAsConflicting(TransactionState thisTransactionState, TransactionItem item, TransactionState conflictingTx)
    {
        final MppIndexResultActions[] result = new MppIndexResultActions[1];
        final Optional<RemoveParticipantsFromIndex>[] rollbackAction = new Optional[1];
        acquireIndex(thisTransactionState, (index, items) -> {
            Pair<Optional<RemoveParticipantsFromIndex>, MppIndexResultActions> pair = resolveConflict(thisTransactionState, item, conflictingTx, true);
            result[0] = pair.right;
            rollbackAction[0] = pair.left;
        });
        doRemoveParticipantsFromIndex(rollbackAction[0]);
        return result[0];
    }

    // TODO [MPP] this timestamp is not yet used, but I have feeling that it is needed.
    public void acquireAndMarkAsCommitted(TransactionState transactionState, long timestampOfCommit)
    {
        final Optional<RemoveParticipantsFromIndex>[] result = new Optional[1];
        // acquire
        acquireIndex(transactionState, (index, items) -> {
            result[0] = markAsCommitted(transactionState, items, timestampOfCommit);
        });
        // released
        Optional<RemoveParticipantsFromIndex> removeParticipantsFromIndexOpt = result[0];
        doRemoveParticipantsFromIndex(removeParticipantsFromIndexOpt);
    }

    public MppIndexResultActions acquireAndReCheck(TransactionState transactionState)
    {
        final MppIndexResultActions[] result = new MppIndexResultActions[1];
        acquireIndex(transactionState, (index, items) -> {
            result[0] = reCheckTransaction(transactionState, items);
        });
        return result[0];
    }

    private MppIndexResultActions reCheckTransaction(TransactionState transactionState, List<TransactionItem> items)
    {
        Optional<MpPaxosParticipant> participant = findParticipant(items.iterator().next(), transactionState);
        if(!participant.isPresent()) {
            return CheckForRollbackResult.result;
        }
        else
        {
            return reCheckTransactionParticipant(participant.get());
        }
    }

    private void doRemoveParticipantsFromIndex(Optional<RemoveParticipantsFromIndex> removeParticipantsFromIndexOpt)
    {
        removeParticipantsFromIndexOpt.ifPresent(removeParticipants -> {
            List<TransactionItem> itemsToAcquire = removeParticipants.getSortedItemsToAcquire();

            // optionally acquire again, all that are going to be modified
            acquireIndexForItems(null, (index, items) -> {
                // items are the same as itemsToAcquire
                removeParticipants.getParticipants().forEach(this::removeParticipantFromIndex);
            }, itemsToAcquire);
        });
    }

    private static class RemoveParticipantsFromIndex {
        Stream<MpPaxosParticipant> participants;

        Stream<TransactionItem> items;

        private RemoveParticipantsFromIndex(Stream<MpPaxosParticipant> participants, Stream<TransactionItem> items)
        {
            this.participants = participants;
            this.items = items;
        }

        public static RemoveParticipantsFromIndex reduce(RemoveParticipantsFromIndex first, RemoveParticipantsFromIndex second) {
            return new RemoveParticipantsFromIndex(Stream.concat(first.participants, second.participants), Stream.concat(first.items, second.items));
        }

        public Collection<MpPaxosParticipant> getParticipants()
        {
            return participants.collect(Collectors.toList());
        }

        public List<TransactionItem> getSortedItemsToAcquire()
        {
            return items.sorted().distinct().collect(Collectors.toList());
        }

        public static BinaryOperator<RemoveParticipantsFromIndex> reducer = (r1, r2) -> reduce(r1,r2);
    }

    /**
     *
     * @param transactionState
     * @param items
     * @param timestampOfCommit
     * @return
     */
    private Optional<RemoveParticipantsFromIndex> markAsCommitted(TransactionState transactionState, List<TransactionItem> items, long timestampOfCommit)
    {
        final Optional<MpPaxosParticipant> participant = findParticipant(items.iterator().next(), transactionState);

        // Add to all participants, that are not participating in paxos round that this TX has committed.
        tellNonParticipatingThatThisTransactionHasCommitted(transactionState, items, timestampOfCommit);

        if(!participant.isPresent()) {
            // This in principle should not happen, unless this node receives commit after it was down.
            // Then other transactions will rollback anyway, because rollback will be noticed on other nodes.
            logger.warn("markAsCommited could not find participant for committed transaction: {}", transactionState);
            return Optional.empty();
        }
        else {
            final MpPaxosParticipant paxosParticipant = participant.get();
            Stream<MpPaxosParticipant> participantsToRollbackStream = paxosParticipant.getParticipantsToRollback().stream();
            final Optional<RemoveParticipantsFromIndex> participantsToRemove = rollbackParticipantsDueToTx(transactionState, participantsToRollbackStream);
            return participantsToRemove;
        }

    }

    /**
     * Other transactions maybe are currently resolving conflicts and if they did they would participate in same round as just commited transaction.
     * If that is the case, then next time they do something, they will notice that they should rollback themselfs.
     *
     */
    private void tellNonParticipatingThatThisTransactionHasCommitted(TransactionState transactionState, List<TransactionItem> items, long timestampOfCommit)
    {
        getIndexForItems(items).entrySet().stream().forEach(entry -> {
            entry.getValue().ifPresent(pointers -> {
                pointers.getParticipantsUnsafe().stream().filter(par -> !transactionState.equals(par.getTransactionState()))
                .filter(par -> !par.isParticipatingInPaxosRound())
                .forEach(par -> {
                    par.transactionHasCommitted(transactionState.getTransactionId(), timestampOfCommit);
                });
            });
        });
    }

    private Optional<RemoveParticipantsFromIndex> rollbackParticipantsDueToTx(TransactionState transactionStateThatCausedRollbackOf, Stream<MpPaxosParticipant> participantsToRollback) {
        return rollbackParticipantsDueToTx(transactionStateThatCausedRollbackOf, participantsToRollback, false);
    }

    private Optional<RemoveParticipantsFromIndex> rollbackParticipantsDueToTx(TransactionState transactionStateThatCausedRollbackOf, Stream<MpPaxosParticipant> participantsToRollback, boolean allLocksForParticipantsToRollbackWereAcquired)
    {
        return participantsToRollback.map(par -> {
                    par.markThatItHasToRollback();
                    getDeleteTransactionsDataService().deleteAllPrivateTransactionData(par.getTransactionState().id());
                    par.markItWasRolledBack();

                    final TransactionState txStateOfRolledback = par.getTransactionState();
                    if (transactionStateThatCausedRollbackOf.hasExactlySameItems(txStateOfRolledback) || allLocksForParticipantsToRollbackWereAcquired)
                    {
                        // then all locks are acquired, and it it can be removed from index
                        removeParticipantFromIndex(par);

                        // after remopval, participants that were blocked by articipant "par" could retry and maybe have green light to begin execution.
                        // How to notify them?
                        return Optional.<MpPaxosParticipant>empty();
                    }
                    else
                    {
                        // not all locks were acquired therefore it has to:
                        // 1. release current locks
                        // 2. acquire new locks which are sorted concatentation of all rolled back transactions
                        // 3. remove these participants from index
                        return Optional.of(par);
                    }
                }).filter(Optional::isPresent)
                                                                                                   .map(Optional::get)
                                                                                                   .map(par -> new RemoveParticipantsFromIndex(Stream.of(par), getTransactionItemsOwnedByThisNodeSorted(par.getTransactionState()).stream()))
                                                                                                   .reduce(RemoveParticipantsFromIndex.reducer);
    }

    /**
     * This method assumes that all locks for given participant were acquired.
     * @param mpPaxosParticipant
     */
    private void removeParticipantFromIndex(MpPaxosParticipant mpPaxosParticipant)
    {
        final TransactionState transactionState = mpPaxosParticipant.getTransactionState();
        final List<TransactionItem> itsItems = getTransactionItemsOwnedByThisNodeSorted(transactionState);
        itsItems.forEach(item -> {
            final MppPaxosRoundPointers mppPaxosRoundPointers = getIndexUnsafe().get(item);
            if (mppPaxosRoundPointers != null)
            {
                mppPaxosRoundPointers.removeParticipant(mpPaxosParticipant);
                if (mppPaxosRoundPointers.isEmpty())
                {
                    getIndexUnsafe().remove(item);
                }
            }
        });
    }

    private Pair<Optional<RemoveParticipantsFromIndex>, MppIndexResultActions> resolveConflict(TransactionState thisTransactionState, TransactionItem itemOfConflict, TransactionState checkedTx, boolean isConflict)
    {
        final Optional<MpPaxosParticipant> checkedPaxosParticipant = findParticipant(itemOfConflict, checkedTx);
        final Optional<MpPaxosParticipant> participant = findParticipant(itemOfConflict, thisTransactionState);

        if(!participant.isPresent()) {
            // That means that other conflict was resolved before, which allowed this transaction to proceed, but other transaction has won and rollbacked this one.
            // If this is correct then, there should be no transaction data.
            return Pair.create(Optional.empty(), CheckForRollbackResult.result);
        }

        final MpPaxosParticipant paxosParticipant = participant.get();

        if (checkedPaxosParticipant.isPresent() && isConflict)
        {
            // add them to their rollback lists and recheck
            if(checkedPaxosParticipant.get().addToRollbackParticipants(paxosParticipant)) {
                // rollback checkedPaxosParticipant
                Optional<RemoveParticipantsFromIndex> removeParticipantsFromIndex = rollbackParticipantsDueToTx(paxosParticipant.getTransactionState(), Stream.of(checkedPaxosParticipant.get()));
                return Pair.create(removeParticipantsFromIndex, reCheckTransactionParticipant(paxosParticipant));
            }
            if(paxosParticipant.addToRollbackParticipants(checkedPaxosParticipant.get()))
            {
                // if has to rollback self, then it will be possible and no futher action will be required
                Optional<RemoveParticipantsFromIndex> nextAction = rollbackParticipantsDueToTx(checkedPaxosParticipant.get().getTransactionState(), Stream.of(paxosParticipant), true);
                Preconditions.checkState(!nextAction.isPresent());
                return Pair.create(Optional.empty(), CheckForRollbackResult.result);
            }

            return Pair.create(Optional.empty(), reCheckTransactionParticipant(paxosParticipant));
        }
        else if(checkedPaxosParticipant.isPresent())
        {
            paxosParticipant.addAsNonConflicting(checkedPaxosParticipant.get().getTransactionState().getTransactionId());
            return Pair.create(Optional.empty(), reCheckTransactionParticipant(paxosParticipant));
        }
        else {
            // if there is no conflict (isConflict = false) or checkedPaxosParticipant does not exist any more - because it could get rolled back
            // then there is nothing else to do.
            return Pair.create(Optional.empty(), reCheckTransactionParticipant(paxosParticipant));
        }
    }

    private Optional<MpPaxosParticipant> findParticipant(TransactionItem item, TransactionState tx)
    {
        return getIndexUnsafe().get(item).getParticipantsUnsafe().stream().filter(p -> p.getTransactionState().equals(tx)).findFirst();
    }

    private static MppIndexResultActions createIndexResult(UUID paxosId, MpPaxosParticipant paxosParticipant)
    {
        return new MppIndexResultActions(Sets.newHashSet(paxosId), Collections.emptyMap(), paxosParticipant);
    }

    private Map<TransactionItem, List<TransactionState>> participantsToTransactionStates(Map<TransactionItem, List<MpPaxosParticipant>> participants)
    {
        return participants.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                                                                         v -> v.getValue().stream().map(MpPaxosParticipant::getTransactionState).collect(Collectors.toList())));
    }

    private Set<UUID> getAllPaxosRounds(Map<TransactionItem, List<MpPaxosParticipant>> participants)
    {
        return participants.entrySet().stream().map(Map.Entry::getValue).flatMap(ps -> ps.stream().map(p -> p.paxosId)).filter(Optional::isPresent).map(Optional::get).collect(Collectors.toSet());
    }

    private static Set<MpPaxosParticipant> getSetOfParticipants(Map<TransactionItem, List<MpPaxosParticipant>> participants)
    {
        return getSetOfValues(participants);
    }

    private static <T> Set<T> getSetOfValues(Map<TransactionItem, List<T>> participants)
    {
        return participants.entrySet().stream().map(Map.Entry::getValue).flatMap(Collection::stream).collect(Collectors.toSet());
    }

    private static List<UUID> collectTransactionIdsFromParticipants(MppPaxosRoundPointers pointers)
    {
        return pointers.getParticipantsUnsafe().stream().map(p -> p.getTransactionState().getTransactionId()).collect(Collectors.toList());
    }




    public static class CommittedParticipant {
        private final UUID transactionId;
        private final long timeOfCommit;


        public CommittedParticipant(UUID transactionId, long timeOfCommit)
        {
            this.transactionId = transactionId;
            this.timeOfCommit = timeOfCommit;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CommittedParticipant that = (CommittedParticipant) o;

            if (!transactionId.equals(that.transactionId)) return false;

            return true;
        }

        public int hashCode()
        {
            return transactionId.hashCode();
        }
    }

    public static class MpPaxosParticipant {
        private Optional<UUID> paxosId;

        private final TransactionState transactionState;

        private Set<MpPaxosParticipant> rollbackTheseParticipantsOnCommit;

        private Set<UUID> nonConflictingTransactionIds;

        private Set<CommittedParticipant> committedParticipants;

        private volatile boolean hasToRollback = false;
        private volatile boolean wasRolledBack = false;

        public MpPaxosParticipant(Optional<UUID> paxosId, TransactionState transactionState, int conflictsToResolve)
        {
            this.paxosId = paxosId;
            this.transactionState = transactionState;
        }

        /**
         *
         * @param paxosParticipant
         * @return {@code true} if has to rollback this participant, false otherwise
         */
        public synchronized boolean addToRollbackParticipants(MpPaxosParticipant paxosParticipant) {
            if(hasThatParticipantAlreadyCommitted(paxosParticipant)) {
                markThatItHasToRollback();
                return true;
            }

            addToRollbackParticipantsInternal(paxosParticipant);
            return false;
        }

        private boolean hasThatParticipantAlreadyCommitted(MpPaxosParticipant paxosParticipant)
        {
            UUID transactionId = paxosParticipant.getTransactionState().getTransactionId();
            return hasThatTransactionAlreadyCommitted(transactionId);
        }

        private boolean hasThatTransactionAlreadyCommitted(UUID transactionId)
        {
            return committedParticipants != null && committedParticipants.contains(new CommittedParticipant(transactionId, 0L));
        }

        public synchronized Collection<MpPaxosParticipant> getParticipantsToRollback()
        {
            if(rollbackTheseParticipantsOnCommit == null)
            {
                return Collections.emptyList();
            }
            else {
                return Collections.unmodifiableCollection(rollbackTheseParticipantsOnCommit);
            }
        }

        private void addToRollbackParticipantsInternal(MpPaxosParticipant paxosParticipant)
        {
            if(rollbackTheseParticipantsOnCommit == null) {
                rollbackTheseParticipantsOnCommit = new HashSet<>();
            }

            rollbackTheseParticipantsOnCommit.add(paxosParticipant);
        }

        public static MpPaxosParticipant createForNewRound(UUID paxosId, TransactionState transactionState)
        {
            return new MpPaxosParticipant(Optional.of(paxosId), transactionState, 0);
        }

        boolean isParticipatingInPaxosRound()
        {
            return paxosId.isPresent() && !wasRolledBack && !hasToRollback;
        }

        public Optional<UUID> getPaxosId()
        {
            return paxosId;
        }

        public TransactionState getTransactionState()
        {
            return transactionState;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MpPaxosParticipant that = (MpPaxosParticipant) o;

            if (!transactionState.equals(that.transactionState)) return false;

            return true;
        }

        public int hashCode()
        {
            return transactionState.hashCode();
        }

        public static MpPaxosParticipant createAwaitingForConflictResolution(TransactionState transactionState)
        {
            return new MpPaxosParticipant(Optional.<UUID>empty(), transactionState, 0);
        }

        /**
         * Is conflict either resolved as non conflicting or as actual conflict.
         */
        public boolean hasResolvedConflictWith(MpPaxosParticipant paxosParticipant) {
            return getParticipantsToRollback().contains(paxosParticipant) ||
                   containsNonConflicting(paxosParticipant.getTransactionState().getTransactionId());
        }

        private boolean containsNonConflicting(UUID transactionId)
        {
            return nonConflictingTransactionIds != null && nonConflictingTransactionIds.contains(transactionId);
        }

        public void addAsNonConflicting(UUID transactionId)
        {
            if(hasThatTransactionAlreadyCommitted(transactionId)) {
                removeThatTransactionFromAlreadyCommitted(transactionId);
            }
            if(nonConflictingTransactionIds == null)
            {
                nonConflictingTransactionIds = new HashSet<>();
            }

            nonConflictingTransactionIds.add(transactionId);
        }

        /**
         * It can be removed to save memory if it is non conflicting anyway
         * @param transactionId
         */
        private void removeThatTransactionFromAlreadyCommitted(UUID transactionId)
        {
            if(committedParticipants != null) {
                committedParticipants.remove(new CommittedParticipant(transactionId, 0L));
            }
        }

        public Set<UUID> getNonConflictingUnsafe() {
            return nonConflictingTransactionIds;
        }

        public void joinRound(UUID paxosId)
        {
            this.paxosId = Optional.of(paxosId);
        }


        public boolean hasToRollback() {
            return this.hasToRollback;
        }

        public void markThatItHasToRollback()
        {
            hasToRollback = true;
        }

        public void markItWasRolledBack()
        {
            wasRolledBack = true;
        }

        public boolean wasItRolledBack() {
            return wasRolledBack;
        }

        private synchronized void transactionHasCommittedInternal(UUID transactionId, long timestampOfCommit) {
            if(containsNonConflicting(transactionId)) {
                return;
            }
            else {
                if(committedParticipants == null)
                {
                    committedParticipants = new HashSet<>();
                }
                committedParticipants.add(new CommittedParticipant(transactionId, timestampOfCommit));
            }
        }

        public void transactionHasCommitted(UUID transactionId, long timestampOfCommit)
        {
            transactionHasCommittedInternal(transactionId, timestampOfCommit);
        }
    }


    private static MppPaxosRoundPointers createEmptyPaxosPointers()
    {
        return new MppPaxosRoundPointers(new LinkedHashSet<>());
    }

    /**
     *  Creating unique id is enough. Paxos State will be created on demand later on.
     */
    private static UUID inititatePaxosState()
    {
        final UUID paxosId = UUIDGen.getTimeUUID();

        return paxosId;
    }

    private static boolean wholeIndexIsEmpty(Map<TransactionItem, Optional<MppPaxosRoundPointers>> getIndex)
    {
        return getIndex.entrySet().stream().allMatch(e -> !e.getValue().isPresent());
    }

    public static class MppIndexResultActions
    {

        private final MpPaxosParticipant paxosParticipant;

        private Set<UUID> potentialPaxosRounds;

        private Map<TransactionItem, List<TransactionState>> conflictsToBeChecked;

        public MppIndexResultActions(Set<UUID> potentialPaxosRounds, Map<TransactionItem,
                                                                        List<TransactionState>> conflictsToBeChecked,
                                     MpPaxosParticipant paxosParticipant) {
            this.conflictsToBeChecked = copy(conflictsToBeChecked);
            this.potentialPaxosRounds = potentialPaxosRounds;
            this.paxosParticipant = paxosParticipant;
        }

        private static Map<TransactionItem, List<TransactionState>> copy(Map<TransactionItem, List<TransactionState>> conflictsToBeChecked)
        {
            Map<TransactionItem, List<TransactionState>> copied = new HashMap<>();
            conflictsToBeChecked.entrySet().forEach(e -> {
                copied.put(e.getKey(), Lists.newArrayList(e.getValue()));
            });
            return copied;
        }


        boolean hasSingleLogicalPaxosInstanceAtThisNode()
        {
            return potentialPaxosRounds.size() == 1;
        }

        public boolean needsToCheckForRollback() {
            return false;
        }

        public boolean canProceed()
        {
            return !needsToCheckForRollback() && hasSingleLogicalPaxosInstanceAtThisNode() && !hasToCheckForConflicts();
        }

        public boolean hasToCheckForConflicts()
        {
            return !getSetOfValues(conflictsToBeChecked).isEmpty();
        }

        public Map<TransactionItem, List<TransactionState>> getConflictsToBeChecked()
        {
            return conflictsToBeChecked;
        }

        public Set<UUID> getPotentialPaxosRounds()
        {
            return potentialPaxosRounds;
        }

        public MpPaxosParticipant getPaxosParticipant()
        {
            return paxosParticipant;
        }
    }

    /**
     * Should do quorum read of this transaction data that belongs to this node.
     */
    public static class CheckForRollbackResult extends MppIndexResultActions {

        public static CheckForRollbackResult result = new CheckForRollbackResult();

        public CheckForRollbackResult()
        {
            super(Collections.emptySet(), Collections.emptyMap(), null);
        }

        public boolean needsToCheckForRollback()
        {
            return true;
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
        final List<TransactionItem> ownedByThisNode = getTransactionItemsOwnedByThisNodeSorted(transactionState);

        acquireIndexForItems(transactionState, mpPaxosIndex, ownedByThisNode);
    }

    private void acquireIndexForItems(TransactionState transactionState, BiConsumer<MpPaxosIndex, List<TransactionItem>> mpPaxosIndex, List<TransactionItem> ownedByThisNode)
    {
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

    private List<TransactionItem> getTransactionItemsOwnedByThisNodeSorted(TransactionState transactionState)
    {
        final Stream<TransactionItem> sorted = getTransactionItemsOwnedByThisNode().apply(transactionState)
                                                                                   .sorted();
        return sorted.collect(Collectors.toList());
    }

    /**
     *
     * Not very elegant, but I want to unlock index as soon as possible.
     *
     */
    public MppIndexResultActions acquireIndexAndAdd(TransactionState transactionState) {
        final MppIndexResultActions[] result = new MppIndexResultActions[1];
        acquireIndex(transactionState, (index, items) -> {
            result[0] = index.addItToIndex(transactionState, items);
        });
        return result[0];
    }

    protected Function<TransactionState, Stream<TransactionItem>> getTransactionItemsOwnedByThisNode()
    {
        return TRANSACTION_ITEMS_OWNED_BY_THIS_NODE;
    }

    private static final Function<TransactionItem, LockKey> toLockKey = txItem -> new LockKey(txItem.getKsName(), txItem.getCfName(), (Long) txItem.getToken().getTokenValue());

    private static class LockKey
    {
        private LockKey(String keyspace, String cf, Long token)
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
        // TODO Proposal is TransactionState
    }

    // TODO [MPP] rename it later
    public static class MppPaxosRoundPointers
    {
        /**
         * It has to be LinkedHashSet, or at least Linked smth because assumption is
         * that if new participant is added then it is responsible to check for conflicts with all before him.
         * Insertion order is important.
         */
        private final LinkedHashSet<MpPaxosParticipant> participants;

        public MppPaxosRoundPointers(LinkedHashSet<MpPaxosParticipant> participants)
        {
            this.participants = participants;
        }

        public void addParticipant(MpPaxosParticipant paxosParticipant)
        {
            participants.add(paxosParticipant);
        }

        public Set<MpPaxosParticipant> getParticipantsUnsafe()
        {
            return participants;
        }

        public void removeParticipant(MpPaxosParticipant mpPaxosParticipant)
        {
            participants.remove(mpPaxosParticipant);
        }

        public boolean isEmpty()
        {
            return participants.isEmpty();
        }
    }
}
