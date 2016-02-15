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
import org.apache.cassandra.mpp.transaction.TransactionId;
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


//                MpPaxosRoundPointer pointer = createNewPaxosRoundPointer(paxosId, transactionState);
//                pointers.addPointer(pointer);

                getIndexUnsafe().put(entry, pointers);
            });


//            MppIndexForItemResult sameResultForAll = createIndexForItemResultWithCreatedPointer();

//            final Map<TransactionItem, MppIndexForItemResult> resultForItem = gotIndex.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toMap(Function.identity(), ti -> sameResultForAll));

            return createIndexResult(paxosId, paxosParticipant);
        }
        else
        {

//            MppPaxosRoundPointers pointers = null;
//            final List<UUID> transactionIdsToCheckForConflict = collectTransactionIdsFromParticipants(pointers);


//            final Map<TransactionItem, Set<MpPaxosParticipant>> participants = gotIndex.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
//                                                                                                                                v -> v.getValue().map(p -> p.getParticipantsUnsafe()).orElse(Collections.emptySet())));

            // Register itself in each
            MpPaxosParticipant paxosParticipant = MpPaxosParticipant.createAwaitingForConflictResolution(transactionState);
            gotIndex.forEach((entry, optionalPointers) -> {
                MppPaxosRoundPointers pointers = optionalPointers.orElse(createEmptyPaxosPointers());
                pointers.addParticipant(paxosParticipant);
                // Add to index
                getIndexUnsafe().put(entry, pointers);
            });

            return reCheckTransactionParticipant(paxosParticipant);

//            final Map<TransactionItem, List<MpPaxosParticipant>> participantsUntilThisOne = getParticipantsUntil(gotIndex, paxosParticipant);
//
//
//            final Set<MpPaxosParticipant> allOtherPotentiallyConflictingParticipants = participantsUntilThisOne.entrySet().stream().map(Map.Entry::getValue).flatMap(Collection::stream).collect(Collectors.toSet());
//
//
//            paxosParticipant.setConflictsToResolve(allOtherPotentiallyConflictingParticipants.size());
//
//
//            // Could be 0, 1 or more
//            final Set<UUID> potentialNumberOfPaxosRounds = getAllPaxosRounds(participantsUntilThisOne);
//
//            final Map<TransactionItem, List<TransactionState>> participantsToTransactionStates = participantsToTransactionStates(participantsUntilThisOne);
//
//            MppIndexResultActions result = new MppIndexResultActions(potentialNumberOfPaxosRounds, participantsToTransactionStates);
//
//            return result;


            // These pointers are not defined, but some others are. Need to add itself to awaiting candidates and to "check for conflict" for all other present
//            final Stream<Map.Entry<TransactionItem, Optional<MppPaxosRoundPointers>>> pointersToAddForAwaiting = gotIndex.entrySet().stream().filter(p -> !p.getValue().isPresent());

            // Pointers are defined. They can be either:
            // 1) Just awaiting and no rounds
            // 2) Just rounds (at least 1 round)

            // IF transaction can start it's own round, and moves transactions from awaiting

//            final Stream<Map.Entry<TransactionItem, Optional<MppPaxosRoundPointers>>> definedPointersToAddToCheckForConflicts = gotIndex.entrySet().stream().filter(p -> p.getValue().isPresent());
//
//            definedPointersToAddToCheckForConflicts.map(entry -> {
//                final TransactionItem ti = entry.getKey();
//                final MppPaxosRoundPointers pointers = entry.getValue().get();
//
//                Preconditions.checkState(pointers.areEmpty(), "Pointers should not be totally");
//
//                return 5;
//            });
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

    private Map<TransactionItem, List<MpPaxosParticipant>> filterParticipants(Map<TransactionItem, List<MpPaxosParticipant>> participantsMap, Predicate<MpPaxosParticipant> predicate)
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

        public RemoveParticipantsFromIndex(Stream<MpPaxosParticipant> participants, Stream<TransactionItem> items)
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
        paxosParticipant.resolveConflict();

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

    private MppIndexResultActions createIndexResult(UUID paxosId, MpPaxosParticipant paxosParticipant)
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

    public static class HasToRollbackMpPaxosParticipantException extends RuntimeException {

        private final MpPaxosParticipant paxosParticipantToRollback;

        public HasToRollbackMpPaxosParticipantException(MpPaxosParticipant paxosParticipantToRollback)
        {
            this.paxosParticipantToRollback = paxosParticipantToRollback;
        }
    }

    public static class MpPaxosParticipant {
        private Optional<UUID> paxosId;

        private final TransactionState transactionState;

        private Set<MpPaxosParticipant> rollbackTheseParticipantsOnCommit;

        private int conflictsToResolve;

        private Set<UUID> nonConflictingTransactionIds;

        private Set<CommittedParticipant> committedParticipants;

        private volatile boolean hasToRollback = false;
        private volatile boolean wasRolledBack = false;

        public MpPaxosParticipant(Optional<UUID> paxosId, TransactionState transactionState, int conflictsToResolve)
        {
            this.paxosId = paxosId;
            this.transactionState = transactionState;
            this.conflictsToResolve = conflictsToResolve;
        }

        public MpPaxosParticipant createInheritor(TransactionState otherTransaction) {
            Preconditions.checkState(paxosId.isPresent(), "It can create inheritor if this one participates in paxos");

            return new MpPaxosParticipant(paxosId, otherTransaction, 0);
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

        public synchronized boolean conflictsAreResolved() {
            return conflictsToResolve == 0;
        }

        public synchronized void resolveConflict()
        {
            conflictsToResolve -= 1;
        }

        public void setConflictsToResolve(int conflictsToResolve)
        {
            Preconditions.checkState(this.conflictsToResolve == 0, "only after initialization");
            this.conflictsToResolve = conflictsToResolve;
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

//    private MppIndexResultActions createIndexResult(int numberOfPossiblePaxosRounds, Map<TransactionItem, MppIndexForItemResult> perItemResults)
//    {
//        return new MppIndexResultActions(numberOfPossiblePaxosRounds, perItemResults);
//    }

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

        private final MpPaxosParticipant paxosParticipant;

        private Set<UUID> potentialPaxosRounds;

//        private int numberOfRoundsItBelongs;

//        private Map<TransactionItem, MppIndexForItemResult> results;

        private Map<TransactionItem, List<TransactionState>> conflictsToBeChecked;


//        public MppIndexResultActions(int numberOfRoundsItBelongs, Map<TransactionItem, MppIndexForItemResult> results)
//        {
//            this.numberOfRoundsItBelongs = numberOfRoundsItBelongs;
//            this.results = results;
//        }

        public MppIndexResultActions(Set<UUID> potentialPaxosRounds, Map<TransactionItem,
                                                                        List<TransactionState>> conflictsToBeChecked,
                                     MpPaxosParticipant paxosParticipant) {
            this.conflictsToBeChecked = copy(conflictsToBeChecked);
            this.potentialPaxosRounds = potentialPaxosRounds;
            this.paxosParticipant = paxosParticipant;
        }

        private Map<TransactionItem, List<TransactionState>> copy(Map<TransactionItem, List<TransactionState>> conflictsToBeChecked)
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

//        public Map<TransactionItem, MppIndexForItemResult> getResults()
//        {
//            return results;
//        }

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

    // TODO [MPP] rename it later
    public static class MppPaxosRoundPointers
    {
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

    public static class MppPaxosRoundPointersOld
    {

        /**
         * Here are transactions that need to wait for other
         * transactions to complete before they can start paxos round on their own.
         */
        private final Collection<TransactionState> awaitingCandidates;

        private final Collection<MpPaxosRoundPointer> paxosRounds;

        public MppPaxosRoundPointersOld(Collection<TransactionState> awaitingCandidates, Collection<MpPaxosRoundPointer> paxosRounds)
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
