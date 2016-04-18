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

import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.MultiPartitionPaxosTimeoutException;
import org.apache.cassandra.exceptions.TransactionRolledBackException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.metrics.CASClientRequestMetrics;
import org.apache.cassandra.mpp.MppServicesLocator;
import org.apache.cassandra.mpp.transaction.TransactionId;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.paxos.MpPaxosId;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MppCommitWriteResponseHandler;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.mppaxos.MpCommit;
import org.apache.cassandra.service.mppaxos.MpCommitWithHints;
import org.apache.cassandra.service.mppaxos.MpPrePrepareMpPaxosCallback;
import org.apache.cassandra.service.mppaxos.MpPrepareCallback;
import org.apache.cassandra.service.mppaxos.MpProposeCallback;
import org.apache.cassandra.service.mppaxos.ReplicasGroupsOperationCallback;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 04/04/16
 */
public class StorageProxyMpPaxosExtensions
{
    private static final CASClientRequestMetrics casWriteMetrics = new CASClientRequestMetrics("MPPCASWrite");

    private static final CASClientRequestMetrics casReadMetrics = new CASClientRequestMetrics("MPPCASRead");

    private static final Logger logger = LoggerFactory.getLogger(StorageProxyMpPaxosExtensions.class);

    private static final Map<Phase, Phase> nextPhaseIs;

    public static Phase getNextExpectedPhase(Phase current)
    {
        return nextPhaseIs.get(current);
    }

    public static class CommonStateHolder {
        private final Set<TransactionId> acquiredTransactionIdsForRepair = new HashSet<>();

        private final Set<TransactionId> transactionIdsForWhichRepairCompleted = new HashSet<>();

        private final Lock lock = new ReentrantLock();

        boolean acquireTransactionForRepair(TransactionId txId) {
            lock.lock();
            try {
                if(transactionIdsForWhichRepairCompleted.contains(txId)) {
                    // Transaction was already repaired and repair finished.
                    return false;
                }
                if(acquiredTransactionIdsForRepair.contains(txId)) {
                    // Transaction TxID was already acquired by someone else.
                    return false;
                }
                else {
                    acquiredTransactionIdsForRepair.add(txId);
                    return true;
                }

            }
            finally {
                lock.unlock();
            }
        }

        void transactionWithIdHasDoneRepairing(TransactionId txId) {
            lock.lock();
            try {
                Preconditions.checkState(!transactionIdsForWhichRepairCompleted.contains(txId), "Apparently this transaction was already repaired by some one else, but it should be only acquired by single executor");
                transactionIdsForWhichRepairCompleted.add(txId);
            }
            finally {
                lock.unlock();
            }
        }

        boolean wasAlreadyRepaired(TransactionId txId) {
            lock.lock();
            try {
                return transactionIdsForWhichRepairCompleted.contains(txId);
            }
            finally {
                lock.unlock();
            }
        }
    }

    public static class ReplicaInPhaseHolder
    {
        private ReplicaInPhase replicaInPhase;
        private TransitionResult transitionResult;
        private CommonStateHolder commonState;

        public ReplicaInPhaseHolder(ReplicaInPhase replicaInPhase, CommonStateHolder commonState)
        {
            this.replicaInPhase = replicaInPhase;
            this.commonState = commonState;
        }

        public ReplicaInPhase getReplicaInPhase()
        {
            return replicaInPhase;
        }

        public void setReplicaInPhase(ReplicaInPhase replicaInPhase)
        {
            this.replicaInPhase = replicaInPhase;
        }

        public void setTransitionResult(TransitionResult transitionResult)
        {
            this.transitionResult = transitionResult;
        }

        public TransitionResult getTransitionResult()
        {
            return transitionResult;
        }

        public CommonStateHolder getCommonState()
        {
            return commonState;
        }
    }

    interface ReplicaInPhase
    {
        TransactionState getTransactionState();

        Replica getReplica();

        ClientState getState();

        Optional<MpPaxosId> getPaxosId();
    }

    static abstract class AbstractReplicaInPhase implements ReplicaInPhase
    {
        protected TransactionState transactionState;
        protected ClientState state;
        protected Replica replica;

        protected AbstractReplicaInPhase(TransactionState transactionState, ClientState state, Replica replica)
        {
            this.transactionState = transactionState;
            this.state = state;
            this.replica = replica;
        }


        @Override
        public TransactionState getTransactionState()
        {
            return transactionState;
        }

        @Override
        public ClientState getState()
        {
            return state;
        }

        @Override
        public Replica getReplica()
        {
            return replica;
        }

        @Override
        public Optional<MpPaxosId> getPaxosId()
        {
            return Optional.empty();
        }
    }

    public static void setPhaseOnReplicas(ReplicasGroupAndOwnedItems replicaGroup, Phase phase)
    {
        replicaGroup.getReplicasGroup().getAllReplicasInThatGroup().forEach(replica -> replica.setPhase(phase));
    }

    public static Phase findMinimumPhase(ReplicasGroup replicasGroup)
    {
        return replicasGroup.getAllReplicasInThatGroup().stream().map(Replica::getPhase).sorted().findFirst().get();
    }

    public static Optional<Phase> getPhaseOfQuorum(ReplicasGroup replicaGroup)
    {
        List<Replica> allReplicasInThatGroup = replicaGroup.getAllReplicasInThatGroup();
        int size = allReplicasInThatGroup.size();
        long quorum = size / 2 + 1;
        Map<Phase, Long> countByPhase = allReplicasInThatGroup.stream().map(Replica::getPhase).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        return countByPhase.entrySet().stream().filter(e -> e.getValue() >= quorum).map(Map.Entry::getKey).findFirst();
    }


    public static Phase getMaximumPhaseSharedByQuorum(ReplicasGroup replicasGroup)
    {
        List<Replica> allReplicasInThatGroup = replicasGroup.getAllReplicasInThatGroup();
        return getMaximumPhaseSharedByQuorum(allReplicasInThatGroup.stream().map(x -> x).collect(Collectors.toList()));
    }

    public static Phase getMaximumPhaseSharedByQuorum(Collection<WithPhase> withPhases)
    {
        int size = withPhases.size();
        if(size == 2) {
            return withPhases.stream().map(WithPhase::getPhase).sorted().findFirst().get();
        }
        else {
            int quorum = size / 2 + 1;
            List<Phase> collected = withPhases.stream().map(WithPhase::getPhase).sorted().collect(Collectors.toList());
            return collected.get(quorum - 1);
        }
    }

    public static Phase findNextPhaseForReplica(Replica replica, Phase minimumPhaseAmongAllReplicaGroups, Phase nextExpectedPhase)
    {
        if (replica.getPhase() == nextExpectedPhase)
        {
            return replica.getPhase();
        }
        else if (replica.getPhase() == minimumPhaseAmongAllReplicaGroups)
        {
            return nextExpectedPhase;
        }
        else
        {
            return getNextExpectedPhase(replica.getPhase());
        }
    }

    public static String phasesForReplicaGroup(ReplicasGroup rg) {
        return "ReplicaGroup: \t" + rg.getAllReplicasInThatGroup().stream().map(replica -> {
          return replica.getHostAddress() + " -> " + replica.getPhase().toString() + "\t";
        }).reduce((s1,s2) -> s1 + s2).get();
    }

    public static String phasesForReplicaGroups(Collection<ReplicasGroup> replicaGroups) {
        return replicaGroups.stream().map(rg -> phasesForReplicaGroup(rg)).reduce((s1,s2) -> s1 + s2).get();
    }

    public static Phase findNextPhaseForReplica(Replica replica, Collection<ReplicasGroup> replicaGroups)
    {
        Phase minimumPhaseAmongAllReplicaGroups = findMinimumPhaseAmongAllReplicaGroups(replicaGroups);
        if(minimumPhaseAmongAllReplicaGroups.ordinal() < replica.getPhase().ordinal()) {
            return getNextExpectedPhase(minimumPhaseAmongAllReplicaGroups);
        }

        Phase nextExpectedPhase = getNextExpectedPhase(minimumPhaseAmongAllReplicaGroups);
        Phase nextPhaseForReplica = findNextPhaseForReplica(replica, minimumPhaseAmongAllReplicaGroups, nextExpectedPhase);
        logger.debug("Replica {} is in phase {} and minimum phase among quorum is {}. Computed next phase to be {} Phases for replica groups are {}", replica.getHostAddress(),
                     replica.getPhase(), minimumPhaseAmongAllReplicaGroups, nextPhaseForReplica, phasesForReplicaGroups(replicaGroups));

        // This doesn't work well, it is too simple because phases in enum and their ordinals do not match with expected path transitions. Rollback causes trouble.
//        if(nextPhaseForReplica.ordinal() > minimumPhaseAmongAllReplicaGroups.ordinal() + 1) {
//            logger.error("Replica {} skips too many phases Replica phase {}. minimum among quorum {} next for replica {} ERROR Replica phase",
//                         replica.getHostAddress(),
//                         replica.getPhase(),
//                         minimumPhaseAmongAllReplicaGroups,
//                         nextExpectedPhase);
//        }

        return nextPhaseForReplica;
    }

    public static TransitionId findNextTransitionId(Replica replica, Phase nextPhaseForReplica)
    {
        return transitionToPhase(replica.getPhase(), nextPhaseForReplica);
    }

    public static class BaseReplicaInPhase extends AbstractReplicaInPhase
    {

        protected BaseReplicaInPhase(TransactionState transactionState, ClientState state, Replica replica, Phase phase)
        {
            super(transactionState, state, replica);
            if (phase != null)
                replica.setPhase(phase);
        }
    }

    public static class ReplicaInPrePreparedPhase extends AbstractReplicaInPhase
    {

        /**
         * This is response for PRE PREPARED PHASE.
         */
        Optional<MpPaxosId> paxosIdOpt;

        MpPrepareCallback summary;

        public ReplicaInPrePreparedPhase(TransactionState transactionState, ClientState state, Replica replica,
                                         Map<InetAddress, Optional<MpPaxosId>> responsesByReplica, MpPrepareCallback summary)
        {
            super(transactionState, state, replica);
            Preconditions.checkArgument(responsesByReplica.size() == 1);
            Preconditions.checkArgument(responsesByReplica.entrySet().iterator().next().getKey().getHostAddress().equals(replica.getHostAddress()));
            this.paxosIdOpt = responsesByReplica.entrySet().iterator().next().getValue();
            this.summary = summary;
        }

        private ReplicaInPrePreparedPhase(TransactionState transactionState, ClientState state, Replica replica,
                                          Optional<MpPaxosId> paxosId, MpPrepareCallback summary)
        {
            super(transactionState, state, replica);
            this.paxosIdOpt = paxosId;
            this.summary = summary;
        }

        public ReplicaInPrePreparedPhase copyWith(MpPrepareCallback summary)
        {
            return new ReplicaInPrePreparedPhase(transactionState, state, replica, paxosIdOpt, summary);
        }

        @Override
        public Optional<MpPaxosId> getPaxosId()
        {
            return paxosIdOpt;
        }

        public MpPrepareCallback getSummary()
        {
            return summary;
        }
    }

    public static class ReplicaInPreparedPhase extends ReplicaInPrePreparedPhase
    {

        private final UUID ballot;

        public boolean timeOutProposalIfPartialResponses;

        private Optional<TransactionId> txRepairedAlready = Optional.empty();

        protected ReplicaInPreparedPhase(TransactionState transactionState,
                                         ClientState state,
                                         Replica replica,
                                         Optional<MpPaxosId> paxosId,
                                         MpPrepareCallback summary,
                                         UUID ballot,
                                         boolean timeOutProposalIfPartialResponses)
        {
            super(transactionState, state, replica, paxosId, summary);
            this.ballot = ballot;
            this.timeOutProposalIfPartialResponses = timeOutProposalIfPartialResponses;
        }

        protected ReplicaInPreparedPhase(TransactionState transactionState,
                                         ClientState state,
                                         Replica replica,
                                         Optional<MpPaxosId> paxosId,
                                         MpPrepareCallback summary,
                                         UUID ballot,
                                         boolean timeOutProposalIfPartialResponses,
                                         Optional<TransactionId> txRepairedAlready)
        {
            this(transactionState, state, replica, paxosId, summary, ballot, timeOutProposalIfPartialResponses);
            this.txRepairedAlready = txRepairedAlready;
        }

        public UUID getBallot()
        {
            return ballot;
        }
    }

    public static class ReplicaInReparingInProgressPhase extends ReplicaInPreparedPhase
    {

        ReplicaGroupsPhaseExecutor repairInProgressPaxosExecutor;

        volatile boolean isRepairDone = false;

        protected ReplicaInReparingInProgressPhase(TransactionState transactionState,
                                                   ClientState state,
                                                   Replica replica,
                                                   Optional<MpPaxosId> paxosId,
                                                   MpPrepareCallback summary,
                                                   UUID ballot,
                                                   boolean timeOutProposalIfPartialResponses,
                                                   ReplicaGroupsPhaseExecutor repairInProgressPaxosExecutor)
        {
            super(transactionState, state, replica, paxosId, summary, ballot, timeOutProposalIfPartialResponses);
            this.repairInProgressPaxosExecutor = repairInProgressPaxosExecutor;
        }
    }

    public static class ReplicaWaitsTillRepairIsDone extends ReplicaInPreparedPhase
    {
        private final TransactionId waitTillThisRepairIsDone;

        protected ReplicaWaitsTillRepairIsDone(TransactionState transactionState,
                                                   ClientState state,
                                                   Replica replica,
                                                   Optional<MpPaxosId> paxosId,
                                                   MpPrepareCallback summary,
                                                   UUID ballot,
                                                   boolean timeOutProposalIfPartialResponses,
                                                   TransactionId waitTillThisRepairIsDone)
        {
            super(transactionState, state, replica, paxosId, summary, ballot, timeOutProposalIfPartialResponses);
            this.waitTillThisRepairIsDone = waitTillThisRepairIsDone;
        }
    }

    public enum TransitionId
    {
        NOOP, TO_PRE_PREPARED, TO_PREPARED, TO_PROPOSED, TO_COMMITTED, TO_PREPARED_FROM_REPARING,
    }

    public static Transition findTransitionByTransitionId(TransitionId transitionId)
    {
        if (transitionId == TransitionId.NOOP) return noopTransition;
        else if (transitionId == TransitionId.TO_PRE_PREPARED) return transitionToPrePrepared;
        else if (transitionId == TransitionId.TO_PREPARED) return transitionToPrepared;
        else if (transitionId == TransitionId.TO_PREPARED_FROM_REPARING) return transitionToPreparedFromReparing;
        else if (transitionId == TransitionId.TO_COMMITTED) return transitionToCommitted;
        else if (transitionId == TransitionId.TO_PROPOSED) return transitionToProposed;
        else
        {
            throw new RuntimeException("Unknown transition id " + transitionId);
        }
    }

    interface Transition
    {
        TransitionResult transition(ReplicaInPhase replicaInPhase);
    }

    interface TransitionResult extends WithPhase
    {
        boolean wasTransitionSuccessful();

        Phase phaseAfterTransition();

        ReplicaInPhase getReplicaInPhase();
    }

    public static class TransitionResultImpl implements TransitionResult
    {

        private final boolean wasTransitionSuccessful;
        private final Phase phaseAfterTransition;
        private final ReplicaInPhase replicaInPhase;

        public TransitionResultImpl(ReplicaInPhase replicaInPhase, Phase phaseAfterTransition, boolean wasTransitionSuccessful)
        {
            this.wasTransitionSuccessful = wasTransitionSuccessful;
            this.phaseAfterTransition = phaseAfterTransition;
            this.replicaInPhase = replicaInPhase;
        }


        public boolean wasTransitionSuccessful()
        {
            return wasTransitionSuccessful;
        }

        public Phase phaseAfterTransition()
        {
            return phaseAfterTransition;
        }

        public ReplicaInPhase getReplicaInPhase()
        {
            return replicaInPhase;
        }

        public Phase getPhase()
        {
            return phaseAfterTransition();
        }
    }


    public static ReplicaGroupsPhaseExecutor createMultiPartitionPaxosPhaseExecutor(List<ReplicasGroupAndOwnedItems> replicaGroups, TransactionState transactionState, ClientState state)
    {
        Phase noPhase = Phase.NO_PHASE;
        Collection<Replica> allReplicas = replicaGroups.iterator().next().getAllReplicas();
        CommonStateHolder commonState = new CommonStateHolder();
        allReplicas.forEach(replica -> {
            ReplicaInPhaseHolder holder = new ReplicaInPhaseHolder(new BaseReplicaInPhase(transactionState, state, replica, noPhase), commonState);
            replica.setHolder(holder);
        });
        logger.debug("createMultiPartitionPaxosPhaseExecutor replicaGroups {}. TransactionState is {}", replicaGroups, transactionState);
        List<ReplicasGroup> inPhases = replicaGroups.stream()
                                                                .map(ReplicasGroupAndOwnedItems::getReplicasGroup)
                                                                .collect(Collectors.toList());

        return new ReplicaGroupsPhaseExecutor(transactionState, Phase.AFTER_COMMIT_PHASE, inPhases, replicaGroups.iterator().next().getAllReplicas());
    }

    public static ReplicaGroupsPhaseExecutor createMultiPartitionPaxosPhaseExecutorForRead(List<ReplicasGroupAndOwnedItems> replicaGroups, TransactionState transactionState, ClientState state)
    {
        /**
         *      This transaction does not propose any values.
                It has no data
                Does preprepare phase makes sense?
                - making data consistent - No
                - joing paxos round - Yes
                - creating paxos round - No     -
         */
        Phase startFromPrePreparePhase = Phase.PRE_PREPARE_PHASE;
        Collection<Replica> allReplicas = replicaGroups.iterator().next().getAllReplicas();
        CommonStateHolder commonState = new CommonStateHolder();
        allReplicas.forEach(replica -> {
            replica.setPhase(startFromPrePreparePhase);
            ReplicaInPhaseHolder holder = new ReplicaInPhaseHolder(new ReplicaInPrePreparedPhase(transactionState, state, replica, Optional.<MpPaxosId>empty(), null), commonState);
            replica.setHolder(holder);
        });
        logger.debug("createMultiPartitionPaxosPhaseExecutor replicaGroups {}. TransactionState is {}", replicaGroups, transactionState);
        List<ReplicasGroup> inPhases = replicaGroups.stream()
                                                    .map(ReplicasGroupAndOwnedItems::getReplicasGroup)
                                                    .collect(Collectors.toList());

        // Reading should finish after it successfully transitions into prepare phase, that happens after repair phase has finished.
        return new ReplicaGroupsPhaseExecutor(transactionState, Phase.PREPARE_PHASE, inPhases, replicaGroups.iterator().next().getAllReplicas());
    }

    public static ReplicaGroupsPhaseExecutor createRepairInProgressPaxosExecutor(TransactionState inProgressTransactionState, ClientState state, UUID ballot)
    {
        logger.debug("createRepairInProgressPaxosExecutor for TxId {} using ballot {}", inProgressTransactionState.getTransactionId(), ballot);
        List<ReplicasGroupAndOwnedItems> replicasGroupAndOwnedItems = ForEachReplicaGroupOperations.groupItemsByReplicas(inProgressTransactionState);
        Map<InetAddress, Optional<MpPaxosId>> responsesByReplica = Collections.emptyMap(); // TODO [MPP] Maybe it will work.
        CommonStateHolder commonState = new CommonStateHolder();
        Collection<Replica> allReplicas = replicasGroupAndOwnedItems.iterator().next().getAllReplicas();
        allReplicas.forEach(replica -> {
            ReplicaInPhaseHolder holder = new ReplicaInPhaseHolder(new ReplicaInPreparedPhase(inProgressTransactionState, state, replica, Optional.empty(), null, ballot, false), commonState);
            replica.setHolder(holder);
            replica.setPhase(Phase.PREPARE_PHASE);
        });

//        List<ReplicaGroupInPhaseHolder> inPhases = replicasGroupAndOwnedItems.stream().map(rg -> {
//            Pair<List<InetAddress>, Integer> p = getPaxosParticipants(rg, consistencyForPaxos);
//            List<InetAddress> liveEndpoints = p.left;
//            int requiredParticipants = p.right;
//            // TODO [MPP] This can be later deleted or at least simplified.
//            return new ReplicaGroupInPreparedPhase(rg, inProgressTransactionState, state, responsesByReplica, null, liveEndpoints, requiredParticipants, ballot, false);
//        }).map(ReplicaGroupInPhaseHolder::new)
//                                                                             .collect(Collectors.toList());

        List<ReplicasGroup> replicaGroups = replicasGroupAndOwnedItems.stream()
                                                    .map(ReplicasGroupAndOwnedItems::getReplicasGroup)
                                                    .collect(Collectors.toList());

        // TODO [MPP] It might be done if we see that transaction was rolled back.
        ReplicaGroupsPhaseExecutor replicaGroupsPhaseExecutor = new ReplicaGroupsPhaseExecutor(inProgressTransactionState, Phase.AFTER_COMMIT_PHASE, replicaGroups, replicasGroupAndOwnedItems.iterator().next().getAllReplicas());
        replicaGroupsPhaseExecutor.setIsRepairing(true);
        return replicaGroupsPhaseExecutor;
    }

    public static Phase findMinimumPhaseAmongAllReplicaGroups(Collection<ReplicasGroup> replicaGroups)
    {
        return replicaGroups.stream().map(StorageProxyMpPaxosExtensions::getMaximumPhaseSharedByQuorum).sorted().findFirst().get();
    }

    public enum PhaseExecutorResult {
        FINISHED, ROLLED_BACK;
    }

    public static class ReplicaGroupsPhaseExecutor
    {

        private final Collection<Replica> replicas;
        private final TransactionState transactionState;
        Phase isDoneWhen;

        Collection<ReplicasGroup> replicasInPhase;

        private PhaseExecutorResult phaseExecutorResult = null;

        private boolean isRepairing = false;

        public ReplicaGroupsPhaseExecutor(TransactionState transactionState, Phase isDoneWhen, Collection<ReplicasGroup> replicasInPhase, Collection<Replica> allReplicas)
        {
            this.isDoneWhen = isDoneWhen;
            this.replicasInPhase = replicasInPhase;
            this.replicas = allReplicas;
            this.transactionState = transactionState;
        }

        public static Phase getCurrentPhaseOfReplicaGroup(ReplicasGroup rg) {
            return getMaximumPhaseSharedByQuorum(rg);
        }

        boolean areInSamePhase()
        {
            return replicasInPhase.stream().map(ReplicaGroupsPhaseExecutor::getCurrentPhaseOfReplicaGroup).distinct().count() == 1;
        }

        /**
         * All replica groups MUST be in phase with replica group with minimum phase.
         *
         * @return
         */
        Phase findMinimumPhaseAmongAllReplicaGroups()
        {
            return replicasInPhase.stream().map(ReplicaGroupsPhaseExecutor::getCurrentPhaseOfReplicaGroup).sorted().findFirst().get();
        }

        boolean isDone()
        {
            return areInSamePhase() && (findMinimumPhaseAmongAllReplicaGroups() == isDoneWhen);
        }

        /**
         *
         * @return {@code true if has to rollback}
         */
        boolean runTransitionsForEachReplica()
        {
            Collection<ReplicasGroup> replicaGroups = replicasInPhase;

            logger.debug("Will run transitions for each replica. ReplicaGroups size: {}", replicaGroups.size());
            // TODO [MPP] This parallel stream might be a problem.
            replicas.stream().forEach(replica -> {
                Phase nextPhaseForReplica = findNextPhaseForReplica(replica, replicaGroups);
                Phase currentPhase = replica.getPhase();
                TransitionId transitionId = transitionToPhase(currentPhase, nextPhaseForReplica);
                logger.debug("Replica {} is in phase '{}' and next expected phase is {}. It will transition using {}",
                             replica.getHostAddress(),
                             currentPhase,
                             nextPhaseForReplica,
                             transitionId);

                Transition transitionForReplica = findTransitionByTransitionId(transitionId);
                ReplicaInPhaseHolder holder = replica.getReplicaInPhaseHolder();
                ReplicaInPhase replicaInPhase = holder.getReplicaInPhase();
                // Run transition on replica
                try {
                    TransitionResult transitionResult = transitionForReplica.transition(replicaInPhase);
                    holder.setTransitionResult(transitionResult);
                }
                catch (WriteTimeoutException timeout) {
                    logger.error("Timeout occurred for replica {}. It will transition it to same phase as before: {}", replica.getHostAddress(), currentPhase, timeout);
                    holder.setTransitionResult(new TransitionResultImpl(replica.getReplicaInPhaseHolder().getReplicaInPhase(), currentPhase, false));
                }
                catch (ClassCastException classCastException) {
                    logger.error("ClassCastException caught. Replica is {} currentPhase {}, " +
                                 "nextPhaseForReplica is {}, replica in phase class is {}",
                                 replica.getHostAddress(), currentPhase,
                                 nextPhaseForReplica,
                                 replicaInPhase.getClass().toString(),
                                 classCastException);
                    throw classCastException;
                }
            });

            logger.debug("Transitions for replica groups are done. Time to evaluate responses");

            // For each replica group
            Map<Replica, Phase> replicaToProposedPhase = new HashMap<>();

            // After transition, some replicas could not transition forward. Their phase must be accepted.
            replicas.forEach(replica -> {
                TransitionResult transitionResult = replica.getReplicaInPhaseHolder().getTransitionResult();
                if (!transitionResult.wasTransitionSuccessful())
                {
                    logger.debug("Replica {} failed to transition and will transition to phase {}", replica, transitionResult.phaseAfterTransition());
                    replica.getReplicaInPhaseHolder().setReplicaInPhase(transitionResult.getReplicaInPhase());
                    replica.setPhase(transitionResult.phaseAfterTransition());
                }
                else { // transition was successful.
                    replicaToProposedPhase.put(replica, transitionResult.phaseAfterTransition());
                }
            });



            boolean existsReplicaGroupWhichHasQuorumOfRollbacks = replicasInPhase.stream().map(replicaGroupInPhaseHolder -> {
                ReplicasGroup replicasGroup = replicaGroupInPhaseHolder;

                List<Replica> allReplicasInThatGroup = replicasGroup.getAllReplicasInThatGroup();

                List<TransitionResult> transitionResults = allReplicasInThatGroup.stream().map(Replica::getReplicaInPhaseHolder).map(x -> x.getTransitionResult()).collect(Collectors.toList());
                return getMaximumPhaseSharedByQuorum(transitionResults.stream().map(x -> (WithPhase) x).collect(Collectors.toList()));
            }).filter(Phase.ROLLBACK_PHASE::equals).findAny().isPresent();

            if(existsReplicaGroupWhichHasQuorumOfRollbacks) {
                // TODO [MPP] If there is a replica group from which quorum of replicas transitioned into rollback phase, then we need to rollback whole transaction.
                return true;
            }
            else {
                // TODO [MPP] Continue as before
                replicasInPhase.forEach(replicaGroupInPhaseHolder -> {
                    ReplicasGroup replicasGroup = replicaGroupInPhaseHolder;

                    List<Replica> allReplicasInThatGroup = replicasGroup.getAllReplicasInThatGroup();
                    if(allReplicasInThatGroup.size() == 2) {
                        logger.debug("2ReplicasInGroup {}  TxId {} Phase of replica groups {}", allReplicasInThatGroup, transactionState.getTransactionId(), phasesForReplicaGroups(replicaGroups));
                    }

                    List<TransitionResult> transitionResults = allReplicasInThatGroup.stream().map(Replica::getReplicaInPhaseHolder).map(x -> x.getTransitionResult()).collect(Collectors.toList());
                    Phase quorumPhaseAfterTransition = getMaximumPhaseSharedByQuorum(transitionResults.stream().map(x -> (WithPhase) x).collect(Collectors.toList()));

                    List<Replica> successfullyTransitioned = allReplicasInThatGroup.stream().filter(replica -> replica.getReplicaInPhaseHolder().getTransitionResult().wasTransitionSuccessful()).collect(Collectors.toList());

                    // Phase of replica that successfully transitioned will be set to minimum quorum phase among replica groups it belongs to
                    successfullyTransitioned.forEach(replica -> {
                        Phase phase = replicaToProposedPhase.get(replica); // phase after successful transition
                        // if this phase is further than what quorum says, then this replica must "stay back".
                        if (phase.ordinal() > quorumPhaseAfterTransition.ordinal())
                        {
                            replicaToProposedPhase.put(replica, quorumPhaseAfterTransition);
                        }
                    });
                });


                replicaToProposedPhase.forEach((replica, phase) -> {
                    TransitionResult transitionResult = replica.getReplicaInPhaseHolder().getTransitionResult();

                    replica.getReplicaInPhaseHolder().setReplicaInPhase(transitionResult.getReplicaInPhase());
                    replica.setPhase(phase);
                });
            }

            // After sorting out transition results, clear them
            replicas.forEach(replica -> {
                replica.getReplicaInPhaseHolder().setTransitionResult(null);
            });

            return false;
        }

        public void tryToExecute()
        {
            UUID txId = transactionState.getTransactionId();
            logger.debug("ReplicaGroupsPhaseExecutor begins. Number of replica groups {}. Replica groups are: {}", replicasInPhase.size(), replicasInPhase);

            final long start = System.nanoTime();
            consistencyForPaxos.validateForCas();
            long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getMppContentionTimeout());
            while (System.nanoTime() - start < timeout)
            {
                Phase minimumPhaseAmongAllReplicaGroups = findMinimumPhaseAmongAllReplicaGroups();
                logger.debug("Current groups minimum phase is {}", minimumPhaseAmongAllReplicaGroups);

                // for testing purposes, it stops execution after transaction was proposed leaving it saved as "in progress"
                if(Phase.PROPOSE_PHASE == minimumPhaseAmongAllReplicaGroups &&
                   txForTestingInProgress(transactionState)) {
                    phaseExecutorResult = PhaseExecutorResult.FINISHED;
                    return;
                }

                boolean hasToRollback = runTransitionsForEachReplica();

                if(hasToRollback)
                {
                    logger.debug("Phase executor for transaction {} has to roll back", txId);
                    phaseExecutorResult = PhaseExecutorResult.ROLLED_BACK;
                    return;
                }

                if (isDone())
                {
                    phaseExecutorResult = PhaseExecutorResult.FINISHED;
                    return;
                }

                Phase phaseAfterTransistion = findMinimumPhaseAmongAllReplicaGroups();
                logger.debug("Minimum phase after transitions is {} TxID {}", phaseAfterTransistion, txId);
                if (phaseAfterTransistion.ordinal() <= minimumPhaseAmongAllReplicaGroups.ordinal())
                {
                    logger.debug("Phase after transition is not the next phase. Will try again after sleep");
                    // Something didn't work well. Try again.
                    Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
                }
            }
            logger.error("MultiPartitionPaxosTimeoutException timeout occurred. TxID {}", txId);
            throw new MultiPartitionPaxosTimeoutException("Timeout occured. TxID is: " + txId);
            //throw new WriteTimeoutException(WriteType.CAS, consistencyForPaxos, 0, 0);
        }

        private boolean txForTestingInProgress(TransactionState transactionState)
        {
            return !isRepairing && transactionState.getTransactionItems().size() == 1 && "stop_after_proposed".equals(transactionState.getTransactionItems().iterator().next().getCfName());
        }

        public PhaseExecutorResult getPhaseExecutorResult()
        {
            return phaseExecutorResult;
        }

        public void setIsRepairing(boolean isRepairing)
        {
            this.isRepairing = isRepairing;
        }

        public boolean isRepairing()
        {
            return isRepairing;
        }

        public void setRepairing(boolean isRepairing)
        {
            this.isRepairing = isRepairing;
        }
    }

    public static ConsistencyLevel consistencyForCommit = ConsistencyLevel.QUORUM;
    public static ConsistencyLevel consistencyForPaxos = ConsistencyLevel.LOCAL_TRANSACTIONAL;

    public static Transition noopTransition = replicaGroup -> new TransitionResultImpl(replicaGroup, replicaGroup.getReplica().getPhase(), true);

    public static Transition transitionToPrePrepared = replicaInPhase -> {
        Preconditions.checkState(replicaInPhase.getReplica().getPhase() == Phase.NO_PHASE);
        // TODO [MPP] Logic from MppServicesLocator should be moved elsewhere.
        MpPrePrepareMpPaxosCallback callback = ((MppServiceImpl) MppServicesLocator.getInstance()).prePrepareReplica(replicaInPhase.getTransactionState(), replicaInPhase.getReplica(), null);
        callback.await();

        Map<InetAddress, Optional<MpPaxosId>> responsesByReplica = callback.getResponsesByReplica();

        if (responsesByReplica.size() == 1 && responsesByReplica.entrySet().iterator().next().getValue().isPresent())
        {
            // success
            ReplicaInPrePreparedPhase replicaInPrePreparedPhase = new ReplicaInPrePreparedPhase(replicaInPhase.getTransactionState(), replicaInPhase.getState(), replicaInPhase.getReplica(), responsesByReplica, null);
            return new TransitionResultImpl(replicaInPrePreparedPhase, Phase.PRE_PREPARE_PHASE, true);
        }
        else
        {
            // failed transition
            return new TransitionResultImpl(replicaInPhase, Phase.NO_PHASE, false);
        }
    };

    public static Transition transitionToPrepared = replicaInPhase -> {
        ReplicaInPrePreparedPhase inPhase = (ReplicaInPrePreparedPhase) replicaInPhase;

        List<InetAddress> liveEndpoints = Collections.singletonList(replicaInPhase.getReplica().getHost());
        int requiredParticipants = 1;
        MpPrepareCallback summary = inPhase.getSummary();

        if (summary != null && summary.wasRolledBack())
        {
            return new TransitionResultImpl(inPhase, Phase.ROLLBACK_PHASE, false);
        }

        UUID ballot = createBallot(inPhase.getState(), summary);

        MpCommit toPrepare = MpCommit.newPrepare(inPhase.getTransactionState(), ballot);
        summary = preparePaxos(toPrepare, liveEndpoints, requiredParticipants, consistencyForPaxos, null);

        if(summary.wasRolledBack()) {
            return new TransitionResultImpl(inPhase, Phase.ROLLBACK_PHASE, false);
        }
        else if (notPromised(summary))
        {
            Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
            logger.debug("PreparePaxos. Ballot {} wasn't accepted by replica {} TxId {}", ballot, inPhase.getReplica().getHostAddress(), inPhase.getTransactionState().getTransactionId());
            ReplicaInPrePreparedPhase newInPhase = inPhase.copyWith(summary);
            return new TransitionResultImpl(newInPhase, Phase.PRE_PREPARE_PHASE, false);
        }
        else
        {
            MpCommit inProgress = summary.mostRecentInProgressCommitWithUpdate;
            MpCommit mostRecent = summary.mostRecentCommit;

            logger.debug("PreparePaxos. Ballot {} was accepted by replica {} TxId {}", ballot, inPhase.getReplica().getHostAddress(), inPhase.getTransactionState().getTransactionId());

            if (inProgressNeedsToBeCompleted(inProgress, mostRecent) &&
                !inProgressIsSelf(inPhase.getTransactionState(), inProgress.update) &&
                !wasAlreadyRepaired(inPhase, inProgress))
            {

                if(inPhase.getReplica().getReplicaInPhaseHolder().getCommonState().acquireTransactionForRepair(inProgress.update.id())) {
                    // This Replica acquired transaction to repair
                    logger.debug("PreparePaxos. Found in progress proposal that needs to be completed. Proposal for TxId {}. TxId {}", inProgress.update.getTransactionId(), inPhase.getTransactionState().getTransactionId());
                    logger.debug("PreparePaoxs. Replica {} and TxId {} has found in progress transaction {} and acquired it for repair.",
                                 inPhase.getReplica().getHostAddress(), inPhase.getTransactionState().getTransactionId(),
                                 inProgress.update.getTransactionId());
                    // This in progress transaction can be also for different replicas,
                    // therefore process needs to start over for that transaction.
                    // but the assumption is
                    // that in progress transaction was already in pre prepared phase,
                    // otherwise we wouldn't see it as in progress
                    //
                    // Therefore I need to nest multipartition paxos and initialize it, but jump to proposal phase.
                    // So I need to know for that new transaction:
                    // - replica groups - OK - from ForEachReplicaGroupOperations
                    // - transaction state - OK - from `inProgress`
                    // - client state - RATHER OK - I can only pass what I already have.
                    // - responses by replica - More less OK - works with empty, because it is not used later
                    // - summary - OK - I just got it
                    // - liveEndpoints - OK - I can find it.
                    // - requiredParticipants - OK
                    // - ballot - OK

                    ReplicaGroupsPhaseExecutor repairInProgressPaxosExecutor = createRepairInProgressPaxosExecutor(inProgress.update, inPhase.getState(), ballot);
                    // TODO [MPP] Run it in background ?
                    // TODO [MPP] tryToExecute must return some sensible result, or future.

                    // TODO [MPP] This Stage is incorrect, but I probably don't care.
                    CompletableFuture<PhaseExecutorResult> repairResult = new CompletableFuture<>();

                    CompletableFuture<?> futureWhenRepairIsDone = StorageProxy.performLocally(() -> {
                        try {
                            repairInProgressPaxosExecutor.tryToExecute();
                            PhaseExecutorResult phaseExecutorResult = repairInProgressPaxosExecutor.getPhaseExecutorResult();
                            repairResult.complete(phaseExecutorResult);
                        } catch (TransactionRolledBackException ex)
                        {
                            repairResult.complete(PhaseExecutorResult.ROLLED_BACK);
                        }
                        catch (Exception e) {
                            repairResult.completeExceptionally(e);
                            logger.error("Error caught in during repair of {}", inProgress.update.getTransactionId(), e);
                            throw e;
                        }
                    });

                    ReplicaInReparingInProgressPhase repairing = new ReplicaInReparingInProgressPhase(inPhase.getTransactionState(),
                                                                                                      inPhase.getState(),
                                                                                                      inPhase.getReplica(),
                                                                                                      inPhase.getPaxosId(),
                                                                                                      summary,
                                                                                                      ballot, true,
                                                                                                      repairInProgressPaxosExecutor);

                    CommonStateHolder commonState = inPhase.getReplica().getReplicaInPhaseHolder().getCommonState();
                    TransactionId inProgressTransactionIdToRepair = inProgress.update.id();

                    repairResult.handleAsync((result, ex) -> {
                        logger.debug("Nested in progress repair in execution of TxId {} has completed with smth {} ex {}", inPhase.getTransactionState().getTransactionId(), String.valueOf(result), String.valueOf(ex));
                        commonState.transactionWithIdHasDoneRepairing(inProgressTransactionIdToRepair);
                        if (repairing.repairInProgressPaxosExecutor.equals(repairInProgressPaxosExecutor))
                        {
                            logger.debug("Marking replicaGroupInReparingInProgressPhase as done");
                            repairing.isRepairDone = true;
                        }
                        else {
                            // Checked during test that it does not happen.
                            logger.debug("CASE_OTHER_REPAIR Repairing future has completed with result {} or error {}, but it uses different executor. Expected executor of Tx {} but it was {}",
                                         String.valueOf(result), String.valueOf(ex), repairInProgressPaxosExecutor.transactionState.getTransactionId(), repairing.repairInProgressPaxosExecutor.transactionState.getTransactionId());
                        }
                        return null;
                    });

                    return new TransitionResultImpl(repairing, Phase.BEGIN_AND_REPAIR_PHASE, false);
                }
                else {
                    // It knows that it has to wait for repair of transaction.
                    ReplicaWaitsTillRepairIsDone waitsForRepairOfTx = new ReplicaWaitsTillRepairIsDone(inPhase.getTransactionState(),
                                                     inPhase.getState(),
                                                     inPhase.getReplica(),
                                                     inPhase.getPaxosId(),
                                                     summary,
                                                     ballot, true,
                                                     inProgress.update.id());
                    return new TransitionResultImpl(waitsForRepairOfTx, Phase.BEGIN_AND_REPAIR_PHASE, false);
                }

            }
            else
            {
                // Nothing in progress that needs to be completed. Phase completes
                ReplicaInPreparedPhase newInPhase = new ReplicaInPreparedPhase(
                                                                              inPhase.getTransactionState(),
                                                                              inPhase.getState(),
                                                                              inPhase.getReplica(),
                                                                              inPhase.getPaxosId(),
                                                                              summary,
                                                                              ballot, true);

                return new TransitionResultImpl(newInPhase, Phase.PREPARE_PHASE, true);
            }
        }
    };

    private static boolean inProgressIsSelf(TransactionState transactionState, TransactionState inProgress)
    {
        return transactionState.getTransactionId().equals(inProgress.getTransactionId());
    }

    private static boolean wasAlreadyRepaired(ReplicaInPrePreparedPhase inPhase, MpCommit inProgress)
    {
        return inPhase.getReplica().getReplicaInPhaseHolder().getCommonState().wasAlreadyRepaired(inProgress.update.id());
    }

    public static Transition transitionToPreparedFromReparing = replicaInPhase -> {
        // It just waits until nested repair is done.

        if(ReplicaInReparingInProgressPhase.class.isInstance(replicaInPhase)) {
            ReplicaInReparingInProgressPhase inPhase = (ReplicaInReparingInProgressPhase) replicaInPhase;
            if (inPhase.isRepairDone)
            {
                ReplicaInPhase otherReplicaInPhase = inPhase.repairInProgressPaxosExecutor.replicas.stream().filter(otherReplica -> otherReplica.equals(replicaInPhase.getReplica()))
                                                                                                   .findFirst().map(r -> r.getReplicaInPhaseHolder().getReplicaInPhase()).get();

                if(ReplicaInPreparedPhase.class.isInstance(otherReplicaInPhase)) {
                    ReplicaInPreparedPhase otherInPrepared = (ReplicaInPreparedPhase) otherReplicaInPhase;

                    ReplicaInPreparedPhase newInPhase = new ReplicaInPreparedPhase(
                                                                                  inPhase.getTransactionState(),
                                                                                  inPhase.getState(),
                                                                                  inPhase.getReplica(),
                                                                                  inPhase.getPaxosId(),
                                                                                  inPhase.summary,
                                                                                  otherInPrepared.ballot, true,
                                                                                  Optional.of(otherInPrepared.getTransactionState().id()));
                    return new TransitionResultImpl(newInPhase, Phase.PREPARE_PHASE, true);
                }
                else
                {
                    return new TransitionResultImpl(replicaInPhase, Phase.PREPARE_PHASE, true);
                }
            }
            else
            {
                return new TransitionResultImpl(inPhase, Phase.BEGIN_AND_REPAIR_PHASE, false);
            }
        }
        else if(ReplicaWaitsTillRepairIsDone.class.isInstance(replicaInPhase)) {
            ReplicaWaitsTillRepairIsDone waitTill = (ReplicaWaitsTillRepairIsDone) replicaInPhase;
            boolean success = waitTill.getReplica().getReplicaInPhaseHolder().getCommonState().wasAlreadyRepaired(waitTill.waitTillThisRepairIsDone);
            if(success) {
                return new TransitionResultImpl(waitTill, Phase.PREPARE_PHASE, true);
            }
            else {
                return new TransitionResultImpl(waitTill, Phase.BEGIN_AND_REPAIR_PHASE, false);
            }

        }
        else {
            // Other replicas made this one, transition into this phase, it should be at least prepared
            return new TransitionResultImpl(replicaInPhase, Phase.PRE_PREPARE_PHASE, true);
        }
    };

    public static class ReplicaInProposedPhase extends ReplicaInPreparedPhase
    {

        MpCommit proposal;

        protected ReplicaInProposedPhase(TransactionState transactionState,
                                         ClientState state,
                                         Replica replica,
                                         Optional<MpPaxosId> paxosId,
                                         MpPrepareCallback summary,
                                         UUID ballot,
                                         boolean timeOutProposalIfPartialResponses,
                                         MpCommit proposal)
        {
            super(transactionState, state, replica, paxosId, summary,
                  ballot,
                  timeOutProposalIfPartialResponses);
            this.proposal = proposal;
        }

        public static ReplicaInPhase fromInPrepared(ReplicaInPreparedPhase inPrepared, MpCommit proposal)
        {
            return new ReplicaInProposedPhase(inPrepared.getTransactionState(),
                                              inPrepared.getState(),
                                              inPrepared.getReplica(),
                                              inPrepared.getPaxosId(),
                                              inPrepared.getSummary(),
                                              inPrepared.ballot,
                                              inPrepared.timeOutProposalIfPartialResponses,
                                              proposal);
        }
    }


    /**
     * Can transition to 'proposed', only from 'prepared'.
     */
    public static Transition transitionToProposed = replicaInPhase -> {
        // Replica Group in Phase should be prepared
        ReplicaInPreparedPhase inPrepared = (ReplicaInPreparedPhase) replicaInPhase;
        UUID ballot = inPrepared.ballot;
        MpCommit proposal = MpCommit.newProposal(ballot, replicaInPhase.getTransactionState());
        Tracing.trace("Multi partition paxos; proposing client-requested transaction state for {}", ballot);


        List<InetAddress> participants = Collections.singletonList(replicaInPhase.getReplica().getHost());
        try {
            boolean proposed = proposePaxos(proposal, participants, 1, inPrepared.timeOutProposalIfPartialResponses, consistencyForPaxos, null);
            logger.debug("PaxosProposed response is {} for replica {}, TxId {}", proposed, replicaInPhase.getReplica(), replicaInPhase.getTransactionState().getTransactionId());

            if (proposed)
            {
                ReplicaInPhase newInPhase = ReplicaInProposedPhase.fromInPrepared(inPrepared, proposal);
                return new TransitionResultImpl(newInPhase, Phase.PROPOSE_PHASE, true);
            }
            else
            {

                ReplicaInPrePreparedPhase newInPhase = new ReplicaInPrePreparedPhase(
                                                                                    inPrepared.getTransactionState(),
                                                                                    inPrepared.getState(),
                                                                                    inPrepared.getReplica(),
                                                                                    inPrepared.getPaxosId(),
                                                                                    inPrepared.getSummary());
                return new TransitionResultImpl(newInPhase, Phase.PRE_PREPARE_PHASE, false);
            }
        } catch (TransactionRolledBackException e) {
            return new TransitionResultImpl(replicaInPhase, Phase.ROLLBACK_PHASE, false);
        }
    };

    /**
     * Can transition to 'commit', only from 'proposed'
     */
    public static Transition transitionToCommitted = replicaInPhase -> {
        ReplicaInProposedPhase inProposed = (ReplicaInProposedPhase) replicaInPhase;
        commitPaxos3(inProposed.proposal, consistencyForCommit, true, replicaInPhase.getReplica());
        // TODO [MPP] Maybe I need to check for some error handling, but in principle it should work.

        Tracing.trace("CAS successful");
//        BaseReplicaInPhase newInPhase = new BaseReplicaInPhase(replicaInPhase.getTransactionState(),
//                                                               replicaInPhase.getState(),
//                                                               replicaInPhase.getReplica(),
//                                                               null);
        return new TransitionResultImpl(replicaInPhase, Phase.AFTER_COMMIT_PHASE, true);
    };

    static
    {
        nextPhaseIs = new HashMap<>();

        nextPhaseIs.put(Phase.NO_PHASE, Phase.PRE_PREPARE_PHASE);
        nextPhaseIs.put(Phase.PRE_PREPARE_PHASE, Phase.PREPARE_PHASE);
        nextPhaseIs.put(Phase.BEGIN_AND_REPAIR_PHASE, Phase.PREPARE_PHASE);
        nextPhaseIs.put(Phase.PREPARE_PHASE, Phase.PROPOSE_PHASE);
        nextPhaseIs.put(Phase.PROPOSE_PHASE, Phase.COMMIT_PHASE);
        nextPhaseIs.put(Phase.COMMIT_PHASE, Phase.AFTER_COMMIT_PHASE);
        nextPhaseIs.put(Phase.ROLLBACK_PHASE, Phase.ROLLBACK_PHASE);
    }

    /**
     * @param fromPhase
     * @param toPhase
     * @return transition which moves from {@param fromPhase} to {@param toPhase}
     */
    public static TransitionId transitionToPhase(Phase fromPhase, Phase toPhase)
    {
        if (fromPhase == toPhase)
        {
            return TransitionId.NOOP;
        }
        else if(toPhase == Phase.ROLLBACK_PHASE) {
            return TransitionId.NOOP;
        }
        else if(fromPhase == Phase.ROLLBACK_PHASE) {
            return TransitionId.NOOP;
        }
        else if (fromPhase == Phase.NO_PHASE && toPhase == Phase.PRE_PREPARE_PHASE)
        {
            return TransitionId.TO_PRE_PREPARED;
        }
        else if (fromPhase == Phase.PRE_PREPARE_PHASE && toPhase == Phase.PREPARE_PHASE)
        {
            return TransitionId.TO_PREPARED;
        }
        else if (fromPhase == Phase.PREPARE_PHASE && toPhase == Phase.PREPARE_PHASE)
        {
//            return transitionToPrepared; // TODO [MPP] This will make replica group refresh its ballot, could be NOOP operation.
            return TransitionId.TO_PREPARED;
        }
        else if (fromPhase == Phase.BEGIN_AND_REPAIR_PHASE && toPhase == Phase.PREPARE_PHASE)
        {
            return TransitionId.TO_PREPARED_FROM_REPARING;
        }
        else if (fromPhase == Phase.PREPARE_PHASE && toPhase == Phase.PROPOSE_PHASE)
        {
            return TransitionId.TO_PROPOSED;
        }
        else if (fromPhase == Phase.PROPOSE_PHASE && toPhase == Phase.COMMIT_PHASE)
        {
            return TransitionId.TO_COMMITTED;
        }
        else if (toPhase == Phase.PREPARE_PHASE)
        {
            // ignoring current phase, because it just has to go back couple of phases.
            return TransitionId.TO_PREPARED;
        }
        else if(fromPhase == Phase.AFTER_COMMIT_PHASE)
        {
            // There is nothing else to do after commit phase so just return NOOP
            return TransitionId.NOOP;
        }
        else
        {
            throw new RuntimeException("Unexpected transition required from phase " + fromPhase + " to phase " + toPhase);
        }
    }

    public static TransitionId findTransition(Phase groupsMinimumPhase, Phase thisGroupPhase)
    {
        Phase nextCommonPhase = getNextExpectedPhase(groupsMinimumPhase);
        return transitionToPhase(thisGroupPhase, nextCommonPhase);
    }

    /**
     * Unlike commitPaxos, this does not wait for replies
     */
    private static void sendCommit(MpCommit commit, Iterable<InetAddress> replicas)
    {
        MessageOut<MpCommit> message = new MessageOut<>(MessagingService.Verb.MP_PAXOS_COMMIT, commit, MpCommit.serializer);
        for (InetAddress target : replicas)
            MessagingService.instance().sendOneWay(message, target);
    }

    private static void statistics(boolean isWrite)
    {
        if (isWrite)
            casWriteMetrics.unfinishedCommit.inc();
        else
            casReadMetrics.unfinishedCommit.inc();
    }

    // If we have an in-progress ballot greater than the MRC we know, then it's an in-progress round that
    // needs to be completed, so do it.
    private static boolean inProgressNeedsToBeCompleted(MpCommit inProgress, MpCommit mostRecent)
    {
        return !inProgress.update.isEmpty() && inProgress.isAfter(mostRecent);
    }

    private static boolean notPromised(MpPrepareCallback summary)
    {
        return !summary.promised;
    }

    private static UUID createBallot(ClientState state, MpPrepareCallback summary)
    {
        long minTimestampMicrosToUse = summary == null ? Long.MIN_VALUE : 1 + UUIDGen.microsTimestamp(summary.mostRecentInProgressCommit.ballot);
        long ballotMicros = state.getTimestamp(minTimestampMicrosToUse);
        return UUIDGen.getTimeUUIDFromMicros(ballotMicros);
    }

    /**
     * Sends prepare requests and WAITS (blocks thread)
     *
     * @return
     */
    private static MpPrepareCallback preparePaxos(MpCommit toPrepare, List<InetAddress> endpoints, int requiredParticipants,
                                                  ConsistencyLevel consistencyForPaxos,
                                                  ReplicasGroupsOperationCallback replicasGroupOperationCallback)
    throws WriteTimeoutException
    {
        logger.debug("preparePaxos. for Tx ID {} Targeted endpoints are {}. Required participants {}", toPrepare.update.getTransactionId(), endpoints, requiredParticipants, requiredParticipants);
        MpPrepareCallback callback = new MpPrepareCallback(toPrepare.update, requiredParticipants, consistencyForPaxos, replicasGroupOperationCallback);
        MessageOut<MpCommit> message = new MessageOut<>(MessagingService.Verb.MP_PAXOS_PREPARE, toPrepare, MpCommit.serializer);
        for (InetAddress target : endpoints)
        {
            logger.debug("preparePaxos sending message to target {}", target);
            MessagingService.instance().sendRR(message, target, callback);
        }
        callback.await();
        return callback;
    }

    static AtomicInteger proposePaxosCallsCount = new AtomicInteger(0);

    /**
     * @param proposal has ballot that was prepared + transaction state
     */
    private static boolean proposePaxos(MpCommit proposal, List<InetAddress> endpoints,
                                        int requiredParticipants, boolean timeoutIfPartial,
                                        ConsistencyLevel consistencyLevel, ReplicasGroupsOperationCallback replicasGroupOperationCallback)
    throws WriteTimeoutException
    {
        logger.debug("proposePaxos call {}", proposePaxosCallsCount.incrementAndGet());
        logger.debug("proposePaxos begins. TxId {} Targeted endpoints are {}. Required participants {}. Consistency Level {}", proposal.update.getTransactionId(), endpoints, requiredParticipants, consistencyLevel);
        MpProposeCallback callback = new MpProposeCallback(endpoints.size(), requiredParticipants, !timeoutIfPartial, consistencyLevel, replicasGroupOperationCallback);
        MessageOut<MpCommit> message = new MessageOut<>(MessagingService.Verb.MP_PAXOS_PROPOSE, proposal, MpCommit.serializer);
        for (InetAddress target : endpoints)
        {
            logger.debug("Sending MP_PAXOS_PROPOSE message to target {} with ballot {} proposing transaction with id {}", target, proposal.ballot, proposal.update.id());
            MessagingService.instance().sendRR(message, target, callback);
        }
        logger.debug("Awaiting MP_PAXOS_PROPOSE callback for tx id {}", proposal.update.getTransactionId());
        callback.await();
        if (callback.wasRolledBack())
        {
            logger.info("Callback from proposal says that transaction id {} was rolled back. Throwing excpetion", proposal.update.getTransactionId());
            throw new TransactionRolledBackException(proposal.update);
        }
        if (callback.isSuccessful())
        {
            logger.debug("MP_PAXOS_PROPOSE callback was successful. Tx ID {}", proposal.update.getTransactionId());
            return true;
        }

        if (timeoutIfPartial && !callback.isFullyRefused())
            throw new WriteTimeoutException(WriteType.CAS, consistencyLevel, callback.getAcceptCount(), requiredParticipants);
        logger.debug("MP_PAXOS_PROPOSE callback was not successful, returning false. Tx ID {}", proposal.update.getTransactionId());
        return false;
    }

    private static void commitPaxos3(MpCommit proposal, ConsistencyLevel consistencyLevel, boolean shouldHint, Replica replica) throws WriteTimeoutException
    {
        List<InetAddress> naturalEndpoints = Collections.singletonList(replica.getHost());
        Collection<InetAddress> pendingEndpoints = Collections.emptyList();

        // TODO [MPP] Ignoring parameter consistencyLevel because commits are done seperately for each replica therefore it has to write with One
        // TODO [MPP] Need to think whether it is OK to commit multiple times because it will yell that transaction was rolled back on second time
        AbstractWriteResponseHandler<MpCommit> responseHandler = new MppCommitWriteResponseHandler<>(naturalEndpoints,
                                                                                                     pendingEndpoints,
                                                                                                     ConsistencyLevel.ONE, null, null, WriteType.SIMPLE);
        boolean shouldBlockOverride = true; // TODO [MPP]
        commitPaxosInternal(proposal, shouldHint, shouldBlockOverride, naturalEndpoints, pendingEndpoints, responseHandler);
    }

    private static void commitPaxos2(MpCommit proposal, ConsistencyLevel consistencyLevel, boolean shouldHint, ReplicasGroupAndOwnedItems replicasGroupAndItems) throws WriteTimeoutException
    {
        List<InetAddress> naturalEndpoints = replicasGroupAndItems.getReplicasGroup().getReplicas();
        Collection<InetAddress> pendingEndpoints = Collections.emptyList();

        AbstractWriteResponseHandler<MpCommit> responseHandler = new MppCommitWriteResponseHandler<>(naturalEndpoints,
                                                                                                     pendingEndpoints,
                                                                                                     consistencyLevel, null, null, WriteType.SIMPLE);
        boolean shouldBlockOverride = true; // TODO [MPP]
        commitPaxosInternal(proposal, shouldHint, shouldBlockOverride, naturalEndpoints, pendingEndpoints, responseHandler);
    }

    private static void commitPaxosInternal(MpCommit proposal, boolean shouldHint, boolean shouldBlock, List<InetAddress> naturalEndpoints, Collection<InetAddress> pendingEndpoints, AbstractWriteResponseHandler<MpCommit> responseHandler)
    {
        MessageOut<MpCommit> message = new MessageOut<>(MessagingService.Verb.MP_PAXOS_COMMIT, proposal, MpCommit.serializer);
        List<InetAddress> allEndpoints = ImmutableList.copyOf(Iterables.concat(naturalEndpoints, pendingEndpoints));

        Map<InetAddress, List<MppHint>> replicaToHints = new HashMap<>();

        // If should hint then send commit wrapped in hint.
        if (shouldHint)
        {
            List<TransactionItemWithAddresses> transactionItemsAndAllTheirEndpoints = ForEachReplicaGroupOperations.mapTransactionItemsToAllEndpoints(proposal.update).collect(Collectors.toList());
            Set<InetAddress> deadReplicas = allEndpoints.stream().filter(destination -> !FailureDetector.instance.isAlive(destination)).collect(Collectors.toSet());
            List<InetAddress> aliveReplicas = allEndpoints.stream().filter(destination -> FailureDetector.instance.isAlive(destination)).collect(Collectors.toList());

            replicaToHints = aliveReplicas.stream().map(aliveReplica -> {
                List<MppHint> hints = deadReplicas.stream().map(deadReplica -> {
                    List<TransactionItem> itemsToHintDeadReplica = transactionItemsAndAllTheirEndpoints
                                                                   .stream()
                                                                   .filter(ti -> ti.getEndPoints().contains(deadReplica))
                                                                   .filter(ti -> ti.getEndPoints().contains(aliveReplica))
                                                                   .map(ti -> ti.getTxItem()).collect(Collectors.toList());

                    return new MppHint(deadReplica, UUIDGen.unixTimestamp(proposal.ballot), proposal.update.id(), itemsToHintDeadReplica);
                }).collect(Collectors.toList());

                return Pair.create(aliveReplica, hints);
            }).filter(p -> !p.right.isEmpty()).collect(Collectors.toMap(p -> p.left, p -> p.right));

            for (InetAddress destination : aliveReplicas)
            {
                if (replicaToHints.containsKey(destination))
                {
                    // replica has hints for dead replicas so send commit with hints.
                    List<MppHint> hints = replicaToHints.get(destination);
                    MessageOut<MpCommitWithHints> messageWithHints = new MessageOut<>(MessagingService.Verb.MP_PAXOS_COMMIT_WITH_HINTS, new MpCommitWithHints(proposal, hints), MpCommitWithHints.serializer);
                    if (shouldBlock)
                        MessagingService.instance().sendRR(messageWithHints, destination, responseHandler, shouldHint);
                    else
                        MessagingService.instance().sendOneWay(messageWithHints, destination);
                }
                else
                {
                    // send commit without hints.
                    if (shouldBlock)
                        MessagingService.instance().sendRR(message, destination, responseHandler, shouldHint);
                    else
                        MessagingService.instance().sendOneWay(message, destination);
                }
            }
        }
        else
        {
            // Then send commits only
            for (InetAddress destination : allEndpoints)
            {
                if (FailureDetector.instance.isAlive(destination))
                {
                    if (shouldBlock)
                        MessagingService.instance().sendRR(message, destination, responseHandler, shouldHint);
                    else
                        MessagingService.instance().sendOneWay(message, destination);
                }
            }
        }

        if (shouldBlock)
            responseHandler.get();
    }


    // TODO [MPP] No support for pending endpoints at the moment, maybe change it later.

    /**
     * @return list of participants, required participants
     */
    private static Pair<List<InetAddress>, Integer> getPaxosParticipants(ReplicasGroupAndOwnedItems replicaGroup, ConsistencyLevel consistencyForPaxos) throws UnavailableException
    {
        List<InetAddress> liveReplicas = replicaGroup.getReplicasGroup().getReplicas();
        TransactionItem transactionItem = replicaGroup.getOwnedItems().iterator().next();
        int replicationFactor = Keyspace.open(transactionItem.getKsName()).getReplicationStrategy().getReplicationFactor();
        int requiredParticipants = replicationFactor / 2 + 1;

        if (liveReplicas.size() < requiredParticipants)
        {
            throw new UnavailableException(consistencyForPaxos, requiredParticipants, liveReplicas.size());
        }

        return Pair.create(liveReplicas, requiredParticipants);
    }
}
