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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.TransactionRolledBackException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteFailureException;
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

    private static Set<UUID> transactionsThatAreAfterPrePrepared = new ConcurrentSkipListSet<>();

    private static Set<UUID> waitingForTransactionUntilItIsInPrePrepared = new ConcurrentSkipListSet<>();

    // TODO [MPP] For TESTING ONLY
    public static void addToWaitUntilAfterPrePrepared(List<UUID> transactionIds)
    {
        logger.debug("addToWaitUntilAfterPrePrepared transactionId {}", transactionIds);
        waitingForTransactionUntilItIsInPrePrepared = new ConcurrentSkipListSet<>(transactionIds);
        transactionsThatAreAfterPrePrepared = new ConcurrentSkipListSet<>();
    }

    private static final Map<Phase, Phase> nextPhaseIs;

    // Transition to phase from key
    private static final Map<Phase, Transition> transitionToPhase;

    public static Phase getNextExpectedPhase(Phase current) {
        return nextPhaseIs.get(current);
    }

    private static class ReplicaGroupInPhaseHolder {
        private ReplicaGroupInPhase replicaGroup;

        public ReplicaGroupInPhaseHolder(ReplicaGroupInPhase replicaGroup)
        {
            this.replicaGroup = replicaGroup;
        }

        public ReplicaGroupInPhase getReplicaGroup()
        {
            return replicaGroup;
        }

        public void setReplicaGroup(ReplicaGroupInPhase replicaGroup)
        {
            this.replicaGroup = replicaGroup;
        }
    }

    public static class ReplicaInPhaseHolder {
        private ReplicaInPhase replicaInPhase;
        private TransitionResult transitionResult;

        public ReplicaInPhaseHolder(ReplicaInPhase replicaInPhase)
        {
            this.replicaInPhase = replicaInPhase;
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
    }

    interface ReplicaInPhase {
        TransactionState getTransactionState();

        Replica getReplica();

        ClientState getState();

        Optional<MpPaxosId> getPaxosId();
    }

    interface ReplicaGroupInPhase {
        Phase currentPhase();

        ReplicasGroupAndOwnedItems getReplicaGroup();

        boolean isQuorum();

        TransactionState getTransactionState();

        ClientState getState();
    }

    static abstract class AbstractReplicaInPhase implements ReplicaInPhase {
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

    static abstract class AbstractReplicaGroupInPhase implements ReplicaGroupInPhase {
        protected ReplicasGroupAndOwnedItems replicaGroup;
        protected TransactionState transactionState;
        protected ClientState state;

        protected AbstractReplicaGroupInPhase(ReplicasGroupAndOwnedItems replicaGroup, TransactionState transactionState, ClientState state, Phase phase)
        {
            this.replicaGroup = replicaGroup;
            this.transactionState = transactionState;
            this.state = state;
            setPhaseOnReplicas(replicaGroup, phase);
        }

        @Override
        final public Phase currentPhase()
        {
            return getMaximumPhaseSharedByQuorum(replicaGroup.getReplicasGroup());
        }

        @Override
        public ReplicasGroupAndOwnedItems getReplicaGroup()
        {
            return replicaGroup;
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
    }

    public static void setPhaseOnReplicas(ReplicasGroupAndOwnedItems replicaGroup, Phase phase) {
        replicaGroup.getReplicasGroup().getAllReplicasInThatGroup().forEach(replica -> replica.setPhase(phase));
    }

    public static Phase findMinimumPhase(ReplicasGroup replicasGroup) {
        return replicasGroup.getAllReplicasInThatGroup().stream().map(Replica::getPhase).sorted().findFirst().get();
    }

    public static Phase getCurrentPhase(ReplicasGroup replicaGroup) {
        Optional<Phase> maybeHasQuorum = getPhaseOfQuorum(replicaGroup);

        return maybeHasQuorum.orElse(findMinimumPhase(replicaGroup));
    }

    public static Optional<Phase> getPhaseOfQuorum(ReplicasGroup replicaGroup)
    {
        List<Replica> allReplicasInThatGroup = replicaGroup.getAllReplicasInThatGroup();
        int size = allReplicasInThatGroup.size();
        long quorum = size / 2 + 1;
        Map<Phase, Long> countByPhase = allReplicasInThatGroup.stream().map(Replica::getPhase).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        return countByPhase.entrySet().stream().filter(e -> e.getValue() >= quorum).map(Map.Entry::getKey).findFirst();
    }


    public static boolean isQuorumInSamePhase(ReplicasGroup replicaGroup) {
        return getPhaseOfQuorum(replicaGroup).isPresent();
    }

    public static Phase getMaximumPhaseSharedByQuorum(ReplicasGroup replicasGroup)
    {
        List<Replica> allReplicasInThatGroup = replicasGroup.getAllReplicasInThatGroup();
        return getMaximumPhaseSharedByQuorum(allReplicasInThatGroup.stream().map(x -> x).collect(Collectors.toList()));
    }

    public static Phase getMaximumPhaseSharedByQuorum(Collection<WithPhase> withPhases) {
        int size = withPhases.size();
        int quorum = size / 2 + 1;
        List<Phase> collected = withPhases.stream().map(WithPhase::getPhase).sorted().collect(Collectors.toList());
        return collected.get(quorum - 1);
    }

    public static Phase findNextPhaseForReplica(Replica replica, Phase minimumPhaseAmongAllReplicaGroups, Phase nextExpectedPhase) {
        if(replica.getPhase() == nextExpectedPhase) {
            return replica.getPhase();
        }
        else if(replica.getPhase() == minimumPhaseAmongAllReplicaGroups ) {
            return nextExpectedPhase;
        }
        else
        {
            return getNextExpectedPhase(replica.getPhase());
        }
    }

    public static Phase findNextPhaseForReplica(Replica replica, Collection<ReplicasGroup> replicaGroups) {
        Phase minimumPhaseAmongAllReplicaGroups = findMinimumPhaseAmongAllReplicaGroups(replicaGroups);
        Phase nextExpectedPhase = getNextExpectedPhase(minimumPhaseAmongAllReplicaGroups);
        return findNextPhaseForReplica(replica, minimumPhaseAmongAllReplicaGroups, nextExpectedPhase);
    }

    public static TransitionId findNextTransitionId(Replica replica, Phase nextPhaseForReplica) {
        return transitionToPhase(replica.getPhase(), nextPhaseForReplica);
    }

    public static class BaseReplicaInPhase extends AbstractReplicaInPhase {

        protected BaseReplicaInPhase(TransactionState transactionState, ClientState state, Replica replica, Phase phase)
        {
            super(transactionState, state, replica);
            if(phase != null)
                replica.setPhase(phase);
        }


//        @Override
//        public boolean isQuorum()
//        {
//            return true;
//        }
    }

    public static class BaseReplicaGroupInPhase extends AbstractReplicaGroupInPhase {

        protected BaseReplicaGroupInPhase(ReplicasGroupAndOwnedItems replicaGroup, TransactionState transactionState, ClientState state, Phase phase)
        {
            super(replicaGroup, transactionState, state, phase);
        }


        @Override
        public boolean isQuorum()
        {
            return true;
        }
    }

    public static class ReplicaInPrePreparedPhase extends AbstractReplicaInPhase {

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

        public ReplicaInPrePreparedPhase copyWith(MpPrepareCallback summary) {
            return new ReplicaInPrePreparedPhase(transactionState, state, replica, paxosIdOpt, summary);
        }

//        @Override
//        public Phase currentPhase()
//        {
//            return Phase.PRE_PREPARE_PHASE;
//        }

//        @Override
//        public boolean isQuorum()
//        {
//            return true; // because it was taken care of in callback handler MppPrePrepareCallback
//        }

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

    public static class ReplicaGroupInPrePreparedPhase extends AbstractReplicaGroupInPhase {

        Map<InetAddress, Optional<MpPaxosId>> responsesByReplica;

        MpPrepareCallback summary;

        public ReplicaGroupInPrePreparedPhase(ReplicasGroupAndOwnedItems replicaGroup,
                                       TransactionState transactionState, ClientState state,
                                       Map<InetAddress, Optional<MpPaxosId>> responsesByReplica, MpPrepareCallback summary)
        {
            super(replicaGroup, transactionState, state, Phase.PRE_PREPARE_PHASE);
            this.responsesByReplica = responsesByReplica;
            this.summary = summary;
        }

        public ReplicaGroupInPrePreparedPhase copyWith(MpPrepareCallback summary) {
            return new ReplicaGroupInPrePreparedPhase(replicaGroup, transactionState, state, responsesByReplica, summary);
        }

//        @Override
//        public Phase currentPhase()
//        {
//            return Phase.PRE_PREPARE_PHASE;
//        }

        @Override
        public boolean isQuorum()
        {
            return true; // because it was taken care of in callback handler MppPrePrepareCallback
        }

        public Map<InetAddress, Optional<MpPaxosId>> getResponsesByReplica()
        {
            return responsesByReplica;
        }

        public MpPrepareCallback getSummary()
        {
            return summary;
        }
    }

    public static class ReplicaGroupInPreparedPhase extends ReplicaGroupInPrePreparedPhase {

        private UUID ballot;

        public List<InetAddress> liveEndpoints;

        public int requiredParticipants;

        public boolean timeOutProposalIfPartialResponses;

        private Optional<TransactionId> txRepairedAlready = Optional.empty();

        protected ReplicaGroupInPreparedPhase(ReplicasGroupAndOwnedItems replicaGroup,
                                              TransactionState transactionState,
                                              ClientState state,
                                              Map<InetAddress, Optional<MpPaxosId>> responsesByReplica,
                                              MpPrepareCallback summary,
                                              List<InetAddress> liveEndpoints,
                                              int requiredParticipants,
                                              UUID ballot,
                                              boolean timeOutProposalIfPartialResponses)
        {
            super(replicaGroup, transactionState, state, responsesByReplica, summary);
            this.liveEndpoints = liveEndpoints;
            this.requiredParticipants = requiredParticipants;
            this.ballot = ballot;
            this.timeOutProposalIfPartialResponses = timeOutProposalIfPartialResponses;
        }

        protected ReplicaGroupInPreparedPhase(ReplicasGroupAndOwnedItems replicaGroup,
                                              TransactionState transactionState,
                                              ClientState state,
                                              Map<InetAddress, Optional<MpPaxosId>> responsesByReplica,
                                              MpPrepareCallback summary,
                                              List<InetAddress> liveEndpoints,
                                              int requiredParticipants,
                                              UUID ballot,
                                              boolean timeOutProposalIfPartialResponses,
                                              Optional<TransactionId> txRepairedAlready)
        {
            this(replicaGroup, transactionState, state, responsesByReplica, summary, liveEndpoints, requiredParticipants, ballot, timeOutProposalIfPartialResponses);
            this.txRepairedAlready = txRepairedAlready;
        }

//        @Override
//        public Phase currentPhase()
//        {
//            return Phase.PREPARE_PHASE;
//        }

        @Override
        public boolean isQuorum()
        {
            return false;
        }

        public UUID getBallot()
        {
            return ballot;
        }
    }

    public static class ReplicaInPreparedPhase extends ReplicaInPrePreparedPhase {

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

//        @Override
//        public Phase currentPhase()
//        {
//            return Phase.PREPARE_PHASE;
//        }

//        @Override
//        public boolean isQuorum()
//        {
//            return false;
//        }

        public UUID getBallot()
        {
            return ballot;
        }
    }

    public static class ReplicaGroupInReparingInProgressPhase extends ReplicaGroupInPreparedPhase {

        ReplicaGroupsPhaseExecutor repairInProgressPaxosExecutor;

        volatile boolean isRepairDone = false;

        protected ReplicaGroupInReparingInProgressPhase(ReplicasGroupAndOwnedItems replicaGroup,
                                                        TransactionState transactionState,
                                                        ClientState state,
                                                        Map<InetAddress, Optional<MpPaxosId>> responsesByReplica,
                                                        MpPrepareCallback summary,
                                                        List<InetAddress> liveEndpoints,
                                                        int requiredParticipants,
                                                        UUID ballot,
                                                        boolean timeOutProposalIfPartialResponses,
                                                        ReplicaGroupsPhaseExecutor repairInProgressPaxosExecutor)
        {
            super(replicaGroup, transactionState, state, responsesByReplica, summary, liveEndpoints, requiredParticipants, ballot, timeOutProposalIfPartialResponses);
            this.repairInProgressPaxosExecutor = repairInProgressPaxosExecutor;
        }

//        public Phase currentPhase()
//        {
//            return Phase.BEGIN_AND_REPAIR_PHASE;
//        }
    }

    public static class ReplicaInReparingInProgressPhase extends ReplicaInPreparedPhase {

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

//        public Phase currentPhase()
//        {
//            return Phase.BEGIN_AND_REPAIR_PHASE;
//        }
    }

    enum TransitionId {
        NOOP, TO_PRE_PREPARED, TO_PREPARED, TO_PROPOSED, TO_COMMITTED, TO_PREPARED_FROM_REPARING,
    }

    public static Transition findTransitionByTransitionId(TransitionId transitionId) {
        if(transitionId == TransitionId.NOOP) return noopTransition;
        else if(transitionId == TransitionId.TO_PRE_PREPARED) return transitionToPrePrepared;
        else if(transitionId == TransitionId.TO_PREPARED) return transitionToPrepared;
        else if(transitionId == TransitionId.TO_PREPARED_FROM_REPARING) return transitionToPreparedFromReparing;
        else if(transitionId == TransitionId.TO_COMMITTED) return transitionToCommitted;
        else if(transitionId == TransitionId.TO_PROPOSED) return transitionToProposed;
        else
        {
            throw new RuntimeException("Unknown transition id " + transitionId);
        }
    }

//    interface Transition {
//        ReplicaGroupInPhase transition(ReplicaGroupInPhase replicaGroup);
//    }

    interface Transition {
        TransitionResult transition(ReplicaInPhase replicaInPhase);
    }

    interface TransitionResult extends WithPhase {
        boolean wasTransitionSuccessful();

        Phase phaseAfterTransition();

        ReplicaInPhase getReplicaInPhase();
    }

    public static class TransitionResultImpl implements TransitionResult {

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


//    public static Transition getNextTransition(Phase phase) {
//        Phase nextPhase = getNextExpectedPhase(phase);
//        return transitionToPhase.get(nextPhase);
//    }

    public static ReplicaGroupsPhaseExecutor createMultiPartitionPaxosPhaseExecutor(List<ReplicasGroupAndOwnedItems> replicaGroups, TransactionState transactionState, ClientState state) {
        Phase noPhase = Phase.NO_PHASE;
        Collection<Replica> allReplicas = replicaGroups.iterator().next().getAllReplicas();
        allReplicas.forEach(replica -> {
            ReplicaInPhaseHolder holder = new ReplicaInPhaseHolder(new BaseReplicaInPhase(transactionState, state, replica, noPhase));
            replica.setHolder(holder);
        });
        logger.debug("createMultiPartitionPaxosPhaseExecutor replicaGroups {}. TransactionState is {}", replicaGroups, transactionState);
        List<ReplicaGroupInPhaseHolder> inPhases = replicaGroups.stream()
                                                              .map(rg -> {

                                                                  return new BaseReplicaGroupInPhase(rg, transactionState, state, noPhase);
                                                              })
                                                              .map(ReplicaGroupInPhaseHolder::new)
                                                              .collect(Collectors.toList());

        return new ReplicaGroupsPhaseExecutor(Phase.AFTER_COMMIT_PHASE, inPhases, replicaGroups.iterator().next().getAllReplicas());
    }

    public static ReplicaGroupsPhaseExecutor createRepairInProgressPaxosExecutor(TransactionState inProgressTransactionState, ClientState state, UUID ballot) {
        logger.debug("createRepairInProgressPaxosExecutor for TxId {} using ballot {}", inProgressTransactionState.getTransactionId(), ballot);
        List<ReplicasGroupAndOwnedItems> replicasGroupAndOwnedItems = ForEachReplicaGroupOperations.groupItemsByReplicas(inProgressTransactionState);
        Map<InetAddress, Optional<MpPaxosId>> responsesByReplica = Collections.emptyMap(); // TODO [MPP] Maybe it will work.

        Collection<Replica> allReplicas = replicasGroupAndOwnedItems.iterator().next().getAllReplicas();
        allReplicas.forEach(replica -> {
            ReplicaInPhaseHolder holder = new ReplicaInPhaseHolder(new ReplicaInPreparedPhase(inProgressTransactionState, state, replica, Optional.empty(), null, ballot, false));
            replica.setHolder(holder);
        });

        List<ReplicaGroupInPhaseHolder> inPhases = replicasGroupAndOwnedItems.stream().map(rg -> {
            Pair<List<InetAddress>, Integer> p = getPaxosParticipants(rg, consistencyForPaxos);
            List<InetAddress> liveEndpoints = p.left;
            int requiredParticipants = p.right;
            // TODO [MPP] This can be later deleted or at least simplified.
            return new ReplicaGroupInPreparedPhase(rg, inProgressTransactionState, state, responsesByReplica, null, liveEndpoints, requiredParticipants, ballot, false);
        }).map(ReplicaGroupInPhaseHolder::new)
        .collect(Collectors.toList());

        // TODO [MPP] It might be done if we see that transaction was rolled back.
        return new ReplicaGroupsPhaseExecutor(Phase.AFTER_COMMIT_PHASE, inPhases, replicasGroupAndOwnedItems.iterator().next().getAllReplicas());
    }

    public static Phase findMinimumPhaseAmongAllReplicaGroups(Collection<ReplicasGroup> replicaGroups) {
        return replicaGroups.stream().map(StorageProxyMpPaxosExtensions::getMaximumPhaseSharedByQuorum).sorted().findFirst().get();
    }

//    public static Phase findMinimumPhaseAmongTransitionResults(Collection<TransitionResult> results) {
//        return results.stream().map(StorageProxyMpPaxosExtensions::getMaximumPhaseSharedByQuorum).sorted().findFirst().get();
//    }



    public static class ReplicaGroupsPhaseExecutor {

        private final Collection<Replica> replicas;
        Phase isDoneWhen;

        Collection<ReplicaGroupInPhaseHolder> replicasInPhase;

        Map<Replica, ReplicaInPhaseHolder> replicaToReplicaInPhaseHolder = new HashMap<>();

        public ReplicaGroupsPhaseExecutor(Phase isDoneWhen, Collection<ReplicaGroupInPhaseHolder> replicasInPhase, Collection<Replica> allReplicas)
        {
            this.isDoneWhen = isDoneWhen;
            this.replicasInPhase = replicasInPhase;
            this.replicas = allReplicas;
            // TODO [MPP] Something needs to be done here.
            replicas.forEach(replica -> {
//                replicaToReplicaInPhaseHolder.put(replica, new ReplicaInPhaseHolder())
            });
        }

        boolean areInSamePhase() {
            return replicasInPhase.stream().map(h -> h.getReplicaGroup().currentPhase()).distinct().count() == 1;
        }

        /**
         * All replica groups MUST be in phase with replica group with minimum phase.
         * @return
         */
        Phase findMinimumPhaseAmongAllReplicaGroups() {
            return replicasInPhase.stream().map(h -> h.getReplicaGroup().currentPhase()).sorted().findFirst().get();
        }

        boolean isDone() {
            return areInSamePhase() && (findMinimumPhaseAmongAllReplicaGroups() == isDoneWhen);
        }

        void runTransitionsForEachReplica()
        {
            final Phase minimumPhaseAmongAllBeforeTransitions = findMinimumPhaseAmongAllReplicaGroups();

            final Map<ReplicaGroupInPhase, Phase> replicaGroupInPhaseToItsQuorumPhase = replicasInPhase.stream().map(h -> h.getReplicaGroup()).collect(Collectors.toMap(Function.identity(), rg -> {
                return getMaximumPhaseSharedByQuorum(rg.getReplicaGroup().getReplicasGroup());
            }));

            List<ReplicasGroup> replicaGroups = replicasInPhase.stream().map(h -> h.getReplicaGroup().getReplicaGroup().getReplicasGroup()).collect(Collectors.toList());

            logger.debug("Will run transitions for each replica. ReplicaGroups size: {}", replicaGroups.size());
            replicas.parallelStream().forEach(replica -> {
                Phase nextPhaseForReplica = findNextPhaseForReplica(replica, replicaGroups);
                TransitionId transitionId = transitionToPhase(replica.getPhase(), nextPhaseForReplica);
                logger.debug("Replica {} is in phase '{}' and next expected phase is {}. It will transition using {}",
                             replica.getHostAddress(),
                             replica.getPhase(),
                             nextPhaseForReplica,
                             transitionId);

                Transition transitionForReplica = findTransitionByTransitionId(transitionId);
                ReplicaInPhaseHolder holder = replica.getReplicaInPhaseHolder();
                ReplicaInPhase replicaInPhase = holder.getReplicaInPhase();
                TransitionResult transitionResult = transitionForReplica.transition(replicaInPhase);
                holder.setTransitionResult(transitionResult);
            });

            logger.debug("Transitions for replica groups are done. Time to evaluate responses");

            // After transition, some replicas could not transition forward. Their phase must be accepted.
            replicas.forEach(replica -> {
                TransitionResult transitionResult = replica.getReplicaInPhaseHolder().getTransitionResult();
                if(!transitionResult.wasTransitionSuccessful()) {
                    logger.debug("Replica {} failed to transition and will transition to phase {}", replica, transitionResult.phaseAfterTransition());
                    replica.getReplicaInPhaseHolder().setReplicaInPhase(transitionResult.getReplicaInPhase());
                    replica.setPhase(transitionResult.phaseAfterTransition());
                }
            });

            // For each replica group
            Map<Replica, Phase> replicaToProposedPhase = new HashMap<>();

            replicasInPhase.forEach(replicaGroupInPhaseHolder -> {
                    ReplicaGroupInPhase replicaGroup = replicaGroupInPhaseHolder.getReplicaGroup();
                    ReplicasGroupAndOwnedItems replicasGroupAndOwnedItems = replicaGroup.getReplicaGroup();
                    ReplicasGroup replicasGroup = replicasGroupAndOwnedItems.getReplicasGroup();
                    Phase quorumPhaseBeforeTransitions = replicaGroupInPhaseToItsQuorumPhase.get(replicaGroup);
                    Phase nextExpectedMinimumPhase = getNextExpectedPhase(minimumPhaseAmongAllBeforeTransitions);

                    List<Replica> allReplicasInThatGroup = replicasGroup.getAllReplicasInThatGroup();
                    long quorum = allReplicasInThatGroup.size() / 2 + 1;

                    List<TransitionResult> transitionResults = allReplicasInThatGroup.stream().map(Replica::getReplicaInPhaseHolder).map(x -> x.getTransitionResult()).collect(Collectors.toList());
                    Phase quorumPhaseAfterTransition = getMaximumPhaseSharedByQuorum(transitionResults.stream().map(x -> (WithPhase) x).collect(Collectors.toList()));

                    List<Replica> successfullyTransitioned = allReplicasInThatGroup.stream().filter(replica -> replica.getReplicaInPhaseHolder().getTransitionResult().wasTransitionSuccessful()).collect(Collectors.toList());

                    // Phase of replica that successfully transitioned will be set to minimum quorum phase among replica groups it belongs to
                    successfullyTransitioned.forEach(replica -> {
                        Phase phase = replicaToProposedPhase.get(replica);
                        if (phase == null || phase.ordinal() > quorumPhaseAfterTransition.ordinal())
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

            // After sorting out transition results, clear them
            replicas.forEach(replica -> {
                replica.getReplicaInPhaseHolder().setTransitionResult(null);
            });

//                if(successfullyTransitioned.size() >= quorum) {
//                    // quorum has successfully transitioned.
//
//
//
//                    // Replicas that succeeded can get phase from quorum.
//                    // Replicas that failed must get their actual phase after transition
//                }
//                else {
//                    // quorum didn't successfully transition.
//                }

//                // TODO [MPP] Below is code probably to be cut out.
//                if (quorumPhaseBeforeTransitions.ordinal() >= nextExpectedMinimumPhase.ordinal())
//                {
//                    // Quorum phase before transitions is same as next expected so there are no conditions
//                }
//                else
//                {
//                    // Quorum is smaller than next expected phase. So replicas had to transition.
//                    // Need to evaluate conditions before transitions.
//
//                    // If condition for transition is satified, then this replica group can propose new phase, but only to replicas which
//
//                    // are in phase no less than quorumPhaseBeforeTransitions.
//
//                    // Others are lagging behind.
//
//                    List<Replica> allReplicasInThatGroup = replicasGroup.getAllReplicasInThatGroup();
//                    long quorum = allReplicasInThatGroup.size() / 2 + 1;
//
//                    if (quorumPhaseBeforeTransitions == Phase.NO_PHASE && nextExpectedMinimumPhase == Phase.PRE_PREPARE_PHASE)
//                    {
//                        // TODO evaluate if quorum of replicas have defined paxosId
//                        List<Replica> replicasThatSuccessfullyPrepared = allReplicasInThatGroup.stream().filter(replica -> {
//                            return replica.getReplicaInPhaseHolder().getReplicaInPhase().getPaxosId().isPresent();
//                        }).collect(Collectors.toList());
//                        int countOfReplicasWithPaxosId = replicasThatSuccessfullyPrepared.size();
//
//                        if (countOfReplicasWithPaxosId >= quorum)
//                        {
//                            // Can propose that
//                        }
//                    }
//                    else if (quorumPhaseBeforeTransitions == Phase.PRE_PREPARE_PHASE && nextExpectedMinimumPhase == Phase.PREPARE_PHASE)
//                    {
//                        // It had to transition doing toPrepareTransition. Quorum should be "promised"
//                    }
//                    else if (quorumPhaseBeforeTransitions == Phase.PREPARE_PHASE && nextExpectedMinimumPhase == Phase.PROPOSE_PHASE)
//                    {
//                        // It had to do propose. Quorum should have accepted proposal
//                    }
//                    else if (quorumPhaseBeforeTransitions == Phase.PROPOSE_PHASE && nextExpectedMinimumPhase == Phase.COMMIT_PHASE)
//                    {
//                        // It had to commit, quorum get write success
//                    }
//                }
//            });


            // Assert ReplicaInPhase state in regard to a group.

            // Set ReplicaInPhase state to minimum state proposed by replica groups.

        }

        /**
         * Transitions are run on single replicas, but results of transitions are evaluated on replica groups.
         *
         * Replica groups might share some replicas.
         */
//        void runTransitionFrom(Phase groupsMinimumPhase) {
//            replicasInPhase.stream().forEach(replicaGroupHolder -> {
//                Phase thisGroupPhase = replicaGroupHolder.replicaGroup.currentPhase();
//                logger.debug("ReplicaGroup {} is in phase {} and current minimum phase is {} and next expected phase is {}",
//                             replicaGroupHolder.getReplicaGroup().getReplicaGroup().getReplicasGroup(),
//                             thisGroupPhase,
//                             groupsMinimumPhase,
//                             getNextExpectedPhase(groupsMinimumPhase));
//
//                TransitionId transitionId = findTransition(groupsMinimumPhase, thisGroupPhase);
//
//                // TODO [MPP] IMPLEMENT IT
//
//                Transition transitionForReplicaGroup = null;
//                // TODO [MPP] Maybe it will return Future<ReplicaGroupInPhase>
//                ReplicaGroupInPhase transitionedReplicaGroup = transitionForReplicaGroup.transition(replicaGroupHolder.getReplicaGroup());
//                replicaGroupHolder.setReplicaGroup(transitionedReplicaGroup);
//            });
//        }

        public void tryToExecute() {
            UUID txId = replicasInPhase.iterator().next().getReplicaGroup().getTransactionState().getTransactionId();
            logger.debug("ReplicaGroupsPhaseExecutor begins. Number of replica groups {}. Replica groups are: {}", replicasInPhase.size(),
                         replicasInPhase.stream().map(x -> x.getReplicaGroup().getReplicaGroup().getReplicasGroup()).collect(Collectors.toList()));

            final long start = System.nanoTime();
            consistencyForPaxos.validateForCas();
            long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getMppContentionTimeout());
            while (System.nanoTime() - start < timeout) {
                Phase minimumPhaseAmongAllReplicaGroups = findMinimumPhaseAmongAllReplicaGroups();
                logger.debug("Current groups minimum phase is {}", minimumPhaseAmongAllReplicaGroups);
                //runTransitionFrom(minimumPhaseAmongAllReplicaGroups);
                runTransitionsForEachReplica();

                if(isDone()) {
                    return;
                }

                Phase phaseAfterTransistion = findMinimumPhaseAmongAllReplicaGroups();
                logger.debug("Phase after transitions is {} TxID {}", phaseAfterTransistion, txId);
                if(phaseAfterTransistion.ordinal() <= minimumPhaseAmongAllReplicaGroups.ordinal()) {
                    logger.debug("Phase after transition is not the next phase. Will try again after sleep");
                    // Something didn't work well. Try again.
                    Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
                }
            }
            logger.error("MultiPartitionPaxosTimeoutException timeout occurred. TxID {}", txId);
            throw new MultiPartitionPaxosTimeoutException("Timeout occured. TxID is: " + txId);
            //throw new WriteTimeoutException(WriteType.CAS, consistencyForPaxos, 0, 0);
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

        if(responsesByReplica.size() == 1 && responsesByReplica.entrySet().iterator().next().getValue().isPresent()) {
            // success
            ReplicaInPrePreparedPhase replicaInPrePreparedPhase = new ReplicaInPrePreparedPhase(replicaInPhase.getTransactionState(), replicaInPhase.getState(), replicaInPhase.getReplica(), responsesByReplica, null);
            return new TransitionResultImpl(replicaInPrePreparedPhase, Phase.PRE_PREPARE_PHASE, true);
        }
        else {
            // failed transition
            return new TransitionResultImpl(replicaInPhase, Phase.NO_PHASE, false);
        }
    };

//    public static Transition transitionToBeginAndRepairPhase = replicaGroupInPhase -> {
//        Pair<List<InetAddress>, Integer> p = getPaxosParticipants(replicaGroupInPhase.getReplicaGroup(), consistencyForPaxos);
//        List<InetAddress> liveEndpoints = p.left;
//        int requiredParticipants = p.right;
//        // TODO Right now I skip paxos repair
//        return null;
//    };

    public static Transition transitionToPrepared = replicaInPhase -> {
        ReplicaInPrePreparedPhase inPhase = (ReplicaInPrePreparedPhase) replicaInPhase;

//        Pair<List<InetAddress>, Integer> p = getPaxosParticipants(replicaGroupInPhase.getReplicaGroup(), consistencyForPaxos);
//        List<InetAddress> liveEndpoints = p.left;
        List<InetAddress> liveEndpoints = Collections.singletonList(replicaInPhase.getReplica().getHost());
//        int requiredParticipants = p.right;
        int requiredParticipants = 1;
        MpPrepareCallback summary = inPhase.getSummary(); // TODO [MPP] This state needs to be available on "re-tried" transition.

        if(summary != null && summary.wasRolledBack())
        {
            throw new TransactionRolledBackException(inPhase.getTransactionState());
        }

        UUID ballot = createBallot(inPhase.getState(), summary);

        MpCommit toPrepare = MpCommit.newPrepare(inPhase.getTransactionState(), ballot);
        summary = preparePaxos(toPrepare, liveEndpoints, requiredParticipants, consistencyForPaxos, null);

//        // TODO [MPP] For testing
//        if(!waitingForTransactionUntilItIsInPrePrepared.isEmpty() && waitingForTransactionUntilItIsInPrePrepared.contains(replicaGroupInPhase.getTransactionState().getTransactionId())) {
//            if(waitingForTransactionUntilItIsInPrePrepared.equals(transactionsThatAreAfterPrePrepared)) {
//                logger.debug("all transactions waited for are in phase after PRE PREPARED");
//            }
//            else {
//                logger.debug("transition to prepared will wait for other transactions");
//                return new ReplicaGroupInPreparedPhase(inPhase.getReplicaGroup(),
//                                                       inPhase.getTransactionState(),
//                                                       inPhase.getState(),
//                                                       inPhase.getResponsesByReplica(),
//                                                       summary,
//                                                       liveEndpoints,
//                                                       requiredParticipants,
//                                                       ballot, true);
//            }
//        }
//        // END TODO [MPP] For Testing

        if (notPromised(summary))
        {
            Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
            logger.debug("PreparePaxos. Ballot {} wasn't accepted by replica {} TxId {}", ballot, inPhase.getReplica().getHostAddress(), inPhase.getTransactionState().getTransactionId());
            ReplicaInPrePreparedPhase newInPhase = inPhase.copyWith(summary);
            return new TransitionResultImpl(newInPhase, Phase.PRE_PREPARE_PHASE, false);
        }
        else {
            MpCommit inProgress = summary.mostRecentInProgressCommitWithUpdate;
            MpCommit mostRecent = summary.mostRecentCommit;

            logger.debug("PreparePaxos. Ballot {} was accepted by replica {} TxId {}", ballot, inPhase.getReplica().getHostAddress(), inPhase.getTransactionState().getTransactionId());

            if(inProgressNeedsToBeCompleted(inProgress, mostRecent) && !wasAlreadyRepaired(inPhase, inProgress) && !inProgressIsSelf(inPhase.getTransactionState(), inProgress.update)) {
                logger.debug("PreparePaxos. Found in progress proposal that needs to be completed. Proposal for TxId {}. TxId {}", inProgress.update.getTransactionId(), inPhase.getTransactionState().getTransactionId());
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
                // - responses by replica - Not OK - but do I even need it for something? TODO [MPP] I'll have to pass empty map.
                // - summary - OK - I just got it
                // - liveEndpoints - OK - I can find it.
                // - requiredParticipants - OK
                // - ballot - OK

                ReplicaGroupsPhaseExecutor repairInProgressPaxosExecutor = createRepairInProgressPaxosExecutor(inProgress.update, inPhase.getState(), ballot);
                // TODO [MPP] Run it in background ?
                // TODO [MPP] tryToExecute must return some sensible result, or future.

                // TODO [MPP] This Stage is incorrect, but I probably don't care.
                CompletableFuture<?> futureWhenRepairIsDone = StorageProxy.performLocally(() -> {
                    repairInProgressPaxosExecutor.tryToExecute();
                });

                ReplicaInReparingInProgressPhase repairing = new ReplicaInReparingInProgressPhase(
                                                                                                                                        inPhase.getTransactionState(),
                                                                                                                                        inPhase.getState(),
                                                                                                                                        inPhase.getReplica(),
                                                                                                                                        inPhase.getPaxosId(),
                                                                                                                                        summary,
//                                                                                                                                        liveEndpoints,
//                                                                                                                                        requiredParticipants,
                                                                                                                                        ballot, true,
                                                                                                                                        repairInProgressPaxosExecutor);

                futureWhenRepairIsDone.handleAsync((smth, ex) -> {
                    logger.debug("Nested in progress repair has completed with smth {} ex {}", String.valueOf(smth), String.valueOf(ex));
                    if(repairing.repairInProgressPaxosExecutor.equals(repairInProgressPaxosExecutor)) {
                        logger.debug("Marking replicaGroupInReparingInProgressPhase as done");
                        repairing.isRepairDone = true;
                    }
                    return null;
                });

                return new TransitionResultImpl(repairing, Phase.BEGIN_AND_REPAIR_PHASE, false);
            }
            else {
                // Nothing in progress that needs to be completed. Phase completes
                ReplicaInPreparedPhase newInPhase = new ReplicaInPreparedPhase(
                                                                                           inPhase.getTransactionState(),
                                                                                           inPhase.getState(),
                                                                                           inPhase.getReplica(),
                                                                                           inPhase.getPaxosId(),
                                                                                           summary,
//                                                                                           liveEndpoints,
//                                                                                           requiredParticipants,
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
        if(ReplicaInPreparedPhase.class.isInstance(inPhase)) {
            ReplicaInPreparedPhase inPreparedPhaseMaybeAfterRepair = (ReplicaInPreparedPhase) inPhase;
            return inPreparedPhaseMaybeAfterRepair.txRepairedAlready.isPresent() &&
                   inPreparedPhaseMaybeAfterRepair.txRepairedAlready.get().equals(inProgress.update.id());
        }
        else {
            return false;
        }
    }

    public static Transition transitionToPreparedFromReparing = replicaInPhase -> {
        // It just waits until nested repair is done.
        ReplicaInReparingInProgressPhase inPhase = (ReplicaInReparingInProgressPhase) replicaInPhase;
        if(inPhase.isRepairDone) {
            ReplicaInPhase otherReplicaInPhase = inPhase.repairInProgressPaxosExecutor.replicas.stream().filter(otherReplica -> otherReplica.equals(replicaInPhase.getReplica()))
                                                          .findFirst().map(r -> r.getReplicaInPhaseHolder().getReplicaInPhase()).get();

            ReplicaInPreparedPhase otherInPrepared = (ReplicaInPreparedPhase) otherReplicaInPhase;

            ReplicaInPreparedPhase newInPhase = new ReplicaInPreparedPhase(
                                                                                      inPhase.getTransactionState(),
                                                                                      inPhase.getState(),
                                                                                      inPhase.getReplica(),
                                                                                      inPhase.getPaxosId(),
                                                                                      inPhase.summary,
//                                                   inPhase.liveEndpoints,
//                                                   inPhase.requiredParticipants,
                                                                                      otherInPrepared.ballot, true,
                                                                                      Optional.of(otherInPrepared.getTransactionState().id()));
            return new TransitionResultImpl(newInPhase, Phase.PREPARE_PHASE, true);
        }
        else {
            return new TransitionResultImpl(inPhase, Phase.BEGIN_AND_REPAIR_PHASE, false);
        }
    };

    public static class ReplicaGroupInProposedPhase extends ReplicaGroupInPreparedPhase {

        MpCommit proposal;

        protected ReplicaGroupInProposedPhase(ReplicasGroupAndOwnedItems replicaGroup,
                                              TransactionState transactionState,
                                              ClientState state,
                                              Map<InetAddress, Optional<MpPaxosId>> responsesByReplica,
                                              MpPrepareCallback summary,
                                              List<InetAddress> liveEndpoints,
                                              int requiredParticipants,
                                              UUID ballot,
                                              boolean timeOutProposalIfPartialResponses,
                                              MpCommit proposal)
        {
            super(replicaGroup, transactionState, state, responsesByReplica, summary, liveEndpoints, requiredParticipants,
                  ballot,
                  timeOutProposalIfPartialResponses);
            this.proposal = proposal;
        }

//        public Phase currentPhase()
//        {
//            return Phase.PROPOSE_PHASE;
//        }

        public boolean isQuorum()
        {
            return true;  // Because quorum is determined in callback for proposePaxos.
        }

        public static ReplicaGroupInPhase fromInPrepared(ReplicaGroupInPreparedPhase inPrepared, MpCommit proposal)
        {
            return new ReplicaGroupInProposedPhase(inPrepared.getReplicaGroup(),
                                                   inPrepared.getTransactionState(),
                                                   inPrepared.getState(),
                                                   inPrepared.getResponsesByReplica(),
                                                   inPrepared.getSummary(),
                                                   inPrepared.liveEndpoints,
                                                   inPrepared.requiredParticipants,
                                                   inPrepared.ballot,
                                                   inPrepared.timeOutProposalIfPartialResponses,
                                                   proposal);
        }
    }

    public static class ReplicaInProposedPhase extends ReplicaInPreparedPhase {

        MpCommit proposal;

        protected ReplicaInProposedPhase(TransactionState transactionState,
                                              ClientState state,
                                              Replica replica,
                                              Optional<MpPaxosId> paxosId,
                                              MpPrepareCallback summary,
//                                              List<InetAddress> liveEndpoints,
//                                              int requiredParticipants,
                                              UUID ballot,
                                              boolean timeOutProposalIfPartialResponses,
                                              MpCommit proposal)
        {
            super(transactionState, state, replica, paxosId, summary,
                  ballot,
                  timeOutProposalIfPartialResponses);
            this.proposal = proposal;
        }

//        public Phase currentPhase()
//        {
//            return Phase.PROPOSE_PHASE;
//        }

//        public boolean isQuorum()
        {
//            return true;  // Because quorum is determined in callback for proposePaxos.
        }

        public static ReplicaInPhase fromInPrepared(ReplicaInPreparedPhase inPrepared, MpCommit proposal)
        {
            return new ReplicaInProposedPhase(inPrepared.getTransactionState(),
                                                   inPrepared.getState(),
                                                   inPrepared.getReplica(),
                                                   inPrepared.getPaxosId(),
                                                   inPrepared.getSummary(),
//                                                   inPrepared.liveEndpoints,
//                                                   inPrepared.requiredParticipants,
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
        boolean proposed = proposePaxos(proposal, participants, 1, inPrepared.timeOutProposalIfPartialResponses, consistencyForPaxos, null);

        logger.debug("PaxosProposed response is {} for replica {}", proposed, replicaInPhase.getReplica());

        if(proposed) {
            // then create ReplicaGroupInProposedState
            ReplicaInPhase newInPhase = ReplicaInProposedPhase.fromInPrepared(inPrepared, proposal);
            return new TransitionResultImpl(newInPhase, Phase.PROPOSE_PHASE, true);
        }
        else {

            ReplicaInPrePreparedPhase newInPhase = new ReplicaInPrePreparedPhase(
                                                                                                               inPrepared.getTransactionState(),
                                                                                                               inPrepared.getState(),
                                                                                                               inPrepared.getReplica(),
                                                                                                               inPrepared.getPaxosId(),
                                                                                                               inPrepared.getSummary());
            return new TransitionResultImpl(newInPhase, Phase.PRE_PREPARE_PHASE, false);
        }
    };

    /**
     *  Can transition to 'commit', only from 'proposed'
     */
    public static Transition transitionToCommitted = replicaInPhase -> {
        ReplicaInProposedPhase inProposed = (ReplicaInProposedPhase) replicaInPhase;
        commitPaxos3(inProposed.proposal, consistencyForCommit, true, replicaInPhase.getReplica());
        // TODO [MPP] Maybe I need to check for some error handling, but in principle it should work.

        Tracing.trace("CAS successful");
        BaseReplicaInPhase newInPhase = new BaseReplicaInPhase(replicaInPhase.getTransactionState(),
                                                                       replicaInPhase.getState(),
                                                                       replicaInPhase.getReplica(),
                                                                       null);
        return new TransitionResultImpl(newInPhase, Phase.AFTER_COMMIT_PHASE, true);
    };

    static {
        nextPhaseIs = new HashMap<>();

        nextPhaseIs.put(Phase.NO_PHASE, Phase.PRE_PREPARE_PHASE);
        nextPhaseIs.put(Phase.PRE_PREPARE_PHASE, Phase.PREPARE_PHASE);
        nextPhaseIs.put(Phase.BEGIN_AND_REPAIR_PHASE, Phase.PREPARE_PHASE);
        nextPhaseIs.put(Phase.PREPARE_PHASE, Phase.PROPOSE_PHASE);
        nextPhaseIs.put(Phase.PROPOSE_PHASE, Phase.COMMIT_PHASE);
        nextPhaseIs.put(Phase.COMMIT_PHASE, Phase.AFTER_COMMIT_PHASE);

        transitionToPhase = new HashMap<>();
        transitionToPhase.put(Phase.PRE_PREPARE_PHASE, transitionToPrePrepared);
        transitionToPhase.put(Phase.PREPARE_PHASE, transitionToPrepared);
        transitionToPhase.put(Phase.PROPOSE_PHASE, transitionToProposed);
        transitionToPhase.put(Phase.COMMIT_PHASE, transitionToCommitted);
    }

    /**
     * @param fromPhase
     * @param toPhase
     * @return transition which moves from {@param fromPhase} to {@param toPhase}
     */
    public static TransitionId transitionToPhase(Phase fromPhase, Phase toPhase) {
        if(fromPhase == toPhase) {
            return TransitionId.NOOP;
        }
        else if(fromPhase == Phase.NO_PHASE && toPhase == Phase.PRE_PREPARE_PHASE) {
//            return transitionToPrePrepared;
            return TransitionId.TO_PRE_PREPARED;
        }
        else if (fromPhase == Phase.PRE_PREPARE_PHASE && toPhase == Phase.PREPARE_PHASE) {
//            return transitionToPrepared;
            return TransitionId.TO_PREPARED;
        }
        else if (fromPhase == Phase.PREPARE_PHASE && toPhase == Phase.PREPARE_PHASE) {
//            return transitionToPrepared; // TODO [MPP] This will make replica group refresh its ballot, could be NOOP operation.
            return TransitionId.TO_PREPARED;
        }
        else if (fromPhase == Phase.BEGIN_AND_REPAIR_PHASE && toPhase == Phase.PREPARE_PHASE) {
//            return transitionToPreparedFromReparing;
            return TransitionId.TO_PREPARED_FROM_REPARING;
        }
        else if (fromPhase == Phase.PREPARE_PHASE && toPhase == Phase.PROPOSE_PHASE) {
//            return transitionToProposed;
            return TransitionId.TO_PROPOSED;
        }
        else if (fromPhase == Phase.PROPOSE_PHASE && toPhase == Phase.COMMIT_PHASE) {
            return TransitionId.TO_COMMITTED;
//            return transitionToCommitted;
        }
        else if (toPhase == Phase.PREPARE_PHASE) {
            // ignoring current phase, because it just has to go back couple of phases.
            return TransitionId.TO_PREPARED;
//            return transitionToPrepared;
        }
        else {
            throw new RuntimeException("Unexpected transition required from phase " + fromPhase + " to phase " + toPhase);
        }
    }

    public static TransitionId findTransition(Phase groupsMinimumPhase, Phase thisGroupPhase) {
        Phase nextCommonPhase = getNextExpectedPhase(groupsMinimumPhase);
        return transitionToPhase(thisGroupPhase, nextCommonPhase);
    }

    public void multiPartitionPaxos(TransactionState transactionState, ReplicasGroupAndOwnedItems replicasGroupAndOwnedItems,
                                 ReplicasGroupsOperationCallback replicasGroupsOperationCallback, ClientState state) {
        ConsistencyLevel consistencyForCommit = ConsistencyLevel.QUORUM;
        ConsistencyLevel consistencyForPaxos = ConsistencyLevel.LOCAL_TRANSACTIONAL;

        final long start = System.nanoTime();
        int contentions = 0;
        try
        {
            consistencyForPaxos.validateForCas();
//            consistencyForCommit.validateForCasCommit(keyspaceName);

            long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getMppContentionTimeout());
            while (System.nanoTime() - start < timeout)
            {
                // for simplicity, we'll do a single liveness check at the start of each attempt
                Pair<List<InetAddress>, Integer> p = getPaxosParticipants(replicasGroupAndOwnedItems, consistencyForPaxos);
                List<InetAddress> liveEndpoints = p.left;
                int requiredParticipants = p.right;

                final Pair<UUID, Integer> pair = beginAndRepairPaxos(start, transactionState, liveEndpoints, requiredParticipants, consistencyForPaxos, consistencyForCommit, true, state);
                final UUID ballot = pair.left;
                contentions += pair.right;

                MpCommit proposal = MpCommit.newProposal(ballot, transactionState);
                Tracing.trace("Multi partition paxos; proposing client-requested transaction state for {}", ballot);
                if (proposePaxos(proposal, liveEndpoints, requiredParticipants, true, consistencyForPaxos, replicasGroupsOperationCallback))
                {
                    commitPaxos(proposal, consistencyForCommit, true);
                    Tracing.trace("CAS successful");
                    return;
                }

                Tracing.trace("Paxos proposal not accepted (pre-empted by a higher ballot)");
                contentions++;
                Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
                // continue to retry
            }

//            throw new WriteTimeoutException(WriteType.CAS, consistencyForPaxos, 0, consistencyForPaxos.blockFor(Keyspace.open(keyspaceName)));
            throw new WriteTimeoutException(WriteType.CAS, consistencyForPaxos, 0, 0);
        }
        catch (WriteTimeoutException|ReadTimeoutException e)
        {
            casWriteMetrics.timeouts.mark();
            throw e;
        }
        catch (WriteFailureException |ReadFailureException e)
        {
            casWriteMetrics.failures.mark();
            throw e;
        }
        catch(UnavailableException e)
        {
            casWriteMetrics.unavailables.mark();
            throw e;
        }
        finally
        {
            if(contentions > 0)
                casWriteMetrics.contention.update(contentions);
            casWriteMetrics.addNano(System.nanoTime() - start);
        }

    }

    /**
     * begin a Paxos session by sending a prepare request and completing any in-progress requests seen in the replies
     *
     * @return the Paxos ballot promised by the replicas if no in-progress requests were seen and a quorum of
     * nodes have seen the mostRecentCommit.  Otherwise, return null.
     */
    private static Pair<UUID, Integer> beginAndRepairPaxos(long start,
                                                           TransactionState transactionState,
                                                           List<InetAddress> liveEndpoints,
                                                           int requiredParticipants,
                                                           ConsistencyLevel consistencyForPaxos,
                                                           ConsistencyLevel consistencyForCommit,
                                                           final boolean isWrite,
                                                           ClientState state)
    throws WriteTimeoutException, WriteFailureException
    {
        long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getCasContentionTimeout());

        MpPrepareCallback summary = null;
        int contentions = 0;
        while (System.nanoTime() - start < timeout)
        {
            // We want a timestamp that is guaranteed to be unique for that node (so that the ballot is globally unique), but if we've got a prepare rejected
            // already we also want to make sure we pick a timestamp that has a chance to be promised, i.e. one that is greater that the most recently known
            // in progress (#5667). Lastly, we don't want to use a timestamp that is older than the last one assigned by ClientState or operations may appear
            // out-of-order (#7801).
            UUID ballot = createBallot(state, summary);

            // prepare
            Tracing.trace("Preparing multi partition paxos {}", ballot);
            MpCommit toPrepare = MpCommit.newPrepare(transactionState, ballot);
//            summary = preparePaxos(toPrepare, liveEndpoints, requiredParticipants, consistencyForPaxos, replicasGroupOperationCallback);
            summary = preparePaxos(toPrepare, liveEndpoints, requiredParticipants, consistencyForPaxos, null);
            if (notPromised(summary))
            {
                Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
                contentions++;
                // sleep a random amount to give the other proposer a chance to finish
                Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
                // try again
                continue;
            }

            MpCommit inProgress = summary.mostRecentInProgressCommitWithUpdate;
            MpCommit mostRecent = summary.mostRecentCommit;

            if (inProgressNeedsToBeCompleted(inProgress, mostRecent))
            {
                Tracing.trace("Finishing incomplete paxos round {}", inProgress);
                statistics(isWrite);
                MpCommit refreshedInProgress = MpCommit.newProposal(ballot, inProgress.update);
//                if (proposePaxos(refreshedInProgress, liveEndpoints, requiredParticipants, false, consistencyForPaxos, replicasGroupOperationCallback))
                if (proposePaxos(refreshedInProgress, liveEndpoints, requiredParticipants, false, consistencyForPaxos, null))
                {
                    try
                    {
                        commitPaxos(refreshedInProgress, consistencyForCommit, false);
                    }
                    catch (WriteTimeoutException e)
                    {
                        // We're still doing preparation for the paxos rounds, so we want to use the CAS (see CASSANDRA-8672)
                        throw new WriteTimeoutException(WriteType.CAS, e.consistency, e.received, e.blockFor);
                    }
                }
                else
                {
                    Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
                    // sleep a random amount to give the other proposer a chance to finish
                    contentions++;
                    Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
                }
                continue;
            }

            // To be able to propose our value on a new round, we need a quorum of replica to have learn the previous one. Why is explained at:
            // https://issues.apache.org/jira/browse/CASSANDRA-5062?focusedCommentId=13619810&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-13619810)
            // Since we waited for quorum nodes, if some of them haven't seen the last commit (which may just be a timing issue, but may also
            // mean we lost messages), we pro-actively "repair" those nodes, and retry.
            Iterable<InetAddress> missingMRC = summary.replicasMissingMostRecentCommit();
            if (Iterables.size(missingMRC) > 0)
            {
//                Tracing.trace("Repairing replicas that missed the most recent commit");
                sendCommit(mostRecent, missingMRC);
                // TODO: provided commits don't invalid the prepare we just did above (which they don't), we could just wait
                // for all the missingMRC to acknowledge this commit and then move on with proposing our value. But that means
                // adding the ability to have commitPaxos block, which is exactly CASSANDRA-5442 will do. So once we have that
                // latter ticket, we can pass CL.ALL to the commit above and remove the 'continue'.
                continue;
            }

            return Pair.create(ballot, contentions);
        }

//        throw new WriteTimeoutException(WriteType.CAS, consistencyForPaxos, 0, consistencyForPaxos.blockFor(Keyspace.open(metadata.ksName)));
        throw new WriteTimeoutException(WriteType.CAS, consistencyForPaxos, 0, 0);
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
        if(isWrite)
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
        logger.debug("preparePaxos. for Tx ID {} Targeted endpoints are {}. Required participants {}", toPrepare.update.getTransactionId(),  endpoints, requiredParticipants, requiredParticipants);
        MpPrepareCallback callback = new MpPrepareCallback(toPrepare.update, requiredParticipants, consistencyForPaxos, replicasGroupOperationCallback);
        MessageOut<MpCommit> message = new MessageOut<>(MessagingService.Verb.MP_PAXOS_PREPARE, toPrepare, MpCommit.serializer);
        for (InetAddress target : endpoints) {
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
        for (InetAddress target : endpoints) {
            logger.debug("Sending MP_PAXOS_PROPOSE message to target {} with ballot {} proposing transaction with id {}", target, proposal.ballot, proposal.update.id());
            MessagingService.instance().sendRR(message, target, callback);
        }
        logger.debug("Awaiting MP_PAXOS_PROPOSE callback for tx id {}", proposal.update.getTransactionId());
        callback.await();
        if(callback.wasRolledBack()) {
            logger.info("Callback from proposal says that transaction id {} was rolled back. Throwing excpetion", proposal.update.getTransactionId());
            throw new TransactionRolledBackException(proposal.update);
        }
        if (callback.isSuccessful()) {
            logger.debug("MP_PAXOS_PROPOSE callback was successful. Tx ID {}", proposal.update.getTransactionId());
            return true;
        }

        if (timeoutIfPartial && !callback.isFullyRefused())
            throw new WriteTimeoutException(WriteType.CAS, consistencyLevel, callback.getAcceptCount(), requiredParticipants);
        logger.debug("MP_PAXOS_PROPOSE callback was not successful, returning false. Tx ID {}", proposal.update.getTransactionId());
        return false;
    }

    private static void commitPaxos(MpCommit proposal, ConsistencyLevel consistencyLevel, boolean shouldHint) throws WriteTimeoutException
    {
//        boolean shouldBlock = consistencyLevel != ConsistencyLevel.ANY;
//        Keyspace keyspace = Keyspace.open(proposal.update.metadata().ksName);

//        Token tk = proposal.update.partitionKey().getToken();
//        List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspace.getName(), tk);
//        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspace.getName());

//        AbstractWriteResponseHandler<MpCommit> responseHandler = null;
//        if (shouldBlock)
//        {
//            AbstractReplicationStrategy rs = keyspace.getReplicationStrategy();
//            responseHandler = rs.getWriteResponseHandler(naturalEndpoints, pendingEndpoints, consistencyLevel, null, WriteType.SIMPLE);
//        }

//        commitPaxosInternal(proposal, shouldHint, shouldBlock, naturalEndpoints, pendingEndpoints, responseHandler);
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
//        boolean shouldBlock = consistencyLevel != ConsistencyLevel.ANY;
//        Keyspace keyspace = Keyspace.open(proposal.update.metadata().ksName);
//
//        Token tk = proposal.update.partitionKey().getToken();
        List<InetAddress> naturalEndpoints = replicasGroupAndItems.getReplicasGroup().getReplicas();
        Collection<InetAddress> pendingEndpoints = Collections.emptyList();

        AbstractWriteResponseHandler<MpCommit> responseHandler = new MppCommitWriteResponseHandler<>(naturalEndpoints,
                                                                                                     pendingEndpoints,
                                                                                                   consistencyLevel, null, null, WriteType.SIMPLE);
//        if (shouldBlock)
//        {
//            AbstractReplicationStrategy rs = keyspace.getReplicationStrategy();
//            responseHandler = rs.getWriteResponseHandler(naturalEndpoints, pendingEndpoints, consistencyLevel, null, WriteType.SIMPLE);
//        }
        boolean shouldBlockOverride = true; // TODO [MPP]
        commitPaxosInternal(proposal, shouldHint, shouldBlockOverride, naturalEndpoints, pendingEndpoints, responseHandler);
    }

    private static void commitPaxosInternal(MpCommit proposal, boolean shouldHint, boolean shouldBlock, List<InetAddress> naturalEndpoints, Collection<InetAddress> pendingEndpoints, AbstractWriteResponseHandler<MpCommit> responseHandler)
    {
        MessageOut<MpCommit> message = new MessageOut<>(MessagingService.Verb.MP_PAXOS_COMMIT, proposal, MpCommit.serializer);
        List<InetAddress> allEndpoints = ImmutableList.copyOf(Iterables.concat(naturalEndpoints, pendingEndpoints));

        Map<InetAddress, List<MppHint>> replicaToHints = new HashMap<>();

        // If should hint then send commit wrapped in hint.
        if(shouldHint) {
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
                if(replicaToHints.containsKey(destination)) {
                    // replica has hints for dead replicas so send commit with hints.
                    List<MppHint> hints = replicaToHints.get(destination);
                    MessageOut<MpCommitWithHints> messageWithHints = new MessageOut<>(MessagingService.Verb.MP_PAXOS_COMMIT_WITH_HINTS, new MpCommitWithHints(proposal, hints), MpCommitWithHints.serializer);
                    if (shouldBlock)
                        MessagingService.instance().sendRR(messageWithHints, destination, responseHandler, shouldHint);
                    else
                        MessagingService.instance().sendOneWay(messageWithHints, destination);
                }
                else {
                    // send commit without hints.
                    if (shouldBlock)
                        MessagingService.instance().sendRR(message, destination, responseHandler, shouldHint);
                    else
                        MessagingService.instance().sendOneWay(message, destination);
                }
            }
        }
        else {
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
     *
     * @return list of participants, required participants
     */
    private static Pair<List<InetAddress>, Integer> getPaxosParticipants(ReplicasGroupAndOwnedItems replicaGroup, ConsistencyLevel consistencyForPaxos) throws UnavailableException {
        List<InetAddress> liveReplicas = replicaGroup.getReplicasGroup().getReplicas();
        TransactionItem transactionItem = replicaGroup.getOwnedItems().iterator().next();
        int replicationFactor = Keyspace.open(transactionItem.getKsName()).getReplicationStrategy().getReplicationFactor();
        int requiredParticipants = replicationFactor / 2 + 1;

        if(liveReplicas.size() < requiredParticipants) {
            throw new UnavailableException(consistencyForPaxos, requiredParticipants, liveReplicas.size());
        }

        return Pair.create(liveReplicas, requiredParticipants);
    }
}
