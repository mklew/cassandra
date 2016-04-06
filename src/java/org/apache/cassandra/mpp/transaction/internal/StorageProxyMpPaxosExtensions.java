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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.metrics.CASClientRequestMetrics;
import org.apache.cassandra.mpp.MppServicesLocator;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.paxos.MpPaxosId;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MppCommitWriteResponseHandler;
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

    enum Phase
    {
        NO_PHASE,
        PRE_PREPARE_PHASE,
        PREPARE_BEGIN_AND_REPAIR_PHASE,
        PROPOSE_BEGIN_AND_REPAIR_PHASE,
        COMMIT_BEGIN_AND_REPAIR_PHASE,
        PREPARE_PHASE,
        PROPOSE_PHASE,
        COMMIT_PHASE,
        AFTER_COMMIT_PHASE;
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

    interface ReplicaGroupInPhase {
        Phase currentPhase();

        ReplicasGroupAndOwnedItems getReplicaGroup();

        boolean isQuorum();

        TransactionState getTransactionState();

        ClientState getState();
    }

    static abstract class AbstractReplicaGroupInPhase implements ReplicaGroupInPhase {
        protected ReplicasGroupAndOwnedItems replicaGroup;
        protected TransactionState transactionState;
        protected ClientState state;

        protected AbstractReplicaGroupInPhase(ReplicasGroupAndOwnedItems replicaGroup, TransactionState transactionState, ClientState state)
        {
            this.replicaGroup = replicaGroup;
            this.transactionState = transactionState;
            this.state = state;
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

    public static class BaseReplicaGroupInPhase extends AbstractReplicaGroupInPhase {

        private final Phase phase;

        protected BaseReplicaGroupInPhase(ReplicasGroupAndOwnedItems replicaGroup, TransactionState transactionState, ClientState state, Phase phase)
        {
            super(replicaGroup, transactionState, state);
            this.phase = phase;
        }

        @Override
        public Phase currentPhase()
        {
            return phase;
        }

        @Override
        public boolean isQuorum()
        {
            return true;
        }
    }

    public static class ReplicaGroupInPrePreparedPhase extends AbstractReplicaGroupInPhase {

        Map<InetAddress, Optional<MpPaxosId>> responsesByReplica;

        MpPrepareCallback summary;

        public ReplicaGroupInPrePreparedPhase(ReplicasGroupAndOwnedItems replicaGroup,
                                       TransactionState transactionState, ClientState state,
                                       Map<InetAddress, Optional<MpPaxosId>> responsesByReplica, MpPrepareCallback summary)
        {
            super(replicaGroup, transactionState, state);
            this.responsesByReplica = responsesByReplica;
            this.summary = summary;
        }

        public ReplicaGroupInPrePreparedPhase copyWith(MpPrepareCallback summary) {
            return new ReplicaGroupInPrePreparedPhase(replicaGroup, transactionState, state, responsesByReplica, summary);
        }

        @Override
        public Phase currentPhase()
        {
            return Phase.PRE_PREPARE_PHASE;
        }

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

        protected ReplicaGroupInPreparedPhase(ReplicasGroupAndOwnedItems replicaGroup,
                                              TransactionState transactionState,
                                              ClientState state,
                                              Map<InetAddress, Optional<MpPaxosId>> responsesByReplica,
                                              MpPrepareCallback summary,
                                              List<InetAddress> liveEndpoints,
                                              int requiredParticipants,
                                              UUID ballot)
        {
            super(replicaGroup, transactionState, state, responsesByReplica, summary);
            this.liveEndpoints = liveEndpoints;
            this.requiredParticipants = requiredParticipants;
            this.ballot = ballot;
        }

        @Override
        public Phase currentPhase()
        {
            return Phase.PREPARE_PHASE;
        }

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

    interface Transition {
        ReplicaGroupInPhase transition(ReplicaGroupInPhase replicaGroup);
    }

    public static Transition getNextTransition(Phase phase) {
        Phase nextPhase = getNextExpectedPhase(phase);
        return transitionToPhase.get(nextPhase);
    }

    public static ReplicaGroupsPhaseExecutor createMultiPartitionPaxosPhaseExecutor(List<ReplicasGroupAndOwnedItems> replicaGroups, TransactionState transactionState, ClientState state) {
        logger.debug("createMultiPartitionPaxosPhaseExecutor replicaGroups {}. TransactionState is {}", replicaGroups, transactionState);
        List<ReplicaGroupInPhaseHolder> inPhases = replicaGroups.stream()
                                                              .map(rg -> new BaseReplicaGroupInPhase(rg, transactionState, state, Phase.NO_PHASE))
                                                              .map(ReplicaGroupInPhaseHolder::new)
                                                              .collect(Collectors.toList());

        return new ReplicaGroupsPhaseExecutor(Phase.AFTER_COMMIT_PHASE, inPhases);
    }


    public static class ReplicaGroupsPhaseExecutor {

        Phase isDoneWhen;

        Collection<ReplicaGroupInPhaseHolder> replicasInPhase;

        public ReplicaGroupsPhaseExecutor(Phase isDoneWhen, Collection<ReplicaGroupInPhaseHolder> replicasInPhase)
        {
            this.isDoneWhen = isDoneWhen;
            this.replicasInPhase = replicasInPhase;
        }

        boolean areInSamePhase() {
            return replicasInPhase.stream().map(h -> h.getReplicaGroup().currentPhase()).distinct().count() == 1;
        }

        Phase findMinimumPhase() {
            return replicasInPhase.stream().map(h -> h.getReplicaGroup().currentPhase()).sorted().findFirst().get();
        }

        boolean isDone() {
            return areInSamePhase() && (findMinimumPhase() == isDoneWhen);
        }

        void runTransition(Transition transition) {
            replicasInPhase.stream().forEach(replicaGroupHolder -> {
                // TODO [MPP] Maybe it will return Future<ReplicaGroupInPhase>
                ReplicaGroupInPhase transitionedReplicaGroup = transition.transition(replicaGroupHolder.getReplicaGroup());

                replicaGroupHolder.setReplicaGroup(transitionedReplicaGroup);
            });
        }

        public void tryToExecute() {
            logger.debug("ReplicaGroupsPhaseExecutor begins. Number of replica groups {}. Replica groups are: {}", replicasInPhase.size(),
                         replicasInPhase.stream().map(x -> x.getReplicaGroup().getReplicaGroup().getReplicasGroup()).collect(Collectors.toList()));

            final long start = System.nanoTime();
            consistencyForPaxos.validateForCas();
            long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getMppContentionTimeout());
            while (System.nanoTime() - start < timeout) {
                Phase startPhase = findMinimumPhase();
                logger.debug("Start phase is {}", startPhase);
                Transition nextTransition = getNextTransition(startPhase);
                logger.debug("Running transition");
                runTransition(nextTransition);


                if(isDone()) {
                    return;
                }

                Phase phaseAfterTransistion = findMinimumPhase();
                logger.debug("Phase after transition is {}", phaseAfterTransistion);
                if(phaseAfterTransistion.ordinal() <= startPhase.ordinal()) {
                    logger.debug("Phase after transition is not the next phase. Will try again after sleep");
                    // Something didn't work well. Try again.
                    Uninterruptibles.sleepUninterruptibly(ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
                }
            }
        }

    }

    public static ConsistencyLevel consistencyForCommit = ConsistencyLevel.QUORUM;
    public static ConsistencyLevel consistencyForPaxos = ConsistencyLevel.LOCAL_TRANSACTIONAL;

    public static Transition toPrePreparePhaseTransition = replicaGroup -> {
        Preconditions.checkState(replicaGroup.currentPhase() == Phase.NO_PHASE);
        // TODO [MPP] Logic from MppServicesLocator should be moved elsewhere.
        MpPrePrepareMpPaxosCallback callback = ((MppServiceImpl) MppServicesLocator.getInstance()).prePrepareReplicaGroup(replicaGroup.getTransactionState(), replicaGroup.getReplicaGroup(), null);
        callback.await();

        Map<InetAddress, Optional<MpPaxosId>> responsesByReplica = callback.getResponsesByReplica();

        return new ReplicaGroupInPrePreparedPhase(replicaGroup.getReplicaGroup(), replicaGroup.getTransactionState(), replicaGroup.getState(), responsesByReplica, null);
    };

    public static Transition transitionToBeginAndRepairPhase = replicaGroupInPhase -> {
        Pair<List<InetAddress>, Integer> p = getPaxosParticipants(replicaGroupInPhase.getReplicaGroup(), consistencyForPaxos);
        List<InetAddress> liveEndpoints = p.left;
        int requiredParticipants = p.right;
        // TODO Right now I skip paxos repair
        return null;
    };

    public static Transition transitionToPrepared = replicaGroupInPhase -> {
        ReplicaGroupInPrePreparedPhase inPhase = (ReplicaGroupInPrePreparedPhase) replicaGroupInPhase;

        Pair<List<InetAddress>, Integer> p = getPaxosParticipants(replicaGroupInPhase.getReplicaGroup(), consistencyForPaxos);
        List<InetAddress> liveEndpoints = p.left;
        int requiredParticipants = p.right;
        MpPrepareCallback summary = inPhase.getSummary(); // TODO [MPP] This state needs to be available on "re-tried" transition.

        UUID ballot = createBallot(inPhase.getState(), summary);

        MpCommit toPrepare = MpCommit.newPrepare(inPhase.getTransactionState(), ballot);
        summary = preparePaxos(toPrepare, liveEndpoints, requiredParticipants, consistencyForPaxos, null);
        if (notPromised(summary))
        {
            Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
            return inPhase.copyWith(summary);
        }
        else {
            MpCommit inProgress = summary.mostRecentInProgressCommitWithUpdate;
            MpCommit mostRecent = summary.mostRecentCommit;
            return new ReplicaGroupInPreparedPhase(inPhase.getReplicaGroup(),
                                                   inPhase.getTransactionState(),
                                                   inPhase.getState(),
                                                   inPhase.getResponsesByReplica(),
                                                   inPhase.getSummary(),
                                                   liveEndpoints,
                                                   requiredParticipants,
                                                   ballot);
        }
    };

    // TODO [MPP] It needs to extend from ReplicaGroupInPreparedPhase because if one replica group gets proposal and other doesn't
    // then both of them start at prepare. So next phase MUST extend previous phases.
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
                                              MpCommit proposal)
        {
            super(replicaGroup, transactionState, state, responsesByReplica, summary, liveEndpoints, requiredParticipants, ballot);
            this.proposal = proposal;
        }

        public Phase currentPhase()
        {
            return Phase.PROPOSE_PHASE;
        }

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
                                                   proposal);
        }
    }

    /**
     * Can transition to 'proposed', only from 'prepared'.
     */
    public static Transition transitionToProposed = replicaGroupInPhase -> {
        // Replica Group in Phase should be prepared
        ReplicaGroupInPreparedPhase inPrepared = (ReplicaGroupInPreparedPhase) replicaGroupInPhase;
        UUID ballot = inPrepared.ballot;
        MpCommit proposal = MpCommit.newProposal(ballot, replicaGroupInPhase.getTransactionState());
        Tracing.trace("Multi partition paxos; proposing client-requested transaction state for {}", ballot);

        boolean proposed = proposePaxos(proposal, inPrepared.liveEndpoints, inPrepared.requiredParticipants, true, consistencyForPaxos, null);

        logger.debug("PaxosProposed response is {} for replica group {}", proposed, replicaGroupInPhase.getReplicaGroup().getReplicasGroup());

        if(proposed) {
            // then create ReplicaGroupInProposedState
            return ReplicaGroupInProposedPhase.fromInPrepared(inPrepared, proposal);
        }
        else {
            return new ReplicaGroupInPrePreparedPhase(inPrepared.getReplicaGroup(),
                                                      inPrepared.getTransactionState(),
                                                      inPrepared.getState(),
                                                      inPrepared.getResponsesByReplica(),
                                                      inPrepared.getSummary());
        }
    };

    /**
     *  Can transition to 'commit', only from 'proposed'
     */
    public static Transition transitionToCommitted = replicaGroupInPhase -> {
        ReplicaGroupInProposedPhase inProposed = (ReplicaGroupInProposedPhase) replicaGroupInPhase;
        commitPaxos2(inProposed.proposal, consistencyForCommit, true, replicaGroupInPhase.getReplicaGroup());
        Tracing.trace("CAS successful");
        return new BaseReplicaGroupInPhase(replicaGroupInPhase.getReplicaGroup(),
                                           replicaGroupInPhase.getTransactionState(),
                                           replicaGroupInPhase.getState(),
                                           Phase.AFTER_COMMIT_PHASE);
    };

    static {
        nextPhaseIs = new HashMap<>();

        nextPhaseIs.put(Phase.NO_PHASE, Phase.PRE_PREPARE_PHASE);
        nextPhaseIs.put(Phase.PRE_PREPARE_PHASE, Phase.PREPARE_PHASE); // TODO [MPP] SKIPPED BEGIN AND REPAIR
        nextPhaseIs.put(Phase.PREPARE_PHASE, Phase.PROPOSE_PHASE);
        nextPhaseIs.put(Phase.PROPOSE_PHASE, Phase.COMMIT_PHASE);
        nextPhaseIs.put(Phase.COMMIT_PHASE, Phase.AFTER_COMMIT_PHASE);

        transitionToPhase = new HashMap<>();
        transitionToPhase.put(Phase.PRE_PREPARE_PHASE, toPrePreparePhaseTransition);
        transitionToPhase.put(Phase.PREPARE_PHASE, transitionToPrepared);
        transitionToPhase.put(Phase.PROPOSE_PHASE, transitionToProposed);
        transitionToPhase.put(Phase.COMMIT_PHASE, transitionToCommitted);
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
        logger.debug("preparePaxos. Targeted endpoints are {}. Required participants {}", endpoints, requiredParticipants, requiredParticipants);
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
        logger.debug("proposePaxos begins. Targeted endpoints are {}. Required participants {}. Consistency Level {}", endpoints, requiredParticipants, consistencyLevel);
        MpProposeCallback callback = new MpProposeCallback(endpoints.size(), requiredParticipants, !timeoutIfPartial, consistencyLevel, replicasGroupOperationCallback);
        MessageOut<MpCommit> message = new MessageOut<>(MessagingService.Verb.MP_PAXOS_PROPOSE, proposal, MpCommit.serializer);
        for (InetAddress target : endpoints) {
            logger.debug("Sending MP_PAXOS_PROPOSE message to target {} with ballot {} proposing transaction with id {}", target, proposal.ballot, proposal.update.id());
            MessagingService.instance().sendRR(message, target, callback);
        }
        logger.debug("Awaiting MP_PAXOS_PROPOSE callback");
        callback.await();

        if (callback.isSuccessful()) {
            logger.debug("MP_PAXOS_PROPOSE callback was successful");
            return true;
        }

        if (timeoutIfPartial && !callback.isFullyRefused())
            throw new WriteTimeoutException(WriteType.CAS, consistencyLevel, callback.getAcceptCount(), requiredParticipants);
        logger.debug("MP_PAXOS_PROPOSE callback was not successful, returning false");
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
