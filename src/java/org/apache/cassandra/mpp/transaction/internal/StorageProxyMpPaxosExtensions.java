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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.metrics.CASClientRequestMetrics;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.mppaxos.MpCommit;
import org.apache.cassandra.service.mppaxos.MpPrepareCallback;
import org.apache.cassandra.service.mppaxos.MpProposeCallback;
import org.apache.cassandra.service.mppaxos.ReplicasGroupsOperationCallback;
import org.apache.cassandra.service.paxos.Commit;
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

            throw new WriteTimeoutException(WriteType.CAS, consistencyForPaxos, 0, consistencyForPaxos.blockFor(Keyspace.open(keyspaceName)));
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
            summary = preparePaxos(toPrepare, liveEndpoints, requiredParticipants, consistencyForPaxos, replicasGroupOperationCallback);
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
                if (proposePaxos(refreshedInProgress, liveEndpoints, requiredParticipants, false, consistencyForPaxos, replicasGroupOperationCallback))
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
                Tracing.trace("Repairing replicas that missed the most recent commit");
                sendCommit(mostRecent, missingMRC);
                // TODO: provided commits don't invalid the prepare we just did above (which they don't), we could just wait
                // for all the missingMRC to acknowledge this commit and then move on with proposing our value. But that means
                // adding the ability to have commitPaxos block, which is exactly CASSANDRA-5442 will do. So once we have that
                // latter ticket, we can pass CL.ALL to the commit above and remove the 'continue'.
                continue;
            }

            return Pair.create(ballot, contentions);
        }

        throw new WriteTimeoutException(WriteType.CAS, consistencyForPaxos, 0, consistencyForPaxos.blockFor(Keyspace.open(metadata.ksName)));
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

        MpPrepareCallback callback = new MpPrepareCallback(toPrepare.update, requiredParticipants, consistencyForPaxos, replicasGroupOperationCallback);
        MessageOut<MpCommit> message = new MessageOut<>(MessagingService.Verb.PAXOS_PREPARE, toPrepare, MpCommit.serializer);
        for (InetAddress target : endpoints)
            MessagingService.instance().sendRR(message, target, callback);
        callback.await();
        return callback;
    }

    /**
     * @param proposal has ballot that was prepared + transaction state
     */
    private static boolean proposePaxos(MpCommit proposal, List<InetAddress> endpoints,
                                        int requiredParticipants, boolean timeoutIfPartial,
                                        ConsistencyLevel consistencyLevel, ReplicasGroupsOperationCallback replicasGroupOperationCallback)
    throws WriteTimeoutException
    {
        MpProposeCallback callback = new MpProposeCallback(endpoints.size(), requiredParticipants, !timeoutIfPartial, consistencyLevel, replicasGroupOperationCallback);
        MessageOut<MpCommit> message = new MessageOut<>(MessagingService.Verb.MP_PAXOS_PROPOSE, proposal, MpCommit.serializer);
        for (InetAddress target : endpoints)
            MessagingService.instance().sendRR(message, target, callback);

        callback.await();

        if (callback.isSuccessful())
            return true;

        if (timeoutIfPartial && !callback.isFullyRefused())
            throw new WriteTimeoutException(WriteType.CAS, consistencyLevel, callback.getAcceptCount(), requiredParticipants);

        return false;
    }

    private static void commitPaxos(MpCommit proposal, ConsistencyLevel consistencyLevel, boolean shouldHint) throws WriteTimeoutException
    {
        boolean shouldBlock = consistencyLevel != ConsistencyLevel.ANY;
        Keyspace keyspace = Keyspace.open(proposal.update.metadata().ksName);

        Token tk = proposal.update.partitionKey().getToken();
        List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(keyspace.getName(), tk);
        Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspace.getName());

        AbstractWriteResponseHandler<Commit> responseHandler = null;
        if (shouldBlock)
        {
            AbstractReplicationStrategy rs = keyspace.getReplicationStrategy();
            responseHandler = rs.getWriteResponseHandler(naturalEndpoints, pendingEndpoints, consistencyLevel, null, WriteType.SIMPLE);
        }

        MessageOut<MpCommit> message = new MessageOut<>(MessagingService.Verb.MP_PAXOS_COMMIT, proposal, MpCommit.serializer);
        for (InetAddress destination : Iterables.concat(naturalEndpoints, pendingEndpoints))
        {
            if (FailureDetector.instance.isAlive(destination))
            {
                if (shouldBlock)
                    MessagingService.instance().sendRR(message, destination, responseHandler, shouldHint);
                else
                    MessagingService.instance().sendOneWay(message, destination);
            }
            else if (shouldHint)
            {
                // TODO [MPP] No hints support in multi partition paxos.
//                submitHint(proposal.makeMutation(), destination, null);
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
