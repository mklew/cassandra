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
package org.apache.cassandra.service.mppaxos;

import java.util.Optional;
import java.util.concurrent.locks.Lock;

import com.google.common.util.concurrent.Striped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.mpp.MppServicesLocator;
import org.apache.cassandra.mpp.transaction.MultiPartitionPaxosIndex;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.internal.SystemKeyspaceMultiPartitionPaxosExtensions;
import org.apache.cassandra.mpp.transaction.paxos.MpPaxosId;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.UUIDGen;

public class MpPaxosState
{
    private static final Striped<Lock> LOCKS = Striped.lazyWeakLock(DatabaseDescriptor.getConcurrentWriters() * 1024);

    private static final Logger logger = LoggerFactory.getLogger(MpPaxosState.class);

    private final MpCommit promised;
    private final MpCommit accepted;
    private final MpCommit mostRecentCommit;

    public MpPaxosState() {
        this(MpCommit.emptyCommit(), MpCommit.emptyCommit(), MpCommit.emptyCommit());
    }

    public MpPaxosState(MpCommit promised, MpCommit accepted, MpCommit mostRecentCommit)
    {
//        assert promised.update.partitionKey().equals(accepted.update.partitionKey()) && accepted.update.partitionKey().equals(mostRecentCommit.update.partitionKey());
//        assert promised.update.metadata() == accepted.update.metadata() && accepted.update.metadata() == mostRecentCommit.update.metadata();

        this.promised = promised;
        this.accepted = accepted;
        this.mostRecentCommit = mostRecentCommit;
    }

    public static MpPrepareResponse prepare(MpCommit toPrepare)
    {
//        long start = System.nanoTime();

        Optional<MpPaxosId> paxosIdOpt = findPaxosId(toPrepare.update);

        if(paxosIdOpt.isPresent()) {
            try
            {
                MpPaxosId paxosId = paxosIdOpt.get();
                Lock lock = LOCKS.get(paxosId);
                lock.lock();
                try
                {
                    MpPaxosState state = SystemKeyspaceMultiPartitionPaxosExtensions.loadPaxosState(paxosId);
                    if (toPrepare.isAfter(state.promised))
                    {
                        Tracing.trace("Promising ballot {}", toPrepare.ballot);
                        SystemKeyspaceMultiPartitionPaxosExtensions.savePaxosPromise(toPrepare, paxosId);
                        boolean promised = true;
                        boolean notRolledBack = false;
                        return new MpPrepareResponse(promised, notRolledBack, state.accepted, state.mostRecentCommit);
                    }
                    else
                    {
                        Tracing.trace("Promise rejected; {} is not sufficiently newer than {}", toPrepare, state.promised);
                        // return the currently promised ballot (not the last accepted one) so the coordinator can make sure it uses newer ballot next time (#5667)
                        boolean notPromised = false;
                        boolean notRolledBack = false;
                        return new MpPrepareResponse(notPromised, notRolledBack, state.promised, state.mostRecentCommit);
                    }
                }
                finally
                {
                    lock.unlock();
                }
            }
            finally
            {
//                Keyspace.open(toPrepare.update.metadata().ksName).getColumnFamilyStore(toPrepare.update.metadata().cfId).metric.casPrepare.addNano(System.nanoTime() - start);
            }
        }
        else {
            boolean notPromised = false;
            boolean wasRolledBack = true;
            // returning nulls, because paxos instance cannot be found at this node..
            // It is possible to send with prepare, paxos id that was received in pre prepare response, but probably it won't be necessary.
            return new MpPrepareResponse(notPromised, wasRolledBack, MpCommit.emptyCommit(), MpCommit.emptyCommit());
        }

    }

    private static Optional<MpPaxosId> findPaxosId(TransactionState transactionState)
    {
        MultiPartitionPaxosIndex index = MppServicesLocator.getIndexInstance();
        return index.acquireAndFindPaxosId(transactionState);
    }

    public static Boolean propose(MpCommit proposal)
    {
//        long start = System.nanoTime();
        try
        {
            Optional<MpPaxosId> paxosIdOpt = findPaxosId(proposal.update);
            if(paxosIdOpt.isPresent()) {
                MpPaxosId paxosId = paxosIdOpt.get();
                Lock lock = LOCKS.get(paxosId);
                lock.lock();
                try
                {
                    MpPaxosState state = SystemKeyspaceMultiPartitionPaxosExtensions.loadPaxosState(paxosId);
                    if (proposal.hasBallot(state.promised.ballot) || proposal.isAfter(state.promised))
                    {
//                        logger.info("About to accept proposal, but making data consistent first. Proposal is {}", proposal);
                        Tracing.trace("Accepting proposal {}", proposal);
                        // TODO Maybe it should be done outside of lock, just before commit? But then transaction that cannot be made consistent, should not be proposed.
                        // MppServicesLocator.getInstance().makeTransactionDataConsistent(proposal.update);
                        logger.info("Accepting proposal {}", proposal);
                        SystemKeyspaceMultiPartitionPaxosExtensions.savePaxosProposal(proposal, paxosId);
                        return true;
                    }
                    else
                    {
                        Tracing.trace("Rejecting proposal for {} because inProgress is now {}", proposal, state.promised);
                        return false;
                    }
                }
                finally
                {
                    lock.unlock();
                }
            }
            else {
                // paxos id wasn't present. This should not happen in normal conditions, because proposer has highest ballot so
                // none of conflicting transactions could commit in the meantime.
                logger.warn("Uncommon conditions. PaxosId cannot be found in propose. Proposed transaction state is {}", proposal.update);
                return false;
            }

        }
        finally
        {
//            Keyspace.open(proposal.update.metadata().ksName).getColumnFamilyStore(proposal.update.metadata().cfId).metric.casPropose.addNano(System.nanoTime() - start);
        }
    }

    public static void commit(MpCommit proposal)
    {
        long start = System.nanoTime();
        try
        {
            Optional<MpPaxosId> paxosIdOpt = findPaxosId(proposal.update);
            if(paxosIdOpt.isPresent()) {
                MpPaxosId paxosId = paxosIdOpt.get();

                // There is no guarantee we will see commits in the right order, because messages
                // can get delayed, so a proposal can be older than our current most recent ballot/commit.
                // Committing it is however always safe due to column timestamps, so always do it. However,
                // if our current in-progress ballot is strictly greater than the proposal one, we shouldn't
                // erase the in-progress update.
                // The table may have been truncated since the proposal was initiated. In that case, we
                // don't want to perform the mutation and potentially resurrect truncated data

                long timestamp = UUIDGen.microsTimestamp(proposal.ballot);
                logger.info("Commiting multi partition paxos proposal. Tx ID is: {}", proposal.update.id());
                MppServicesLocator.getInstance().multiPartitionPaxosCommitPhase(proposal.update, timestamp);
                // We don't need to lock, we're just blindly updating
                logger.info("savePaxosCommit. Tx ID is: {}", proposal.update.id());
                SystemKeyspaceMultiPartitionPaxosExtensions.savePaxosCommit(proposal, paxosId);
            }
            else {
                // Case when this replica has missing Most Recent Commit and it was replied.
                logger.warn("Uncommon conditions. PaxosId cannot be found when doing paxos commit. Transaction ID is: {}", proposal.update.id());
            }

        }
        finally
        {
//            Keyspace.open(proposal.update.metadata().ksName).getColumnFamilyStore(proposal.update.metadata().cfId).metric.casCommit.addNano(System.nanoTime() - start);
        }
    }
}
