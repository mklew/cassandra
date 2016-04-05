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

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.mpp.transaction.client.TransactionStateUtils;
import org.apache.cassandra.mpp.transaction.paxos.MpPaxosId;
import org.apache.cassandra.mpp.transaction.serialization.TransactionStateSerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.mppaxos.MpCommit;
import org.apache.cassandra.service.mppaxos.MpPaxosState;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.db.SystemKeyspace.MULTI_PARTITION_PAXOS;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 04/04/16
 */
public class SystemKeyspaceMultiPartitionPaxosExtensions
{
    public static MpPaxosState loadPaxosState(MpPaxosId paxosId)
    {
        String req = "SELECT * FROM system.%s WHERE paxos_id = ?";
        UntypedResultSet results = executeInternal(String.format(req, MULTI_PARTITION_PAXOS), paxosId.getPaxosId());
        if (results.isEmpty())
            return new MpPaxosState();
        UntypedResultSet.Row row = results.one();
        MpCommit promised = row.has("in_progress_ballot")
                          ? new MpCommit(row.getUUID("in_progress_ballot"), TransactionStateUtils.newTransactionState()) // TODO [MPP] Not sure about creating this new fake transaction state
                          : MpCommit.emptyCommit();
        // either we have both a recently accepted ballot and update or we have neither
        int proposalVersion = row.has("proposal_version") ? row.getInt("proposal_version") : MessagingService.VERSION_21;
        MpCommit accepted = row.has("proposal")
                          ? new MpCommit(row.getUUID("proposal_ballot"), TransactionStateSerializer.fromBytes(row.getBytes("proposal"), proposalVersion))
                          : MpCommit.emptyCommit();
        // either most_recent_commit and most_recent_commit_at will both be set, or neither
        int mostRecentVersion = row.has("most_recent_commit_version") ? row.getInt("most_recent_commit_version") : MessagingService.VERSION_21;
        MpCommit mostRecent = row.has("most_recent_commit")
                            ? new MpCommit(row.getUUID("most_recent_commit_at"), TransactionStateSerializer.fromBytes(row.getBytes("most_recent_commit"), mostRecentVersion))
                            : MpCommit.emptyCommit();
        return new MpPaxosState(promised, accepted, mostRecent);
    }

    public static void savePaxosPromise(MpCommit promise, MpPaxosId paxosId)
    {
        String req = "UPDATE system.%s USING TIMESTAMP ? AND TTL ? SET in_progress_ballot = ? WHERE paxos_id = ?";
        executeInternal(String.format(req, MULTI_PARTITION_PAXOS),
                        UUIDGen.microsTimestamp(promise.ballot),
                        paxosTtl(),
                        promise.ballot,
                        paxosId.getPaxosId());
    }

    public static void savePaxosProposal(MpCommit proposal, MpPaxosId paxosId)
    {
        executeInternal(String.format("UPDATE system.%s USING TIMESTAMP ? AND TTL ? SET proposal_ballot = ?, proposal = ?, proposal_version = ? WHERE paxos_id = ? ", MULTI_PARTITION_PAXOS),
                        UUIDGen.microsTimestamp(proposal.ballot),
                        paxosTtl(),
                        proposal.ballot,
                        TransactionStateSerializer.toBytes(proposal.update, MessagingService.current_version),
                        MessagingService.current_version,
                        paxosId.getPaxosId());
    }

    private static int paxosTtl()
    {
        return 3 * 3600;
        // keep paxos state around for at least 3h
//        return Math.max(3 * 3600, metadata.params.gcGraceSeconds);
    }

    public static void savePaxosCommit(MpCommit commit, MpPaxosId paxosId)
    {
        // We always erase the last proposal (with the commit timestamp to no erase more recent proposal in case the commit is old)
        // even though that's really just an optimization  since SP.beginAndRepairPaxos will exclude accepted proposal older than the mrc.
        String cql = "UPDATE system.%s USING TIMESTAMP ? AND TTL ? " +
                     "SET proposal_ballot = null, proposal = null, most_recent_commit_at = ?, most_recent_commit = ?, " +
                     "most_recent_commit_version = ? WHERE paxos_id = ?";
        executeInternal(String.format(cql, MULTI_PARTITION_PAXOS),
                        UUIDGen.microsTimestamp(commit.ballot),
                        paxosTtl(),
                        commit.ballot,
                        TransactionStateSerializer.toBytes(commit.update, MessagingService.current_version),
                        MessagingService.current_version,
                        paxosId);
    }
}
