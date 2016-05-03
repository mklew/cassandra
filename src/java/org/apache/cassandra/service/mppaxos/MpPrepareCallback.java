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


import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.mpp.transaction.TxLog;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.net.MessageIn;

public class MpPrepareCallback extends AbstractMpPaxosCallback<MpPrepareResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(MpPrepareCallback.class);

    public boolean promised = true;
    public MpCommit mostRecentCommit;
    public MpCommit mostRecentInProgressCommit;
    public MpCommit mostRecentInProgressCommitWithUpdate;
    public TxLog txLog;

    private final TransactionState transactionState;

    private final Map<InetAddress, MpCommit> commitsByReplica = new ConcurrentHashMap<>();

    private final AtomicBoolean rolledBack = new AtomicBoolean(false);

    public MpPrepareCallback(TransactionState transactionState, int targets, ConsistencyLevel consistency, ReplicasGroupsOperationCallback replicasGroupOperationCallback)
    {
        super(targets, consistency, replicasGroupOperationCallback);
        // need to inject the right key in the empty commit so comparing with empty commits in the reply works as expected
        mostRecentCommit = MpCommit.emptyCommit(transactionState);
        mostRecentInProgressCommit = MpCommit.emptyCommit(transactionState);
        mostRecentInProgressCommitWithUpdate = MpCommit.emptyCommit(transactionState);
        this.transactionState = transactionState;
    }

    public synchronized void response(MessageIn<MpPrepareResponse> message)
    {
        MpPrepareResponse response = message.payload;
        logger.debug("Prepare response {} from {}", response, message.from);
        this.txLog = message.payload.txLog;

        if(txLog != TxLog.UNKNOWN) {
            logger.debug("Prepare response says that transaction {} exists in transaction log at replica {} tx log is {}", transactionState.getTransactionId(), message.from, txLog);
            while (latch.getCount() > 0)
                latch.countDown();
            return;
        }

        if(message.payload.rolledBack) {
            logger.debug("Prepare response says that transaction {} was rolled back at replica {} We will rollback that transaction completely", transactionState.getTransactionId(), message.from);
            rolledBack.compareAndSet(false, true);
            while (latch.getCount() > 0)
                latch.countDown();
            return;
        }



        // In case of clock skew, another node could be proposing with ballot that are quite a bit
        // older than our own. In that case, we record the more recent commit we've received to make
        // sure we re-prepare on an older ballot.
        if (response.inProgressCommit.isAfter(mostRecentInProgressCommit))
            mostRecentInProgressCommit = response.inProgressCommit;

        if (!response.promised)
        {
            promised = false;
            while (latch.getCount() > 0)
                latch.countDown();
            return;
        }

        commitsByReplica.put(message.from, response.mostRecentCommit);
        if (response.mostRecentCommit.isAfter(mostRecentCommit))
            mostRecentCommit = response.mostRecentCommit;

        // If some response has an update, then we should replay the update with the highest ballot. So find
        // the the highest commit that actually have an update
        if (response.inProgressCommit.isAfter(mostRecentInProgressCommitWithUpdate) && !response.inProgressCommit.update.isEmpty())
            mostRecentInProgressCommitWithUpdate = response.inProgressCommit;

        latch.countDown();
    }

    public boolean wasRolledBack() {
        return rolledBack.get();
    }

    public Iterable<InetAddress> replicasMissingMostRecentCommit()
    {
        return Iterables.filter(commitsByReplica.keySet(), new Predicate<InetAddress>()
        {
            public boolean apply(InetAddress inetAddress)
            {
                return (!commitsByReplica.get(inetAddress).ballot.equals(mostRecentCommit.ballot));
            }
        });
    }
}
