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


import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.mpp.transaction.TxLog;
import org.apache.cassandra.net.MessageIn;

/**
 * ProposeCallback has two modes of operation, controlled by the failFast parameter.
 *
 * In failFast mode, we will return a failure as soon as a majority of nodes reject
 * the proposal. This is used when replaying a proposal from an earlier leader.
 *
 * Otherwise, we wait for either all replicas to reply or until we achieve
 * the desired quorum. We continue to wait for all replicas even after we know we cannot succeed
 * because we need to know if no node at all have accepted or if at least one has.
 * In the former case, a proposer is guaranteed no-one will
 * replay its value; in the latter we don't, so we must timeout in case another
 * leader replays it before we can; see CASSANDRA-6013
 */
public class MpProposeCallback extends AbstractMpPaxosCallback<MpProposeResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(MpProposeCallback.class);

    private final AtomicInteger accepts = new AtomicInteger(0);
    private final int requiredAccepts;
    private final boolean failFast;
    private final AtomicBoolean rolledBack = new AtomicBoolean(false);
    private TxLog txLog;

    public MpProposeCallback(int totalTargets, int requiredTargets, boolean failFast, ConsistencyLevel consistency, ReplicasGroupsOperationCallback replicasGroupOperationCallback)
    {
        super(totalTargets, consistency, replicasGroupOperationCallback);
        this.requiredAccepts = requiredTargets;
        this.failFast = failFast;
    }

    public void response(MessageIn<MpProposeResponse> msg)
    {
        logger.debug("Propose response {} from {}", msg.payload, msg.from);

        this.txLog = msg.payload.txLog;

        if (msg.payload.promised)
            accepts.incrementAndGet();

        if(msg.payload.rolledback)
            rolledBack.compareAndSet(false, msg.payload.rolledback);

        latch.countDown();

        if (isSuccessful() || (failFast && (latch.getCount() + accepts.get() < requiredAccepts)) || wasRolledBack() || txLog != TxLog.UNKNOWN)
        {
            while (latch.getCount() > 0)
                latch.countDown();
        }
    }

    public int getAcceptCount()
    {
        return accepts.get();
    }

    public boolean wasRolledBack() {
        return rolledBack.get();
    }

    public TxLog getTxLog()
    {
        return txLog;
    }

    public boolean isSuccessful()
    {
        return accepts.get() >= requiredAccepts && !rolledBack.get();
    }

    // Note: this is only reliable if !failFast
    public boolean isFullyRefused()
    {
        // We need to check the latch first to avoid racing with a late arrival
        // between the latch check and the accepts one
        return latch.getCount() == 0 && accepts.get() == 0;
    }
}
