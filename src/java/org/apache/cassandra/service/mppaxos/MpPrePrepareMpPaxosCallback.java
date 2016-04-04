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
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.mpp.transaction.paxos.MpPaxosId;
import org.apache.cassandra.net.MessageIn;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 04/04/16
 */
public class MpPrePrepareMpPaxosCallback extends AbstractMpPaxosCallback<MpPrePrepareResponse>
{
    private static final Logger logger = LoggerFactory.getLogger(MpPrePrepareMpPaxosCallback.class);

    private final Map<InetAddress, Optional<MpPaxosId>> responsesByReplica = new ConcurrentHashMap<>();

    private final AtomicInteger roundsPrepared = new AtomicInteger(0);

    private final boolean failFast = false;

    public MpPrePrepareMpPaxosCallback(int targets, ConsistencyLevel consistency, ReplicasGroupsOperationCallback replicasGroupOperationCallback)
    {
        super(targets, consistency, replicasGroupOperationCallback);
    }

    public void response(MessageIn<MpPrePrepareResponse> msg)
    {
        InetAddress replica = msg.from;
        Optional<MpPaxosId> paxosId = msg.payload.paxosId;
        logger.debug("MpPrePrepareMpPaxos response {} from {}", paxosId, replica);

        responsesByReplica.put(replica, paxosId);

        // count prepared rounds.
        if (msg.payload.paxosId.isPresent())
            roundsPrepared.incrementAndGet();

        latch.countDown();

//        if (isSuccessful() || (failFast && (latch.getCount() + roundsPrepared.get() < requiredAccepts)))
        if (isSuccessful())
        {
            while (latch.getCount() > 0)
                latch.countDown();
        }
    }

    public Map<InetAddress, Optional<MpPaxosId>> getResponsesByReplica()
    {
        return responsesByReplica;
    }

    public boolean isSuccessful()
    {
        return roundsPrepared.get() >= requiredPaxosRounds();
    }

    public int requiredPaxosRounds() {
        return replicationFactor / 2 + 1;
    }

    // TODO [MPP] I've hardcoded it just to finish it quicker
    private final static int replicationFactor = 3;
}
