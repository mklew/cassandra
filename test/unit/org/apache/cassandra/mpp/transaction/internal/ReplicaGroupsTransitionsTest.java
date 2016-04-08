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
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import junit.framework.Assert;

import static org.apache.cassandra.mpp.transaction.internal.StorageProxyMpPaxosExtensions.*;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 07/04/16
 */
public class ReplicaGroupsTransitionsTest
{

    Replica replica1;
    Replica replica2;
    Replica replica3;
    Replica replica4;
    Replica replica5;


    @Test
    public void shouldFindCorrectTransitionForReplica() throws Throwable
    {
        // Replica group 1: replica1, replica2, replica3 - quorum PREPARED
        // Replica group 2: replica2, replica3, replica4 - quorum PREPARED
        // Replica group 3: replica1, replica4, replica5 - quorum PROPOSED

        setPhasesOnReplicas(Phase.PRE_PREPARE_PHASE, Phase.PREPARE_PHASE, Phase.PREPARE_PHASE, Phase.PROPOSE_PHASE, Phase.PROPOSE_PHASE);

        ReplicasGroup rg1 = new ReplicasGroup(Arrays.asList(replica1, replica2, replica3));
        ReplicasGroup rg2 = new ReplicasGroup(Arrays.asList(replica2, replica3, replica4));
        ReplicasGroup rg3 = new ReplicasGroup(Arrays.asList(replica1, replica4, replica5));

        Assert.assertEquals(Phase.PREPARE_PHASE, getMaximumPhaseSharedByQuorum(rg1));
        Assert.assertEquals(Phase.PREPARE_PHASE, getMaximumPhaseSharedByQuorum(rg2));
        Assert.assertEquals(Phase.PROPOSE_PHASE, getMaximumPhaseSharedByQuorum(rg3));

        List<ReplicasGroup> replicasGroups = Arrays.asList(rg1, rg2, rg3);
        Phase minimumPhaseAmongAllReplicaGroups = findMinimumPhaseAmongAllReplicaGroups(replicasGroups);
        Assert.assertEquals(Phase.PREPARE_PHASE, minimumPhaseAmongAllReplicaGroups);

        Phase nextExpectedPhase = getNextExpectedPhase(minimumPhaseAmongAllReplicaGroups);
        Assert.assertEquals(Phase.PROPOSE_PHASE, nextExpectedPhase);

        Phase nextPhaseForReplica1 = findNextPhaseForReplica(replica1, minimumPhaseAmongAllReplicaGroups, nextExpectedPhase);
        Assert.assertEquals(Phase.PREPARE_PHASE, nextPhaseForReplica1);
        Assert.assertEquals(Phase.PREPARE_PHASE, findNextPhaseForReplica(replica1, replicasGroups));
        Assert.assertEquals(TransitionId.TO_PREPARED, findNextTransitionId(replica1, nextPhaseForReplica1));

        Phase nextPhaseForReplica2 = findNextPhaseForReplica(replica2, minimumPhaseAmongAllReplicaGroups, nextExpectedPhase);
        Assert.assertEquals(Phase.PROPOSE_PHASE, nextPhaseForReplica2);
        Assert.assertEquals(Phase.PROPOSE_PHASE, findNextPhaseForReplica(replica2, replicasGroups));
        Assert.assertEquals(TransitionId.TO_PROPOSED, findNextTransitionId(replica2, nextPhaseForReplica2));

        Phase nextPhaseForReplica3 = findNextPhaseForReplica(replica3, minimumPhaseAmongAllReplicaGroups, nextExpectedPhase);
        Assert.assertEquals(Phase.PROPOSE_PHASE, nextPhaseForReplica3);
        Assert.assertEquals(Phase.PROPOSE_PHASE, findNextPhaseForReplica(replica3, replicasGroups));
        Assert.assertEquals(TransitionId.TO_PROPOSED, findNextTransitionId(replica3, nextPhaseForReplica3));

        Phase nextPhaseForReplica4 = findNextPhaseForReplica(replica4, minimumPhaseAmongAllReplicaGroups, nextExpectedPhase);
        Assert.assertEquals(Phase.PROPOSE_PHASE, nextPhaseForReplica4);
        Assert.assertEquals(Phase.PROPOSE_PHASE, findNextPhaseForReplica(replica4, replicasGroups));
        Assert.assertEquals(TransitionId.NOOP, findNextTransitionId(replica4, nextPhaseForReplica4));

        Phase nextPhaseForReplica5 = findNextPhaseForReplica(replica5, minimumPhaseAmongAllReplicaGroups, nextExpectedPhase);
        Assert.assertEquals(Phase.PROPOSE_PHASE, nextPhaseForReplica5);
        Assert.assertEquals(Phase.PROPOSE_PHASE, findNextPhaseForReplica(replica5, replicasGroups));
        Assert.assertEquals(TransitionId.NOOP, findNextTransitionId(replica5, nextPhaseForReplica5));

    }

    private ReplicasGroup setPhasesOnReplicas(Phase phase1, Phase phase2, Phase phase3, Phase phase4, Phase phase5) throws UnknownHostException
    {
        replica1 = new Replica(InetAddress.getByName("127.0.0.1"), phase1);
        replica2 = new Replica(InetAddress.getByName("127.0.0.2"), phase2);
        replica3 = new Replica(InetAddress.getByName("127.0.0.3"), phase3);
        replica4 = new Replica(InetAddress.getByName("127.0.0.4"), phase4);
        replica5 = new Replica(InetAddress.getByName("127.0.0.5"), phase5);
        List<Replica> replicas = Arrays.asList(replica1, replica2, replica3, replica4, replica5);
        ReplicasGroup rg = new ReplicasGroup(replicas);
        return rg;
    }
}
