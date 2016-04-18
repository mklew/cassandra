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

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 07/04/16
 */

public class ReplicaPhasesTest
{
    @Test
    public void shouldCorrectlyFindQuorumPhase1() throws Throwable {
        ReplicasGroup rg = createReplicaGroup(Phase.PREPARE_PHASE, Phase.PREPARE_PHASE, Phase.PREPARE_PHASE);
        Assert.assertEquals(Phase.PREPARE_PHASE, StorageProxyMpPaxosExtensions.getPhaseOfQuorum(rg).get());
    }

    @Test
    public void shouldCorrectlyFindQuorumPhase2() throws Throwable {
        ReplicasGroup rg = createReplicaGroup(Phase.PRE_PREPARE_PHASE, Phase.PREPARE_PHASE, Phase.PREPARE_PHASE);
        Assert.assertEquals(Phase.PREPARE_PHASE, StorageProxyMpPaxosExtensions.getPhaseOfQuorum(rg).get());
    }

    @Test
    public void shouldCorrectlyFindQuorumPhase3() throws Throwable {
        ReplicasGroup rg = createReplicaGroup(Phase.PRE_PREPARE_PHASE, Phase.PREPARE_PHASE, Phase.PRE_PREPARE_PHASE);
        Assert.assertEquals(Phase.PRE_PREPARE_PHASE, StorageProxyMpPaxosExtensions.getPhaseOfQuorum(rg).get());
    }

    @Test
    public void shouldCorrectlyFindQuorumPhase4() throws Throwable {
        ReplicasGroup rg = createReplicaGroup(Phase.PRE_PREPARE_PHASE, Phase.PREPARE_PHASE, Phase.PRE_PREPARE_PHASE);
        Assert.assertEquals(Phase.PRE_PREPARE_PHASE, StorageProxyMpPaxosExtensions.getMaximumPhaseSharedByQuorum(rg));
    }

    @Test
    public void shouldCorrectlyFindMinimumQuorumPhase4() throws Throwable {
        ReplicasGroup rg = createReplicaGroup(Phase.PRE_PREPARE_PHASE, Phase.PREPARE_PHASE, Phase.PROPOSE_PHASE);
        Assert.assertEquals(Phase.PREPARE_PHASE, StorageProxyMpPaxosExtensions.getMaximumPhaseSharedByQuorum(rg));
    }

    @Test
    public void shouldCorrectlyFindMinimumQuorumPhase5() throws Throwable {
        ReplicasGroup rg = createReplicaGroup(Phase.COMMIT_PHASE, Phase.PREPARE_PHASE, Phase.PROPOSE_PHASE);
        Assert.assertEquals(Phase.PROPOSE_PHASE, StorageProxyMpPaxosExtensions.getMaximumPhaseSharedByQuorum(rg));
    }

    @Test
    public void shouldCorrectlyFindMinimumQuorumPhase6() throws Throwable {
        ReplicasGroup rg = createReplicaGroup(Phase.COMMIT_PHASE, Phase.PROPOSE_PHASE, Phase.PROPOSE_PHASE);
        Assert.assertEquals(Phase.PROPOSE_PHASE, StorageProxyMpPaxosExtensions.getMaximumPhaseSharedByQuorum(rg));
    }

    @Test
    public void shouldCorrectlyFindMinimumQuorumPhase7() throws Throwable {
        ReplicasGroup rg = createReplicaGroup(Phase.COMMIT_PHASE, Phase.PROPOSE_PHASE, Phase.COMMIT_PHASE);
        Assert.assertEquals(Phase.COMMIT_PHASE, StorageProxyMpPaxosExtensions.getMaximumPhaseSharedByQuorum(rg));
    }

    @Test
    public void shouldCorrectlyFindMinimumQuorumPhase10() throws Throwable {
        ReplicasGroup rg = createReplicaGroup(Phase.COMMIT_PHASE, Phase.PROPOSE_PHASE, Phase.AFTER_COMMIT_PHASE);
        Assert.assertEquals(Phase.COMMIT_PHASE, StorageProxyMpPaxosExtensions.getMaximumPhaseSharedByQuorum(rg));
    }

    @Test
    public void shouldCorrectlyFindMinimumQuorumPhase8() throws Throwable {
        ReplicasGroup rg = createReplicaGroup(Phase.PREPARE_PHASE, Phase.PROPOSE_PHASE, Phase.COMMIT_PHASE);
        Assert.assertEquals(Phase.PROPOSE_PHASE, StorageProxyMpPaxosExtensions.getMaximumPhaseSharedByQuorum(rg));
    }

    @Test
    public void shouldCorrectlyFindMinimumQuorumPhase11() throws Throwable {
        ReplicasGroup rg = createReplicaGroup(Phase.PREPARE_PHASE, Phase.PROPOSE_PHASE, Phase.BEGIN_AND_REPAIR_PHASE);
        Assert.assertEquals(Phase.PREPARE_PHASE, StorageProxyMpPaxosExtensions.getMaximumPhaseSharedByQuorum(rg));
    }

    @Test
    public void shouldCorrectlyFindMinimumQuorumPhase12() throws Throwable {
        ReplicasGroup rg = createReplicaGroupFive(Phase.PREPARE_PHASE, Phase.PROPOSE_PHASE, Phase.BEGIN_AND_REPAIR_PHASE, Phase.PROPOSE_PHASE, Phase.PREPARE_PHASE);
        Assert.assertEquals(Phase.PREPARE_PHASE, StorageProxyMpPaxosExtensions.getMaximumPhaseSharedByQuorum(rg));
    }

    @Test
    public void shouldCorrectlyFindMinimumQuorumPhase13() throws Throwable {
        ReplicasGroup rg = createReplicaGroupFive(Phase.COMMIT_PHASE, Phase.COMMIT_PHASE, Phase.BEGIN_AND_REPAIR_PHASE, Phase.PROPOSE_PHASE, Phase.PREPARE_PHASE);
        Assert.assertEquals(Phase.PROPOSE_PHASE, StorageProxyMpPaxosExtensions.getMaximumPhaseSharedByQuorum(rg));
    }

    @Test
    public void shouldCorrectlyFindMinimumQuorumPhase14() throws Throwable {
        ReplicasGroup rg = createReplicaGroupFive(Phase.BEGIN_AND_REPAIR_PHASE, Phase.PROPOSE_PHASE, Phase.PREPARE_PHASE, Phase.COMMIT_PHASE, Phase.COMMIT_PHASE);
        Assert.assertEquals(Phase.PROPOSE_PHASE, StorageProxyMpPaxosExtensions.getMaximumPhaseSharedByQuorum(rg));
    }

    @Test
    public void reproduceError() throws Throwable {
        /**
         * ReplicaGroup: 	127.0.0.1 -> PROPOSE_PHASE	127.0.0.2 -> PRE_PREPARE_PHASE	127.0.0.3 -> PRE_PREPARE_PHASE
         ReplicaGroup: 	127.0.0.2 -> PRE_PREPARE_PHASE	127.0.0.3 -> PRE_PREPARE_PHASE	127.0.0.4 -> ROLLBACK_PHASE
         ReplicaGroup: 	127.0.0.5 -> PRE_PREPARE_PHASE	127.0.0.1 -> PROPOSE_PHASE	127.0.0.2 -> PRE_PREPARE_PHASE
         */
        Replica replica1 = new Replica(InetAddress.getByName("127.0.0.1"), Phase.PROPOSE_PHASE);
        Replica replica2 = new Replica(InetAddress.getByName("127.0.0.2"), Phase.PRE_PREPARE_PHASE);
        Replica replica3 = new Replica(InetAddress.getByName("127.0.0.3"), Phase.PRE_PREPARE_PHASE);
        Replica replica4 = new Replica(InetAddress.getByName("127.0.0.4"), Phase.ROLLBACK_PHASE);
        Replica replica5 = new Replica(InetAddress.getByName("127.0.0.5"), Phase.PRE_PREPARE_PHASE);

        ReplicasGroup rg1 = new ReplicasGroup(Arrays.asList(replica1, replica2, replica3));
        ReplicasGroup rg2 = new ReplicasGroup(Arrays.asList(replica2, replica3, replica4));
        ReplicasGroup rg3 = new ReplicasGroup(Arrays.asList(replica5, replica1, replica2));

        List<ReplicasGroup> replicaGroups = Arrays.asList(rg1, rg2, rg3);
        Phase nextPhaseForReplica = StorageProxyMpPaxosExtensions.findNextPhaseForReplica(replica1, replicaGroups);
        Assert.assertEquals(Phase.PREPARE_PHASE, nextPhaseForReplica);

        StorageProxyMpPaxosExtensions.TransitionId transitionId = StorageProxyMpPaxosExtensions.transitionToPhase(replica1.getPhase(), nextPhaseForReplica);
        Assert.assertEquals(StorageProxyMpPaxosExtensions.TransitionId.TO_PREPARED, transitionId);
    }

    @Test
    public void reproduceError2() throws Throwable {
        /**
         * ReplicaGroup: 	127.0.0.1 -> PROPOSE_PHASE	127.0.0.2 -> PRE_PREPARE_PHASE	127.0.0.3 -> PRE_PREPARE_PHASE
         ReplicaGroup: 	127.0.0.2 -> PRE_PREPARE_PHASE	127.0.0.3 -> PRE_PREPARE_PHASE	127.0.0.4 -> ROLLBACK_PHASE
         ReplicaGroup: 	127.0.0.5 -> PRE_PREPARE_PHASE	127.0.0.1 -> PROPOSE_PHASE	127.0.0.2 -> PRE_PREPARE_PHASE
         */
        Replica replica1 = new Replica(InetAddress.getByName("127.0.0.1"), Phase.PROPOSE_PHASE);
        Replica replica2 = new Replica(InetAddress.getByName("127.0.0.2"), Phase.PRE_PREPARE_PHASE);

        ReplicasGroup rg1 = new ReplicasGroup(Arrays.asList(replica1, replica2));

        List<ReplicasGroup> replicaGroups = Arrays.asList(rg1);
        Phase nextPhaseForReplica = StorageProxyMpPaxosExtensions.findNextPhaseForReplica(replica1, replicaGroups);
        Assert.assertEquals(Phase.PREPARE_PHASE, nextPhaseForReplica);

        StorageProxyMpPaxosExtensions.TransitionId transitionId = StorageProxyMpPaxosExtensions.transitionToPhase(replica1.getPhase(), nextPhaseForReplica);
        Assert.assertEquals(StorageProxyMpPaxosExtensions.TransitionId.TO_PREPARED, transitionId);
    }


    private static ReplicasGroup createReplicaGroup(Phase phase1, Phase phase2, Phase phase3) throws UnknownHostException
    {
        Replica replica1 = new Replica(InetAddress.getByName("127.0.0.1"), phase1);
        Replica replica2 = new Replica(InetAddress.getByName("127.0.0.2"), phase2);
        Replica replica3 = new Replica(InetAddress.getByName("127.0.0.3"), phase3);
        List<Replica> replicas = Arrays.asList(replica1, replica2, replica3);
        ReplicasGroup rg = new ReplicasGroup(replicas);
        return rg;
    }

    private static ReplicasGroup createReplicaGroupFive(Phase phase1, Phase phase2, Phase phase3, Phase phase4, Phase phase5) throws UnknownHostException
    {
        Replica replica1 = new Replica(InetAddress.getByName("127.0.0.1"), phase1);
        Replica replica2 = new Replica(InetAddress.getByName("127.0.0.2"), phase2);
        Replica replica3 = new Replica(InetAddress.getByName("127.0.0.3"), phase3);
        Replica replica4 = new Replica(InetAddress.getByName("127.0.0.4"), phase4);
        Replica replica5 = new Replica(InetAddress.getByName("127.0.0.5"), phase5);
        List<Replica> replicas = Arrays.asList(replica1, replica2, replica3, replica4, replica5);
        ReplicasGroup rg = new ReplicasGroup(replicas);
        return rg;
    }
}
