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

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 07/04/16
 */
public class Replica implements WithPhase
{
    private final InetAddress host;

    private Phase phase;

    private StorageProxyMpPaxosExtensions.ReplicaInPhaseHolder holder;

    public Replica(InetAddress host)
    {
        this.host = host;
        this.phase = Phase.NO_PHASE;
    }

    public Replica(InetAddress host, Phase phase)
    {
        this.host = host;
        this.phase = phase;
    }

    public String getHostAddress() {
        return host.getHostAddress();
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Replica replica = (Replica) o;

        if (!host.getHostAddress().equals(replica.host.getHostAddress())) return false;

        return true;
    }

    public int hashCode()
    {
        return host.getHostAddress().hashCode();
    }

    public InetAddress getHost()
    {
        return host;
    }

    public Phase getPhase()
    {
        return phase;
    }

    public void setPhase(Phase phase)
    {
        checkPhaseRequirements(phase);
        this.phase = phase;
    }

    private void checkPhaseRequirements(Phase phase)
    {
//        if(phase.ordinal() > Phase.PRE_PREPARE_PHASE.ordinal()) {
//            Preconditions.checkState(ballot != null, "In order to change Phase from " + this.phase +
//                                                     " to phase " + phase +
//                                                     " ballot is required, but was null. Replica is "
//                                                     + getHostAddress());
//        }
    }

    public StorageProxyMpPaxosExtensions.ReplicaInPhaseHolder getReplicaInPhaseHolder()
    {
        return holder;
    }

    public void setHolder(StorageProxyMpPaxosExtensions.ReplicaInPhaseHolder holder)
    {
        this.holder = holder;
    }

    public String toString()
    {
        return "Replica{" +
               "host=" + host +
               '}';
    }
}
