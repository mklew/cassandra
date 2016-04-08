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
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
* @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
* @since 04/04/16
*/
public class ReplicasGroup
{
    private final List<Replica> replicas;

    ReplicasGroup(List<Replica> replicas)
    {
        this.replicas = replicas;
    }

    public List<InetAddress> getReplicas()
    {
        return replicas.stream().map(Replica::getHost).collect(toList());
    }

    public String toString()
    {
        return "ReplicaGroup[" + replicas + ']';
    }

    public List<Replica> getAllReplicasInThatGroup() {
        return replicas;
    }

    public boolean hasSameReplicasAs(ReplicasGroup replicasGroup)
    {
        List<String> thisHosts = replicas.stream().map(Replica::getHostAddress).sorted().collect(toList());
        List<String> otherHosts = replicasGroup.replicas.stream().map(Replica::getHostAddress).sorted().collect(toList());
        return thisHosts.equals(otherHosts);
    }

    public boolean hasReplica(Replica replica)
    {
        return replicas.contains(replica);
    }
}
