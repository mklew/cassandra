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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.mpp.transaction.client.TransactionItem;

/**
* @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
* @since 04/04/16
*/
public class ReplicasGroupAndOwnedItems
{
    private final ReplicasGroup replicasGroup;

    private final List<TransactionItem> ownedItems;

    private final Set<Replica> allReplicas;

    ReplicasGroupAndOwnedItems(ReplicasGroup replicasGroup, List<TransactionItem> ownedItems, Collection<Replica> allReplicas)
    {
        this.replicasGroup = replicasGroup;
        this.ownedItems = ownedItems;
        this.allReplicas = new HashSet<>(allReplicas);
    }

    public ReplicasGroup getReplicasGroup()
    {
        return replicasGroup;
    }

    public List<TransactionItem> getOwnedItems()
    {
        return ownedItems;
    }

    public Collection<Replica> getAllReplicas()
    {
        return allReplicas;
    }

    public String toString()
    {
        return "ReplicasGroupAndOwnedItems{" +
               "replicasGroup=" + replicasGroup +
               ", ownedItems=" + ownedItems +
               '}';
    }
}
