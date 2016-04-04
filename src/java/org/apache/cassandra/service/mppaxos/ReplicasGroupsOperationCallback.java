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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.mpp.transaction.internal.ReplicasGroupAndOwnedItems;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 04/04/16
 */
public class ReplicasGroupsOperationCallback
{
    protected final CountDownLatch eachReplicasGroupIsDone;

    protected final int targets;

    public ReplicasGroupsOperationCallback(List<ReplicasGroupAndOwnedItems> replicasGroup) {
        Preconditions.checkArgument(!replicasGroup.isEmpty(), "There should be at least one replicas group");
        eachReplicasGroupIsDone = new CountDownLatch(replicasGroup.size());
        targets = replicasGroup.size();
    }

    protected void replicaGroupIsDone() {
        eachReplicasGroupIsDone.countDown();
    }

    public int getResponseCount()
    {
        return (int) (targets - eachReplicasGroupIsDone.getCount());
    }

    public void await() throws WriteTimeoutException
    {
        try
        {
            if (!eachReplicasGroupIsDone.await(DatabaseDescriptor.getWriteRpcTimeout(), TimeUnit.MILLISECONDS))
                throw new WriteTimeoutException(WriteType.CAS, ConsistencyLevel.LOCAL_TRANSACTIONAL, getResponseCount(), targets);
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError("This latch shouldn't have been interrupted.");
        }
    }
}
