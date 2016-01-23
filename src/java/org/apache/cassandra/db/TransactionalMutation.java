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

package org.apache.cassandra.db;

import java.util.Collection;
import java.util.UUID;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.mpp.MppServicesLocator;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;

/**
 * TODO [MPP] Serializer, deserializer, logic to apply that mutation.
 * Also TODO TransactionalVerbHandler, message and others.
 *
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 29/11/15
 */
public class TransactionalMutation implements IMutation
{
    private static final Logger logger = LoggerFactory.getLogger(TransactionalMutation.class);

    private final Mutation mutation;
    private final ConsistencyLevel consistencyLevel;
    private final UUID transactionId;

    public TransactionalMutation(Mutation mutation, ConsistencyLevel consistencyLevel, UUID transactionId)
    {
        final ConsistencyLevel clToUse = validateConsistencyLevel(consistencyLevel);
        Preconditions.checkNotNull(mutation, "Mutation should not be null");
        Preconditions.checkNotNull(transactionId, "Transaction ID should not be null");

        this.mutation = mutation;
        this.consistencyLevel = clToUse;
        this.transactionId = transactionId;
    }

    private ConsistencyLevel validateConsistencyLevel(ConsistencyLevel consistencyLevel)
    {
        if (consistencyLevel.isTransactionalConsistency())
        {
            return consistencyLevel;
        }
        else
        {
            logger.warn("ConsistencyLevel should be [LOCAL_]TRANSACTIONAL, but was {}. Defaulting to TRANSACTIONAL",
                        consistencyLevel);
            return ConsistencyLevel.TRANSACTIONAL;
        }
    }

    public String getKeyspaceName()
    {
        return mutation.getKeyspaceName();
    }

    public Collection<UUID> getColumnFamilyIds()
    {
        return mutation.getColumnFamilyIds();
    }

    public DecoratedKey key()
    {
        return mutation.key();
    }

    public long getTimeout()
    {
        return mutation.getTimeout();
    }

    public String toString(boolean shallow)
    {
        return mutation.toString(shallow);
    }

    public Collection<PartitionUpdate> getPartitionUpdates()
    {
        return mutation.getPartitionUpdates();
    }

    public TransactionItem apply()
    {
        return MppServicesLocator.getInstance().executeTransactionalMutation(this);
//        return MppServiceUtils.transformToResultMessage(txItem);
    }

    public Mutation getMutation()
    {
        return mutation;
    }

    public UUID getTransactionId()
    {
        return transactionId;
    }
}
