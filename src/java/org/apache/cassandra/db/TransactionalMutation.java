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

import java.io.IOException;
import java.util.Collection;
import java.util.UUID;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.mpp.MppServicesLocator;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 29/11/15
 */
public class TransactionalMutation implements IMutation
{
    private static final Logger logger = LoggerFactory.getLogger(TransactionalMutation.class);

    public static final TransactionalMutationSerializer serializer = new TransactionalMutationSerializer();

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
        return MppServicesLocator.getInstance().executeTransactionalMutationLocally(this);
    }

    public TransactionItem toTransactionItem()
    {
        return MppServicesLocator.getInstance().getTransactionItemForMutationNoExecution(this);
    }

    public Mutation getMutation()
    {
        return mutation;
    }

    public UUID getTransactionId()
    {
        return transactionId;
    }

    public MessageOut<TransactionalMutation> createMessage()
    {
        return createMessage(MessagingService.Verb.PRIVATE_MEMTABLE_WRITE);
    }

    public MessageOut<TransactionalMutation> createMessage(MessagingService.Verb verb)
    {
        return new MessageOut<>(verb, this, serializer);
    }

    public static class TransactionalMutationSerializer implements IVersionedSerializer<TransactionalMutation> {

        private static final int SIZE_OF_UUID = TypeSizes.sizeof(UUIDGen.getTimeUUID());

        public void serialize(TransactionalMutation transactionalMutation, DataOutputPlus out, int version) throws IOException
        {
            out.write(UUIDSerializer.instance.serialize(transactionalMutation.transactionId));
            out.writeUTF(transactionalMutation.consistencyLevel.name());
            Mutation.serializer.serialize(transactionalMutation.mutation, out, version);
        }

        public TransactionalMutation deserialize(DataInputPlus in, int version) throws IOException
        {
            final UUID id = UUIDSerializer.instance.deserialize(ByteBufferUtil.read(in, SIZE_OF_UUID));
            ConsistencyLevel consistency = Enum.valueOf(ConsistencyLevel.class, in.readUTF());
            final Mutation m = Mutation.serializer.deserialize(in, version);
            return new TransactionalMutation(m, consistency, id);
        }

        public long serializedSize(TransactionalMutation transactionalMutation, int version)
        {
            return 16 + Mutation.serializer.serializedSize(transactionalMutation.mutation, version)
            + TypeSizes.sizeof(transactionalMutation.consistencyLevel.name());
        }
    }
}
