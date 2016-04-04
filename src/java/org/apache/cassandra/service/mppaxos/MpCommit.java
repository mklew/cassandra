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


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import com.google.common.base.Objects;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.UUIDSerializer;

public class MpCommit
{
    public static final MpCommitSerializer serializer = new MpCommitSerializer();

    public final UUID ballot;
    public  PartitionUpdate update;

    public MpCommit(UUID ballot, TransactionState transactionState)
    {
        assert ballot != null;
//        assert update != null;

        this.ballot = ballot;
//        this.update = update;
    }

    public static MpCommit newPrepare(DecoratedKey key, CFMetaData metadata, UUID ballot)
    {
        return null; // TODO [MPP] Implement it
//        return new MpCommit(ballot, PartitionUpdate.emptyUpdate(metadata, key));
    }

    public static MpCommit newProposal(UUID ballot, PartitionUpdate update)
    {
        update.updateAllTimestamp(UUIDGen.microsTimestamp(ballot));
        return null; // TODO [MPP] Implement it
//        return new MpCommit(ballot, update);
    }

    public static MpCommit emptyCommit(DecoratedKey key, CFMetaData metadata)
    {
        return null; // TODO [MPP] Implement it
//        return new MpCommit(UUIDGen.minTimeUUID(0), PartitionUpdate.emptyUpdate(metadata, key));
    }

    public static MpCommit emptyCommit(UUID paxosId)
    {
        // TODO [MPP] Implement empty version of MpCommit
        return null; // TODO [MPP] Implement it
//        return new MpCommit(UUIDGen.minTimeUUID(0), PartitionUpdate.emptyUpdate(metadata, key));
    }

    public boolean isAfter(MpCommit other)
    {
        return ballot.timestamp() > other.ballot.timestamp();
    }

    public boolean hasBallot(UUID ballot)
    {
        return this.ballot.equals(ballot);
    }

    public Mutation makeMutation()
    {
        return new Mutation(update);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MpCommit commit = (MpCommit) o;

        return ballot.equals(commit.ballot) && update.equals(commit.update);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(ballot, update);
    }

    @Override
    public String toString()
    {
        return String.format("Commit(%s, %s)", ballot, update);
    }

    public static class MpCommitSerializer implements IVersionedSerializer<MpCommit>
    {
        public void serialize(MpCommit commit, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
                ByteBufferUtil.writeWithShortLength(commit.update.partitionKey().getKey(), out);

            UUIDSerializer.serializer.serialize(commit.ballot, out, version);
            PartitionUpdate.serializer.serialize(commit.update, out, version);
        }

        public MpCommit deserialize(DataInputPlus in, int version) throws IOException
        {
            ByteBuffer key = null;
            if (version < MessagingService.VERSION_30)
                key = ByteBufferUtil.readWithShortLength(in);

            UUID ballot = UUIDSerializer.serializer.deserialize(in, version);
            PartitionUpdate update = PartitionUpdate.serializer.deserialize(in, version, SerializationHelper.Flag.LOCAL, key);
//            return new MpCommit(ballot, update);
            return null;
        }

        public long serializedSize(MpCommit commit, int version)
        {
            int size = 0;
            if (version < MessagingService.VERSION_30)
                size += ByteBufferUtil.serializedSizeWithShortLength(commit.update.partitionKey().getKey());

            return size
                 + UUIDSerializer.serializer.serializedSize(commit.ballot, version)
                 + PartitionUpdate.serializer.serializedSize(commit.update, version);
        }
    }
}
