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
import java.util.UUID;

import com.google.common.base.Objects;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.client.TransactionStateUtils;
import org.apache.cassandra.mpp.transaction.serialization.TransactionStateSerializer;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.UUIDSerializer;

public class MpCommit
{
    public static final MpCommitSerializer serializer = new MpCommitSerializer();

    public final UUID ballot;
    public TransactionState update; // TODO this is the update

    public MpCommit(UUID ballot, TransactionState transactionState)
    {
        assert ballot != null;
        assert update != null;
//        assert update != null;

        this.ballot = ballot;
        this.update = transactionState;
//        this.update = update;
    }

    public static MpCommit newPrepare(TransactionState transactionState, UUID ballot)
    {
        return new MpCommit(ballot, transactionState);
    }

    public static MpCommit newProposal(UUID ballot, TransactionState update)
    {
        // This update doesn't have to happen because timestamp will get refreshed during Commit
//        update.updateAllTimestamp(UUIDGen.microsTimestamp(ballot));
        return new MpCommit(ballot, update);
    }

    public static MpCommit emptyCommit()
    {
        return new MpCommit(UUIDGen.minTimeUUID(0), TransactionStateUtils.newTransactionState());
    }

    public static MpCommit emptyCommit(TransactionState transactionState)
    {
        return new MpCommit(UUIDGen.minTimeUUID(0), transactionState);
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
        // TODO this should not be used because mutations are taken from private memtables
//        return new Mutation(update);
        return null;
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
        return String.format("MpCommit(%s, %s)", ballot, update);
    }

    public static class MpCommitSerializer implements IVersionedSerializer<MpCommit>
    {
        public void serialize(MpCommit commit, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(commit.ballot, out, version);
            TransactionStateSerializer.instance.serialize(commit.update, out, version);
        }

        public MpCommit deserialize(DataInputPlus in, int version) throws IOException
        {
            UUID ballot = UUIDSerializer.serializer.deserialize(in, version);
            TransactionState transactionState = TransactionStateSerializer.instance.deserialize(in, version);
            return new MpCommit(ballot, transactionState);
        }

        public long serializedSize(MpCommit commit, int version)
        {
            int size = 0;

            return size
                 + UUIDSerializer.serializer.serializedSize(commit.ballot, version)
                 + TransactionStateSerializer.instance.serializedSize(commit.update, version);
        }
    }
}
