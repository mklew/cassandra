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

package org.apache.cassandra.mpp.transaction.network.messages;

import java.io.IOException;
import java.util.Optional;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.mpp.transaction.network.MppResponseMessage;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 30/01/16
 */
public class PrivateMemtableReadResponse implements MppResponseMessage
{
    private final Optional<PartitionUpdate> partitionUpdateOpt;

    public static final IVersionedSerializer<PrivateMemtableReadResponse> serializer = new Serializer();

    public PrivateMemtableReadResponse(Optional<PartitionUpdate> partitionUpdateOpt)
    {
        this.partitionUpdateOpt = partitionUpdateOpt;
    }

    public Optional<PartitionUpdate> getPartitionUpdateOpt()
    {
        return partitionUpdateOpt;
    }

    private static class Serializer implements IVersionedSerializer<PrivateMemtableReadResponse>
    {

        public void serialize(PrivateMemtableReadResponse privateMemtableReadResponse, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(privateMemtableReadResponse.partitionUpdateOpt.isPresent());

            privateMemtableReadResponse.partitionUpdateOpt.ifPresent(pu -> {
                try
                {
                    PartitionUpdate.serializer.serialize(pu, out, version);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            });
        }

        public PrivateMemtableReadResponse deserialize(DataInputPlus in, int version) throws IOException
        {
            final boolean isPresent = in.readBoolean();
            if (isPresent)
            {
                DecoratedKey key = null; // because version is > 3 it can handle null
                final PartitionUpdate partitionUpdate = PartitionUpdate.serializer.deserialize(in, version, SerializationHelper.Flag.FROM_REMOTE, key);
                return new PrivateMemtableReadResponse(Optional.of(partitionUpdate));
            }
            else
            {
                return new PrivateMemtableReadResponse(Optional.empty());
            }
        }

        public long serializedSize(PrivateMemtableReadResponse privateMemtableReadResponse, int version)
        {
            return 1 + privateMemtableReadResponse.partitionUpdateOpt.map(pu -> PartitionUpdate.serializer.serializedSize(pu, version)).orElse(0L);
        }
    }
}
