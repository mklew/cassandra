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

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.mpp.transaction.TransactionId;
import org.apache.cassandra.mpp.transaction.TransactionTimeUUID;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.serialization.TransactionItemSerializer;
import org.apache.cassandra.serializers.InetAddressSerializer;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 05/04/16
 */
public class MppHint
{
    final private InetAddress destination;
    final private long timestamp;
    final private TransactionId id;
    final private List<TransactionItem> itemsToHint;

    public final static MppHintSerializer serializer = new MppHintSerializer();

    private static final int SIZE_OF_UUID = TypeSizes.sizeof(UUIDGen.getTimeUUID());

    public MppHint(InetAddress destination, long timestamp, TransactionId id, List<TransactionItem> itemsToHint)
    {
        this.destination = destination;
        this.timestamp = timestamp;
        this.id = id;
        this.itemsToHint = itemsToHint;
    }

    public InetAddress getDestination()
    {
        return destination;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public TransactionId getId()
    {
        return id;
    }

    public List<TransactionItem> getItemsToHint()
    {
        return itemsToHint;
    }

    public static class MppHintSerializer implements IVersionedSerializer<MppHint> {

        public void serialize(MppHint mppHint, DataOutputPlus out, int version) throws IOException
        {
            ByteBufferUtil.writeWithLength(mppHint.destination.getAddress(), out);
            out.writeLong(mppHint.timestamp);

            out.write(UUIDSerializer.instance.serialize(mppHint.getId().unwrap()));

            /* serialize size of transaction items */
            int size = mppHint.itemsToHint.size();
            out.writeInt(size);

            assert size > 0;
            for (TransactionItem transactionItem : mppHint.itemsToHint)
            {
                TransactionItemSerializer.instance.serialize(transactionItem, out, version);
            }
        }

        public MppHint deserialize(DataInputPlus in, int version) throws IOException
        {
            InetAddress destination = InetAddressSerializer.instance.deserialize(ByteBufferUtil.readWithLength(in));
            long timestamp = in.readLong();
            final UUID id = UUIDSerializer.instance.deserialize(ByteBufferUtil.read(in, SIZE_OF_UUID));

            final int size = in.readInt();

            assert size > 0;

            List<TransactionItem> transactionItems = new ArrayList<>(size);

            for (int i = 0; i < size; ++i) {
                final TransactionItem transactionItem = TransactionItemSerializer.instance.deserialize(in, version);
                transactionItems.add(transactionItem);
            }

            return new MppHint(destination, timestamp, new TransactionTimeUUID(id), transactionItems);
        }

        public long serializedSize(MppHint mppHint, int version)
        {
            int size = 0;
            size += ByteBufferUtil.serializedSizeWithLength(ByteBufferUtil.bytes(mppHint.destination));
            size += TypeSizes.sizeof(mppHint.timestamp);
            size += TypeSizes.sizeof(mppHint.id.unwrap());
            size += TypeSizes.sizeof(mppHint.getItemsToHint().size());

            for (TransactionItem transactionItem : mppHint.getItemsToHint())
            {
                size += TransactionItemSerializer.instance.serializedSize(transactionItem, version);
            }

            return size;
        }
    }
}
