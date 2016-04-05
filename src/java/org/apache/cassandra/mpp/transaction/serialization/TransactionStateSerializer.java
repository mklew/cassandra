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

package org.apache.cassandra.mpp.transaction.serialization;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.client.TransactionStateUtils;
import org.apache.cassandra.serializers.UUIDSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 26/11/15
 */
public class TransactionStateSerializer implements IVersionedSerializer<TransactionState>
{
    public static final TransactionStateSerializer instance = new TransactionStateSerializer();

    private static final int SIZE_OF_UUID = TypeSizes.sizeof(UUIDGen.getTimeUUID());

    public void serialize(TransactionState transactionState, DataOutputPlus out, int version) throws IOException
    {
        out.write(UUIDSerializer.instance.serialize(transactionState.getTransactionId()));

        /* serialize size of transaction items */
        int size = transactionState.getTransactionItems().size();
        out.writeInt(size);

//        assert size > 0;
        for (TransactionItem transactionItem : transactionState.getTransactionItems())
        {
            TransactionItemSerializer.instance.serialize(transactionItem, out, version);
        }
    }

    public TransactionState deserialize(DataInputPlus in, int version) throws IOException
    {

        final UUID id = UUIDSerializer.instance.deserialize(ByteBufferUtil.read(in, SIZE_OF_UUID));
        final int size = in.readInt();

//        assert size > 0;
        if(size == 0 ) {
            return TransactionStateUtils.recreateTransactionState(id, Collections.emptyList());
        }
        else {
            Collection<TransactionItem> transactionItems = new ArrayList<>(size);

            for (int i = 0; i < size; ++i) {
                final TransactionItem transactionItem = TransactionItemSerializer.instance.deserialize(in, version);
                transactionItems.add(transactionItem);
            }
            return TransactionStateUtils.recreateTransactionState(id, transactionItems);
        }
    }

    public static TransactionState fromBytes(ByteBuffer bytes, int version)
    {
        if (bytes == null)
            return null;

        try
        {
            return TransactionStateSerializer.instance.deserialize(new DataInputBuffer(bytes, true), version);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static ByteBuffer toBytes(TransactionState transactionState, int version)
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            TransactionStateSerializer.instance.serialize(transactionState, out, version);
            return ByteBuffer.wrap(out.getData(), 0, out.getLength());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public long serializedSize(TransactionState transactionState, int version)
    {
        int size = 0;

        size += TypeSizes.sizeof(transactionState.getTransactionId());
        size += TypeSizes.sizeof(transactionState.getTransactionItems().size());
        for (TransactionItem transactionItem : transactionState.getTransactionItems())
        {
            size += TransactionItemSerializer.instance.serializedSize(transactionItem, version);
        }

        return size;
    }
}
