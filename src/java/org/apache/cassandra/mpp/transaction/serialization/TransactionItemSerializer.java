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

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Deserializes {@link org.apache.cassandra.mpp.transaction.client.TransactionItem}
 *
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 26/11/15
 */
public class TransactionItemSerializer implements IVersionedSerializer<TransactionItem>
{
    public static final TransactionItemSerializer instance = new TransactionItemSerializer();

    public void serialize(TransactionItem transactionItem, DataOutputPlus out, int version) throws IOException
    {
        Token.serializer.serialize(transactionItem.getToken(), out, version);
        ByteBufferUtil.writeWithLength(ByteBufferUtil.bytes(transactionItem.getKsName()), out);
        ByteBufferUtil.writeWithLength(ByteBufferUtil.bytes(transactionItem.getCfName()), out);
    }

    public TransactionItem deserialize(DataInputPlus in, int version) throws IOException
    {
        // TODO [MPP] This makes assumption that Murmur3Partitioner is used. For prototype it is simple and sufficient.
        final Token token = Murmur3Partitioner.LongToken.serializer.deserialize(in, Murmur3Partitioner.instance, version);
        final ByteBuffer ksNameByteBuffer = ByteBufferUtil.readWithLength(in);
        final String ksName = ByteBufferUtil.string(ksNameByteBuffer);
        final ByteBuffer cFNameByteBuffer = ByteBufferUtil.readWithLength(in);
        final String cFName = ByteBufferUtil.string(cFNameByteBuffer);
        return new TransactionItem(token, ksName, cFName);
    }

    public long serializedSize(TransactionItem transactionItem, int version)
    {
        int size = 0;
        size += Murmur3Partitioner.LongToken.serializer.serializedSize(transactionItem.getToken(), version);
        size += TypeSizes.sizeof(transactionItem.getKsName());
        size += TypeSizes.sizeof(transactionItem.getCfName());
        size += 4;
        return size;
    }
}
