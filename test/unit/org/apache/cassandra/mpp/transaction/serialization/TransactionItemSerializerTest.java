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
import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.net.MessagingService;

import static junit.framework.Assert.assertEquals;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 26/11/15
 */
public class TransactionItemSerializerTest
{
    private static final Random random = new Random(5);

    @Test
    public void testSerialization() throws IOException
    {
        final TransactionItem transactionItem = getTransactionItem();

        final DataOutputBuffer out = new DataOutputBuffer();
        final int version = MessagingService.VERSION_30;

        TransactionItemSerializer.instance.serialize(transactionItem, out, version);


        DataInputPlus in = new DataInputBuffer(out.buffer(), true);
        final TransactionItem deserialize = TransactionItemSerializer.instance.deserialize(in, version);

        assertEquals(transactionItem, deserialize);
    }

    static TransactionItem getTransactionItem()
    {
        final String table1 = "table" + random.nextInt();
        final String ksName = "some-keyspace-name" + random.nextInt();
        final Murmur3Partitioner.LongToken token = Murmur3Partitioner.instance.getRandomToken(random);
        return new TransactionItem(token, ksName, table1);
    }

    @Test
    public void testValidSizeSerialization() throws IOException
    {
        final TransactionItem transactionItem = getTransactionItem();

        final DataOutputBuffer out = new DataOutputBuffer();
        final int version = MessagingService.VERSION_30;

        TransactionItemSerializer.instance.serialize(transactionItem, out, version);

        final int serializedSize = (int) TransactionItemSerializer.instance.serializedSize(transactionItem, version);

        assertEquals(out.getLength(), serializedSize);
    }
}
