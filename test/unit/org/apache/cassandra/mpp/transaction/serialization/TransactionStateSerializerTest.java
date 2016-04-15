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
import java.util.Collections;
import java.util.UUID;

import org.junit.Test;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.client.TransactionStateUtils;
import org.apache.cassandra.net.MessagingService;
import org.joda.time.DateTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 26/11/15
 */
public class TransactionStateSerializerTest
{
    final int version = MessagingService.VERSION_30;


    @Test
    public void shouldFailSerializingEmptyTransactionState() {
        final TransactionState transactionState = TransactionStateUtils.newTransactionState();

        final DataOutputBuffer out = new DataOutputBuffer();

        try
        {
            TransactionStateSerializer.instance.serialize(transactionState, out, version);
        }
        catch (IOException e)
        {
//            fail();
            // No longer true, because MpCommit has empty transaction if MpCommit is empty.
        }
    }

    private void testSerialization(TransactionState transactionState) throws IOException
    {
        final DataOutputBuffer out = new DataOutputBuffer();

        TransactionStateSerializer.instance.serialize(transactionState, out, version);

        final DataInputBuffer in = new DataInputBuffer(out.buffer(), true);

        final TransactionState deserialize = TransactionStateSerializer.instance.deserialize(in, version);

        assertEquals(transactionState, deserialize);
        final int outLength = out.getLength();
        assertEquals(outLength, TransactionStateSerializer.instance.serializedSize(transactionState, version));

    }

    @Test
    public void shouldSerializeWithSingleTransactionItem() throws IOException
    {
        final TransactionState transactionState = TransactionStateUtils.newTransactionState();
        transactionState.addTxItem(TransactionItemSerializerTest.getTransactionItem());

        testSerialization(transactionState);
    }

    @Test
    public void shouldCreateReadOnlyTransactionUsingSpecialId() throws IOException
    {
        UUID uuid = UUIDs.startOf(new DateTime().plusYears(1).plusDays(1).getMillis());
        TransactionState transactionState = new TransactionState(uuid, Collections.singletonList(TransactionItemSerializerTest.getTransactionItem()));

        assertTrue("should be read only transaction", transactionState.isReadTransaction());
    }

    @Test
    public void transactionCreatedWithNormalTimestampShouldNotBeReadOnly() throws IOException {
        final TransactionState transactionState = TransactionStateUtils.newTransactionState();
        transactionState.addTxItem(TransactionItemSerializerTest.getTransactionItem());

        assertFalse("normal timestamp should not be read only transaction", transactionState.isReadTransaction());
    }

    @Test
    public void shouldSerializeWithMoreThanOneTransactionItem() throws IOException
    {
        final TransactionState transactionState = TransactionStateUtils.newTransactionState();
        transactionState.addTxItem(TransactionItemSerializerTest.getTransactionItem());
        transactionState.addTxItem(TransactionItemSerializerTest.getTransactionItem());
        transactionState.addTxItem(TransactionItemSerializerTest.getTransactionItem());

        testSerialization(transactionState);
    }
}
