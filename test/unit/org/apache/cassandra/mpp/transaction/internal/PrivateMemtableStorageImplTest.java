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

import org.junit.Test;

import com.datastax.driver.core.utils.UUIDs;
import junit.framework.Assert;
import org.apache.cassandra.MockSchema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.mpp.transaction.PrivateMemtableStorage;
import org.apache.cassandra.mpp.transaction.TransactionData;
import org.apache.cassandra.mpp.transaction.TransactionId;
import org.apache.cassandra.mpp.transaction.TransactionTimeUUID;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 01/11/15
 */
public class PrivateMemtableStorageImplTest
{
    @Test
    public void shouldReturnEmptyTransactionDataIfNothingExistsForGivenTransactionId() {
        final PrivateMemtableStorage privateMemtableStorage = new PrivateMemtableStorageImpl();

        final TransactionId txId = new TransactionTimeUUID(UUIDs.timeBased());

        final TransactionData transactionData = privateMemtableStorage.readTransactionData(txId);

        Assert.assertNotNull(transactionData);
        Assert.assertSame(transactionData.getClass(), EmptyTransactionData.class);
    }

    @Test
    public void shouldCreateTransactionDataForNotExistingTransaction() {
        final PrivateMemtableStorage privateMemtableStorage = new PrivateMemtableStorageImpl();
        String fakeCfs = "mockcf1";
        ColumnFamilyStore cfs = MockSchema.newCFS(); // creates mockcf1
        Mutation m = new RowUpdateBuilder(cfs.metadata, 5, "partitionKey").clustering("clustering").add("value", "someVal").build();

        final TransactionId txId = new TransactionTimeUUID(UUIDs.timeBased());

        // when
        privateMemtableStorage.storeMutation(txId, m);

        // then
        final TransactionData transactionData = privateMemtableStorage.readTransactionData(txId);
        Assert.assertNotNull(transactionData);
        Assert.assertTrue("should contain cf: " + fakeCfs + " but had " + transactionData.modifiedCfs(), transactionData.modifiedCfs().contains(fakeCfs));
        Assert.assertSame(TransactionDataImpl.class, transactionData.getClass());
    }
}
