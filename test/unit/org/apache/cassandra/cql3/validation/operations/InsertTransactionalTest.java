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

package org.apache.cassandra.cql3.validation.operations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.mpp.transaction.PrivateMemtableStorage;
import org.apache.cassandra.mpp.transaction.PrivateMemtableStorageLocator;
import org.apache.cassandra.mpp.transaction.TransactionData;
import org.apache.cassandra.mpp.transaction.TransactionId;
import org.apache.cassandra.mpp.transaction.TransactionTimeUUID;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.utils.UUIDGen;
import org.joda.time.DateTime;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 29/11/15
 */
public class InsertTransactionalTest extends CQLTester
{
    @Test
    public void shouldInsertIntoPrivateMemtable() throws Throwable {
        // GIVEN
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s text, i int)");

        UUID txId = UUIDGen.getTimeUUID(DateTime.now().minusSeconds(10).toInstant().getMillis());

        final FakeWriteOnlyPrivateMemtableStorage fakeStorage = new FakeWriteOnlyPrivateMemtableStorage();

        // TODO [MPP] if this is static then it will be shared between all unit tests which means that it might not work if tests are run in parallel.
        PrivateMemtableStorageLocator.instance.setFakePrivateMemtableStorage(fakeStorage);

        // WHEN
        final String expectedTextValue = "text123";
        execute("INSERT INTO %s (k, s, i) VALUES (10, ?, ?) USING TRANSACTION " + txId, expectedTextValue, 10);

        // THEN

        Assert.assertEquals(1, fakeStorage.recordedWrites.size());
        final RecordedStorageInvocation invocation = fakeStorage.recordedWrites.get(0);

        final TransactionTimeUUID expectedTxId = new TransactionTimeUUID(txId);
        Assert.assertEquals(expectedTxId, invocation.txId);
        Assert.assertEquals(1, invocation.mutation.getPartitionUpdates().size());
        final PartitionUpdate partitionUpdate = invocation.mutation.getPartitionUpdates().iterator().next();

        Row next = partitionUpdate.iterator().next();
        final ColumnDefinition iColumn = ColumnDefinition.regularDef(keyspace(), currentTable(), "i", Int32Type.instance);
        final ColumnDefinition sColumn = ColumnDefinition.regularDef(keyspace(), currentTable(), "s", UTF8Type.instance);
        final Cell iCell = next.getCell(iColumn);
        final Cell sCell = next.getCell(sColumn);

        final Integer deserialize = Int32Type.instance.getSerializer().deserialize(iCell.value());
        Assert.assertEquals(new Integer(10), deserialize);
        final String actualText = UTF8Type.instance.getSerializer().deserialize(sCell.value());
        Assert.assertEquals(expectedTextValue, actualText);
    }

    public static <T> Stream<T> asStream(Iterator<T> sourceIterator) {
        return asStream(sourceIterator, false);
    }

    public static <T> Stream<T> asStream(Iterator<T> sourceIterator, boolean parallel) {
        Iterable<T> iterable = () -> sourceIterator;
        return StreamSupport.stream(iterable.spliterator(), parallel);
    }

    private static class RecordedStorageInvocation
    {
        private final TransactionId txId;
        private final Mutation mutation;

        private RecordedStorageInvocation(TransactionId txId, Mutation mutation)
        {
            this.txId = txId;
            this.mutation = mutation;
        }
    }


    private static class FakeWriteOnlyPrivateMemtableStorage implements PrivateMemtableStorage
    {
        private final List<RecordedStorageInvocation> recordedWrites = new ArrayList<>();

        public void storeMutation(TransactionId txId, Mutation mutation)
        {
            recordedWrites.add(new RecordedStorageInvocation(txId, mutation));
        }

        public TransactionData readTransactionData(TransactionId txId)
        {
            throw new IllegalStateException("Does not expect read");
        }

        public Map<TransactionItem, Optional<PartitionUpdate>> readTransactionItems(TransactionId transactionId, List<TransactionItem> transactionItems)
        {
            return null;
        }

        public boolean transactionExistsInStorage(TransactionId transactionId)
        {
            throw new IllegalStateException("Not expected to be used");
        }

        public void removePrivateData(TransactionId id)
        {
            throw new IllegalStateException("Not expected to be used");
        }

        public Collection<TransactionId> getInProgressTransactions()
        {
            return Collections.emptyList();
        }
    }

}
