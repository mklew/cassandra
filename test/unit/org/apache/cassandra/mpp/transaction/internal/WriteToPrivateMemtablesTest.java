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

import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.mpp.transaction.MppCQLTester;
import org.apache.cassandra.mpp.transaction.MppServiceUtils;
import org.apache.cassandra.mpp.transaction.client.TransactionItem;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;
import org.joda.time.DateTime;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 21/01/16
 */
public class WriteToPrivateMemtablesTest extends MppCQLTester
{

    @Test
    public void testWritingScenario() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s text, i int)");
        int modifiedKey = 1001;
        final String expectedTextValue = "text123";
        UUID txId = UUIDGen.getTimeUUID(DateTime.now().minusSeconds(10).toInstant().getMillis());
        execute("INSERT INTO %s (k, s, i) VALUES (?, ?, ?)", 15, "abc", 11);
        final UntypedResultSet resultSet = execute("INSERT INTO %s (k, s, i) VALUES (?, ?, ?) USING TRANSACTION " + txId, modifiedKey, expectedTextValue, 10);
        Assert.assertNotNull("Mutation using transaction should return results with transaction item", resultSet);

        Collection<TransactionItem> transactionItems = mapResultSetToTransactionItems(resultSet);

        final CFMetaData cfMetaData = currentTableMetadata();
        final String ksName = keyspace();
        final String cfName = currentTable();
        final Murmur3Partitioner.LongToken token = Murmur3Partitioner.instance.getToken(ByteBufferUtil.bytes(modifiedKey));

        Assert.assertEquals(1, transactionItems.size());
        final TransactionItem txItem = transactionItems.iterator().next();

        Assert.assertEquals(ksName, txItem.getKsName());
        Assert.assertEquals(cfName, txItem.getCfName());
        Assert.assertEquals(token, txItem.getToken());
    }

    private static Collection<TransactionItem> mapResultSetToTransactionItems(UntypedResultSet resultSet)
    {
        final Stream<UntypedResultSet.Row> stream = streamResultSet(resultSet);
        return stream.map(ROW_TO_TX_ITEM).collect(Collectors.toList());
    }

    private final static Function<UntypedResultSet.Row, TransactionItem> ROW_TO_TX_ITEM = row -> {
        final String ksName = row.getString(MppServiceUtils.KS_NAME_COL);
        final String cfName = row.getString(MppServiceUtils.CF_NAME_COL);
        final long token = row.getLong(MppServiceUtils.TOKEN_NAME_COL);
        return new TransactionItem(token, ksName, cfName);
    };

    private static Stream<UntypedResultSet.Row> streamResultSet(UntypedResultSet resultSet)
    {
        final Iterator<UntypedResultSet.Row> iterator = resultSet.iterator();
        Iterable<UntypedResultSet.Row> iterable = () -> iterator;
        return StreamSupport.stream(iterable.spliterator(), false);
    }
}
