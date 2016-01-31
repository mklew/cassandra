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

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.utils.UUIDs;
import junit.framework.Assert;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.mpp.transaction.MppCQLTester;
import org.apache.cassandra.mpp.transaction.TransactionId;
import org.apache.cassandra.mpp.transaction.TransactionTimeUUID;
import org.apache.cassandra.mpp.transaction.client.TransactionState;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 31/01/16
 */
public class FlushTransactionStatementTest extends MppCQLTester
{

    public static String FLUSH_TRANSACTION_LOCALLY = "FLUSH LOCALLY TRANSACTION ";

    private void flushTransaction(TransactionId transactionId) throws Throwable
    {
        execute(FLUSH_TRANSACTION_LOCALLY + transactionId.unwrap());
    }

    @Before
    public void prepareTable() throws Throwable {
        createMppTestCf1();
    }

    @Test
    public void shouldFailWhenThereIsNoTransactionWithSuchId() throws Throwable
    {
        final TransactionTimeUUID transactionId = new TransactionTimeUUID(UUIDs.timeBased());

        try
        {
            flushTransaction(transactionId);
        }
        catch (IllegalStateException ex)
        {
            Assert.assertTrue(ex.getMessage().contains("TransactionData not found for transaction id "));
            return;
        }
        Assert.fail();
    }

    @Test
    public void shouldFlushTransactionAndBeAbleToReadData() throws Throwable
    {
        final TransactionState transactionState = startTransaction();

        final int pk = 1001;
        final String ck = "ck";
        txInsertToCf1(transactionState, pk, ck);

        final UntypedResultSet resultSet = execute("SELECT * FROM " + keyspace() + "." + cf1Name + " WHERE pk = ? and ck = ?", pk, ck);

        Assert.assertEquals(0, resultSet.size());

        // when
        flushTransaction(transactionState.id());

        // then
        final UntypedResultSet afterFlush = execute("SELECT * FROM " + keyspace() + "." + cf1Name + " WHERE pk = ? and ck = ?", pk, ck);

        Assert.assertEquals(1, afterFlush.size());

        final Collection<Cf1Obj> objs = mapResultToCf1Objs(afterFlush);

        final Cf1Obj cf = objs.iterator().next();

        Assert.assertFalse(cf.description.isPresent());
        Assert.assertFalse(cf.number.isPresent());

        Assert.assertEquals(pk, cf.pk);
        Assert.assertEquals(ck, cf.ck);

    }


    @Test
    public void shouldFlushTransactionAndOverrideNormalWritesThatHappendedBefore() throws Throwable
    {
        final TransactionState transactionState = startTransaction();

        final int pk = 1001;
        final String ck = "ck";
        String ckOther = "ck222";

        final String originalDescription = "This is some already existing description";
        int originalNumber = 55667;

        normalInsertToCf1(pk, ck, originalDescription, originalNumber);
        final int otherNumber = 123;
        normalInsertToCf1(pk, ckOther, "some description", otherNumber);

        final String descriptionFromTransaction = "modified description";
        txInsertToCf1(transactionState, pk, ck, descriptionFromTransaction);

        // when
        flushTransaction(transactionState.id());

        // then

        Assert.assertEquals(1, execute("SELECT * FROM " + keyspace() + "." + cf1Name + " WHERE pk = ? and ck = ?", pk, ck).size());

        final UntypedResultSet afterFlush = execute("SELECT * FROM " + keyspace() + "." + cf1Name + " WHERE pk = ?", pk);

        final Collection<Cf1Obj> objs = mapResultToCf1Objs(afterFlush);

        final Cf1Obj cf = objs.stream().filter(c -> c.ck.equals(ck)).findFirst().get();
        final Cf1Obj cfOther = objs.stream().filter(c -> c.ck.equals(ckOther)).findFirst().get();

        Assert.assertEquals("Has number from original one", Integer.valueOf(originalNumber), cf.number.get());
        Assert.assertEquals("Has description from transaction", descriptionFromTransaction, cf.description.get());
        Assert.assertEquals(pk, cf.pk);
        Assert.assertEquals(ck, cf.ck);

        Assert.assertNotNull(ckOther);
        Assert.assertEquals("Other's number matches", Integer.valueOf(otherNumber), cfOther.number.get());
    }

    @Test
    public void shouldFlushTransactionAndRemoveTransactionsDataAfter() throws Throwable
    {
        final TransactionState transactionState = startTransaction();

        final int pk = 1001;
        final String ck = "ck";
        txInsertToCf1(transactionState, pk, ck);

        Assert.assertEquals(1, getMppService().getInProgressTransactions().size());;
        // when
        flushTransaction(transactionState.id());

        Assert.assertEquals(0, getMppService().getInProgressTransactions().size());;
    }
}
