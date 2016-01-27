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
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.mpp.transaction.MppCQLTester;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.client.dto.TransactionItemDto;
import org.apache.cassandra.mpp.transaction.client.dto.TransactionStateDto;

import static org.apache.cassandra.mpp.transaction.MppTestingUtilities.mapResultToTransactionState;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 24/01/16
 */
public class ReadTransactionStatementTest extends MppCQLTester
{

    @Test
    public void shouldReadTransactionLocallyByTransactionId() throws Throwable {
        final TransactionState transactionState = startTransaction();

        final String cfName1 = createTable("CREATE TABLE %s (k int PRIMARY KEY, s text, i int)");
        int modifiedKey = 101234;
        int key2 = 123;
        final String expectedTextValue = "this is some text";
        UUID txId = transactionState.getTransactionId();

        final UntypedResultSet resultSet1 = execute("INSERT INTO %s (k, s, i) VALUES (?, ?, ?) USING TRANSACTION " + txId, modifiedKey, expectedTextValue, 10);

        final String cfName2 = createTable("CREATE TABLE %s (k int PRIMARY KEY, b int)");


        final UntypedResultSet resultSet2 = execute("INSERT INTO %s (k, b) VALUES (?, ?) USING TRANSACTION " + txId, key2, 12351);

        final UntypedResultSet transationStateAsRows = execute("READ TRANSACTIONAL LOCALLY TRANSACTION " + transactionState.getTransactionId());
        Assert.assertEquals(2, transationStateAsRows.size());
        System.out.println(transationStateAsRows);

        final TransactionState actualTransactionState = mapResultToTransactionState(transationStateAsRows);
        Assert.assertEquals(2, actualTransactionState.getTransactionItems().size());
        Assert.assertEquals(txId, actualTransactionState.getTransactionId());

        Assert.assertEquals(1, actualTransactionState.getTransactionItems().stream().filter(ti -> ti.getCfName().equals(cfName1)).count());
        Assert.assertEquals(1, actualTransactionState.getTransactionItems().stream().filter(ti -> ti.getCfName().equals(cfName2)).count());
    }

    @Test
    public void shouldReadTransactionLocallyByTransactionIdAndGetJsonResponse() throws Throwable {
        final TransactionState transactionState = startTransaction();
        createMppTestCf1();

        txInsertToCf1(transactionState, 10, "a");
        txInsertToCf1(transactionState, 1251, "bbb");
        txInsertToCf1(transactionState, 12314, "bbb", "some description", 14);

        final UntypedResultSet resultsWithJson = execute("READ TRANSACTIONAL LOCALLY AS JSON TRANSACTION " + transactionState.getTransactionId());

        Assert.assertEquals(1, resultsWithJson.size());
        final String json = resultsWithJson.one().getString(Json.JSON_COLUMN_ID.toString());

        final TransactionStateDto transactionStateDto = Json.JSON_OBJECT_MAPPER.readValue(json, TransactionStateDto.class);

        Assert.assertEquals(3, transactionStateDto.getTransactionItems().size());

        final TransactionItemDto ti = transactionStateDto.getTransactionItems().iterator().next();
        Assert.assertEquals(cf1Name, ti.getCfName());

        System.out.println(json);
    }


    @Test
    public void shouldReadTransactionLocallyAndReturnResultsFromSpecificColumnFamilyTest2() throws Throwable {
        createMppTestCf1();

        TransactionState transactionState = startTransaction();

        final int pk = 10;
        final String ck = "ThisIs10";
        final String description = "Some awesome description";
        final int number = 123;
        transactionState = transactionState.merge(txInsertToCf1(transactionState, pk, ck, description, number));

        final Collection<Cf1Obj> cf1Objs = readTransactionalLocallyFromCf1(transactionState);

        Assert.assertEquals(1, cf1Objs.size());

        final Cf1Obj cf1Obj = findExactlyOne(pk, ck, cf1Objs);

        Assert.assertEquals(description, cf1Obj.description.get());
        Assert.assertEquals(Integer.valueOf(number), cf1Obj.number.get());

        final Cf1Obj otherOne = Cf1Obj.createNew(500, "OtherOneIs500", 889);

        transactionState = transactionState.merge(txInsertToCf1(transactionState, otherOne.pk, otherOne.ck, otherOne.number.get()));

        final Collection<Cf1Obj> shouldHave2 = readTransactionalLocallyFromCf1(transactionState);

        Assert.assertEquals(2, shouldHave2.size());

        final Cf1Obj actualOtherOne = findExactlyOne(otherOne.pk, otherOne.ck, shouldHave2);

        Assert.assertEquals(otherOne, actualOtherOne);

        // then rollback
        performRollback(transactionState.getTransactionId());

        final Collection<Cf1Obj> shouldBeEmpty = readTransactionalLocallyFromCf1(transactionState);

        Assert.assertTrue("After rollback it should not return any results", shouldBeEmpty.isEmpty());
    }

    private static Cf1Obj findExactlyOne(int pk, String ck, Collection<Cf1Obj> cf1Objs)
    {
        final List<Cf1Obj> found = cf1Objs.stream().filter(x -> x.ck.equals(ck) && x.pk == pk).collect(Collectors.toList());
        Assert.assertEquals(1, found.size());
        return found.iterator().next();
    }


    @Test
    public void shouldReadTransactionLocallyAndReturnResultsFromSpecificColumnFamily() throws Throwable {
        // start transaction
        final TransactionState transactionState = startTransaction();

        final String cfName = createTable("CREATE TABLE %s (k int PRIMARY KEY, s text, i int)");
        UUID txId = transactionState.getTransactionId();

        int modifiedKey = 101234;
        int key2 = 123;
        final String expectedTextValue = "this is some text";
        final String keyspace = keyspace();
        // insert transactional
        final int expectedIValue = 10;
        final UntypedResultSet ignoreTransactionItemAsRow = execute("INSERT INTO %s (k, s, i) VALUES (?, ?, ?) USING TRANSACTION " + txId, modifiedKey, expectedTextValue, expectedIValue);

        final UntypedResultSet rowResults = execute("READ TRANSACTIONAL LOCALLY TRANSACTION " +
                                                 transactionState.getTransactionId()
                                                 + " FROM " + keyspace+"."+cfName);

        Assert.assertEquals(1, rowResults.size());

        final UntypedResultSet.Row one = rowResults.one();
        final int kValue = one.getInt("k");
        final String sValue = one.getString("s");
        final int iValue = one.getInt("i");

        Assert.assertEquals(modifiedKey, kValue);
        Assert.assertEquals(expectedTextValue, sValue);
        Assert.assertEquals(expectedIValue, iValue);

//        TODO assert that rows are correct rows for that column family

//        final String text2 = expectedTextValue + " added some text";
//        final UntypedResultSet resultSet2 = execute("INSERT INTO %s (k, s, i) VALUES (?, ?, ?) USING TRANSACTION " + txId, modifiedKey + 123, text2, 124);
    }



    @Test
    public void shouldFailOnIllegalStatement() throws Throwable {
        final TransactionState transactionState = startTransaction();
        createMppTestCf1();
        // It is possible to implement, but for now I don't see any need for it.
        // It should be enough to read rows from CF just in normal form - it will be for debugging purposes anyway
        assertInvalidMessage("Returning rows in json form is not supported operation", "READ TRANSACTIONAL LOCALLY AS JSON TRANSACTION " + transactionState.getTransactionId() + " FROM " + keyspace()+"."+cf1Name);
    }

    @Test
    public void shouldReadTransactionLocallyAndReturnRowForColumnFamilyAndToken() throws Throwable {
        final TransactionState transactionState = startTransaction();
        createMppTestCf1();
        // TODO Make at least 2 writes to SAME column family
        String cfName = null; // TODO set it

        txInsertToCf1(transactionState, 1, "some pk", 123);
        final int pk = 12351;
        final String ck = "this is completely different pk";
        Long token = (Long) txInsertToCf1(transactionState, pk, ck, "with some description").singleToken().getTokenValue();

        txInsertToCf1(transactionState, 555, "yet another", "desc", 1);

        final Collection<Cf1Obj> cf1Objs = mapResultToCf1Objs(execute("READ TRANSACTIONAL LOCALLY TRANSACTION "
                                                                      + transactionState.getTransactionId()
                                                                      + " FROM " + keyspace() + "." + cf1Name
                                                                      + " TOKEN " + token));

        Assert.assertEquals(1, cf1Objs.size());
        findExactlyOne(pk, ck, cf1Objs);

    }

    @Test
    public void shouldAcceptTransactionStateEncodedInJson() throws Throwable {
        final TransactionState transactionState = startTransaction();
        createMppTestCf1();

        txInsertToCf1(transactionState, 10, "a");
        txInsertToCf1(transactionState, 1251, "bbb");
        txInsertToCf1(transactionState, 12314, "bbb", "some description", 14);

        final UntypedResultSet resultsWithJson = execute("READ TRANSACTIONAL LOCALLY AS JSON TRANSACTION " + transactionState.getTransactionId());

        Assert.assertEquals(1, resultsWithJson.size());
        final String json = resultsWithJson.one().getString(Json.JSON_COLUMN_ID.toString());


        execute("READ TRANSACTIONAL TRANSACTION AS JSON '" + json + "' FROM " + keyspace()+"." + cf1Name);

    }

    // TODO TEST IT IN MPP_TEST because it requires network READ TRANSACTIONAL TRANSACTION AS JSON <transactionStateInJson> FROM <columnFamilyName>

    // TODO TEST IT IN MPP_TEST because it also requires network READ TRANSACTIONAL TRANSACTION AS JSON <transactionStateInJson> FROM <columnFamilyName> TOKEN <token>


    @Test
    public void shouldReadLocalTransactionWhenThereIsNoDataReturningTxStateWithNoTxItems() throws Throwable {
        // open transaction
        final TransactionState transactionState = startTransaction();

        final UntypedResultSet execute = execute("READ TRANSACTIONAL LOCALLY TRANSACTION " + transactionState.getTransactionId());
        final TransactionState actualState = mapResultToTransactionState(execute);
        Assert.assertEquals(0, actualState.getTransactionItems().size());
    }

    @Test
    public void shouldReadLocalTransactionWhenThereIsData() throws Throwable {
        // open transaction
        final TransactionState transactionState = startTransaction();

        createTable("CREATE TABLE %s (k int PRIMARY KEY, s text, i int)");
        int modifiedKey = 101234;
        final String expectedTextValue = "this is some text";
        UUID txId = transactionState.getTransactionId();

        // write to transaction
        final UntypedResultSet resultSet = execute("INSERT INTO %s (k, s, i) VALUES (?, ?, ?) USING TRANSACTION " + txId, modifiedKey, expectedTextValue, 10);

    }
}
