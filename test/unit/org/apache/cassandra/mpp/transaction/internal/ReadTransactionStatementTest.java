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

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.mpp.transaction.MppCQLTester;
import org.apache.cassandra.mpp.transaction.client.TransactionState;

import static org.apache.cassandra.mpp.transaction.MppTestingUtilities.START_TRANSACTION;
import static org.apache.cassandra.mpp.transaction.MppTestingUtilities.mapResultToTransactionState;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 24/01/16
 */
public class ReadTransactionStatementTest extends MppCQLTester
{

    @Test
    public void shouldReadTransactionLocallyByTransactionId() throws Throwable {
        final TransactionState transactionState = mapResultToTransactionState(execute(START_TRANSACTION));

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
        final TransactionState transactionState = mapResultToTransactionState(execute(START_TRANSACTION));

        // TODO insert something

        final UntypedResultSet resultsWithJson = execute("READ TRANSACTIONAL LOCALLY AS JSON TRANSACTION " + transactionState.getTransactionId());

        // TODO Assert that resultsWithJson have TransactionState encoded in json. 1 row, 1 column
    }

    @Test
    public void shouldReadTransactionLocallyAndReturnResultsFromSpecificColumnFamily() throws Throwable {
        // begin transaction
        final TransactionState transactionState = mapResultToTransactionState(execute(START_TRANSACTION));

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
        final TransactionState transactionState = mapResultToTransactionState(execute(START_TRANSACTION));

        String cfName = null; // TODO set it

        execute("READ TRANSACTIONAL LOCALLY AS JSON TRANSACTION " + transactionState.getTransactionId() + " FROM " + cfName);

        // TODO EXCEPTION EXPECTED, beucase I don't know how to represent rows from different column families in json.
        // TODO or actually I think I know, but there is no point in having that functionality
    }

    @Test
    public void shouldReadTransactionLocallyAndReturnRowForColumnFamilyAndToken() throws Throwable {
        final TransactionState transactionState = mapResultToTransactionState(execute(START_TRANSACTION));

        // TODO Make at least 2 writes to SAME column family
        String cfName = null; // TODO set it
        Long token = null; // TODO set it, take it from response of transactional write

        final UntypedResultSet singleRowExpected = execute("READ TRANSACTIONAL LOCALLY TRANSACTION "
                                                           + transactionState.getTransactionId()
                                                           + " FROM " + cfName
                                                           + " TOKEN " + token);

        // TODO Assert that row has expected data
    }

    // TODO TEST IT IN MPP_TEST because it requires network READ TRANSACTIONAL TRANSACTION AS JSON <transactionStateInJson> FROM <columnFamilyName>

    // TODO TEST IT IN MPP_TEST because it also requires network READ TRANSACTIONAL TRANSACTION AS JSON <transactionStateInJson> FROM <columnFamilyName> TOKEN <token>


    @Test
    public void shouldReadLocalTransactionWhenThereIsNoDataReturningNullResponse() throws Throwable {
        // open transaction
        final TransactionState transactionState = mapResultToTransactionState(execute(START_TRANSACTION));

        final UntypedResultSet execute = execute("READ TRANSACTION LOCALLY " + transactionState.getTransactionId());
    }

    @Test
    public void shouldReadLocalTransactionWhenThereIsData() throws Throwable {
        // open transaction
        final TransactionState transactionState = mapResultToTransactionState(execute(START_TRANSACTION));

        createTable("CREATE TABLE %s (k int PRIMARY KEY, s text, i int)");
        int modifiedKey = 101234;
        final String expectedTextValue = "this is some text";
        UUID txId = transactionState.getTransactionId();

        // write to transaction
        final UntypedResultSet resultSet = execute("INSERT INTO %s (k, s, i) VALUES (?, ?, ?) USING TRANSACTION " + txId, modifiedKey, expectedTextValue, 10);

    }
}
