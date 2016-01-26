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
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.mpp.transaction.MppCQLTester;
import org.apache.cassandra.mpp.transaction.MppServiceUtils;
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

        // TODO insert something

        final UntypedResultSet resultsWithJson = execute("READ TRANSACTIONAL LOCALLY AS JSON TRANSACTION " + transactionState.getTransactionId());

        // TODO Assert that resultsWithJson have TransactionState encoded in json. 1 row, 1 column
    }

    String cf1Name;


    private void createMppTestCf1() {
        cf1Name = createTable("CREATE TABLE %s (pk int, ck text, description text, number int, PRIMARY KEY(pk, ck))");
    }

    private TransactionState txInsertToCf1(TransactionState transactionState, int pk, String ck, String description, int number) throws Throwable {
        final String cql = "INSERT INTO " + keyspace() +"." + cf1Name + " (pk, ck, description, number) values (?, ?, ?, ?) USING TRANSACTION " + transactionState.getTransactionId();
        final UntypedResultSet encodedTxState = execute(cql, pk, ck, description, number);
        return mapResultToTransactionState(encodedTxState);
    }

    private TransactionState txInsertToCf1(TransactionState transactionState, int pk, String ck, String description) throws Throwable {
        final String cql = "INSERT INTO " + keyspace() +"." + cf1Name + " (pk, ck, description) values (?, ?, ?) USING TRANSACTION " + transactionState.getTransactionId();
        final UntypedResultSet encodedTxState = execute(cql, pk, ck, description);
        return mapResultToTransactionState(encodedTxState);
    }

    private TransactionState txInsertToCf1(TransactionState transactionState, int pk, String ck, Integer number) throws Throwable {
        final String cql = "INSERT INTO " + keyspace() +"." + cf1Name + " (pk, ck, number) values (?, ?, ?) USING TRANSACTION " + transactionState.getTransactionId();
        final UntypedResultSet encodedTxState = execute(cql, pk, ck, number);
        return mapResultToTransactionState(encodedTxState);
    }

    private TransactionState txInsertToCf1(TransactionState transactionState, int pk, String ck) throws Throwable {
        final String cql = "INSERT INTO " + keyspace() +"." + cf1Name + " (pk, ck) values (?, ?) USING TRANSACTION " + transactionState.getTransactionId();
        final UntypedResultSet encodedTxState = execute(cql, pk, ck);
        return mapResultToTransactionState(encodedTxState);
    }

    private static class Cf1Obj {
        int pk;

        String ck;

        Optional<String> description;

        Optional<Integer> number;

        public Cf1Obj(int pk, String ck, Optional<String> description, Optional<Integer> number)
        {
            Preconditions.checkNotNull(ck);
            this.pk = pk;
            this.ck = ck;
            this.description = description;
            this.number = number;
        }

        public static Cf1Obj createNew(int pk, String ck) {
            return new Cf1Obj(pk, ck, Optional.empty(), Optional.<Integer>empty());
        }

        public static Cf1Obj createNew(int pk, String ck, String description) {
            return new Cf1Obj(pk, ck, Optional.of(description), Optional.<Integer>empty());
        }

        public static Cf1Obj createNew(int pk, String ck, int number) {
            return new Cf1Obj(pk, ck, Optional.empty(), Optional.of(number));
        }

        public static Cf1Obj createNew(int pk, String ck, String description, int number) {
            return new Cf1Obj(pk, ck, Optional.of(description), Optional.of(number));
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Cf1Obj cf1Obj = (Cf1Obj) o;

            if (pk != cf1Obj.pk) return false;
            if (!ck.equals(cf1Obj.ck)) return false;
            if (description != null ? !description.equals(cf1Obj.description) : cf1Obj.description != null)
                return false;
            if (number != null ? !number.equals(cf1Obj.number) : cf1Obj.number != null) return false;

            return true;
        }

        public int hashCode()
        {
            int result = pk;
            result = 31 * result + ck.hashCode();
            result = 31 * result + (description != null ? description.hashCode() : 0);
            result = 31 * result + (number != null ? number.hashCode() : 0);
            return result;
        }
    }

    private Collection<Cf1Obj> readTransactionalLocallyFromCf1(TransactionState transactionState) throws Throwable {
        final UntypedResultSet rowResults = execute("READ TRANSACTIONAL LOCALLY TRANSACTION " +
                                                    transactionState.getTransactionId()
                                                    + " FROM " + keyspace()+"."+cf1Name);
        return mapResultToCf1Objs(rowResults);
    }

    private static Collection<Cf1Obj> mapResultToCf1Objs(UntypedResultSet rowResults)
    {
        return MppServiceUtils.streamResultSet(rowResults).map(r -> {
            final int pk = r.getInt("pk");
            final String ck = r.getString("ck");
            final String descriptionColumnName = "description";
            String description = null;
            if (r.has(descriptionColumnName))
            {
                description = r.getString(descriptionColumnName);
            }
            String numberColumnName = "number";
            Integer number = null;
            if (r.has(numberColumnName))
            {
                number = r.getInt(numberColumnName);
            }
            return new Cf1Obj(pk, ck, Optional.ofNullable(description), Optional.ofNullable(number));
        }).collect(Collectors.toList());
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

    private TransactionState startTransaction() throws Throwable
    {
        return mapResultToTransactionState(execute(START_TRANSACTION));
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

    // TODO TEST IT IN MPP_TEST because it requires network READ TRANSACTIONAL TRANSACTION AS JSON <transactionStateInJson> FROM <columnFamilyName>

    // TODO TEST IT IN MPP_TEST because it also requires network READ TRANSACTIONAL TRANSACTION AS JSON <transactionStateInJson> FROM <columnFamilyName> TOKEN <token>


    @Test
    public void shouldReadLocalTransactionWhenThereIsNoDataReturningNullResponse() throws Throwable {
        // open transaction
        final TransactionState transactionState = startTransaction();

        final UntypedResultSet execute = execute("READ TRANSACTION LOCALLY " + transactionState.getTransactionId());
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
