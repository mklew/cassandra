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

package org.apache.cassandra.mpp.transaction;

import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.mpp.MppExtensionServices;
import org.apache.cassandra.mpp.transaction.client.TransactionState;

import static org.apache.cassandra.mpp.transaction.MppTestingUtilities.START_TRANSACTION;
import static org.apache.cassandra.mpp.transaction.MppTestingUtilities.mapResultToTransactionState;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 21/01/16
 */
public abstract class MppCQLTester extends CQLTester
{
    protected static MppExtensionServices mppExtensionServices;

    @BeforeClass
    public static void mppSetupClassBefore() {
        mppExtensionServices = new MppExtensionServices();

        mppExtensionServices.startWithoutNetwork();
    }

    protected MppService getMppService() {
        return mppExtensionServices.getMppModule().getMppService();
    }

    @AfterClass
    public static void mppSetupClassAfter() {
        mppExtensionServices.stop();
    }

    public static final String ROLLBACK_TRANSACTION_LOCALLY = "ROLLBACK TRANSACTION LOCALLY ";

    protected void performRollback(UUID txId) throws Throwable
    {
        execute(ROLLBACK_TRANSACTION_LOCALLY + txId);
    }

    protected TransactionState startTransaction() throws Throwable
    {
        return mapResultToTransactionState(execute(START_TRANSACTION));
    }

    protected String cf1Name;

    protected String cf2Name;

    protected void createMppTestCf1() {
        cf1Name = createTable("CREATE TABLE %s (pk int, ck text, description text, number int, PRIMARY KEY(pk, ck))");
    }

    protected void createMppTestCf2() {
        cf2Name = createTable("CREATE TABLE %s (pk int, ck text, price int, PRIMARY KEY(pk, ck))");
    }

    protected TransactionState txInsertToCf2(TransactionState transactionState, int pk, String ck, int price) throws Throwable {
        final String cql = "INSERT INTO " + keyspace() +"." + cf2Name + " (pk, ck, description, price) values (?, ?, ?) USING TRANSACTION " + transactionState.getTransactionId();
        final UntypedResultSet encodedTxState = execute(cql, pk, ck, price);
        return mapResultToTransactionState(encodedTxState);
    }

    protected TransactionState txInsertToCf1(TransactionState transactionState, int pk, String ck, String description, int number) throws Throwable {
        final String cql = "INSERT INTO " + keyspace() +"." + cf1Name + " (pk, ck, description, number) values (?, ?, ?, ?) USING TRANSACTION " + transactionState.getTransactionId();
        final UntypedResultSet encodedTxState = execute(cql, pk, ck, description, number);
        return mapResultToTransactionState(encodedTxState);
    }

    protected void normalInsertToCf1(int pk, String ck, String description, Integer number) throws Throwable {
        final String cql = "INSERT INTO " + keyspace() +"." + cf1Name + " (pk, ck, description, number) values (?, ?, ?, ?)" ;
        execute(cql, pk, ck, description, number);
    }

    protected TransactionState txInsertToCf1(TransactionState transactionState, int pk, String ck, String description) throws Throwable {
        final String cql = "INSERT INTO " + keyspace() +"." + cf1Name + " (pk, ck, description) values (?, ?, ?) USING TRANSACTION " + transactionState.getTransactionId();
        final UntypedResultSet encodedTxState = execute(cql, pk, ck, description);
        return mapResultToTransactionState(encodedTxState);
    }

    protected TransactionState txInsertToCf1(TransactionState transactionState, int pk, String ck, Integer number) throws Throwable {
        final String cql = "INSERT INTO " + keyspace() +"." + cf1Name + " (pk, ck, number) values (?, ?, ?) USING TRANSACTION " + transactionState.getTransactionId();
        final UntypedResultSet encodedTxState = execute(cql, pk, ck, number);
        return mapResultToTransactionState(encodedTxState);
    }

    protected TransactionState txInsertToCf1(TransactionState transactionState, int pk, String ck) throws Throwable {
        final String cql = "INSERT INTO " + keyspace() +"." + cf1Name + " (pk, ck) values (?, ?) USING TRANSACTION " + transactionState.getTransactionId();
        final UntypedResultSet encodedTxState = execute(cql, pk, ck);
        return mapResultToTransactionState(encodedTxState);
    }

    protected static class Cf1Obj {
        public int pk;

        public String ck;

        public Optional<String> description;

        public Optional<Integer> number;

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

    protected Collection<Cf1Obj> readTransactionalLocallyFromCf1(TransactionState transactionState) throws Throwable {
        final UntypedResultSet rowResults = execute("READ TRANSACTIONAL LOCALLY TRANSACTION " +
                                                    transactionState.getTransactionId()
                                                    + " FROM " + keyspace()+"."+cf1Name);
        return mapResultToCf1Objs(rowResults);
    }

    protected static Collection<Cf1Obj> mapResultToCf1Objs(UntypedResultSet rowResults)
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

}
