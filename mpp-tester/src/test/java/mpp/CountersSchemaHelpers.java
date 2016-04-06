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

package mpp;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import org.apache.cassandra.mpp.transaction.MppTestingUtilities;
import org.apache.cassandra.mpp.transaction.client.TransactionState;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 06/04/16
 */
public class CountersSchemaHelpers
{
    public static final ConsistencyLevel consistencyLevel = ConsistencyLevel.LOCAL_TRANSACTIONAL;

    public static class CounterData {
        UUID id; // partition key
        int counter1;
        int counter2;
        int counter3;
        int counter4;
        int counter5;

        public CounterData(UUID id, int counter1, int counter2, int counter3, int counter4, int counter5)
        {
            this.id = id;
            this.counter1 = counter1;
            this.counter2 = counter2;
            this.counter3 = counter3;
            this.counter4 = counter4;
            this.counter5 = counter5;
        }

        public static CounterData newUsingId(UUID counterId)
        {
            return new CounterData(counterId, 0, 0, 0, 0, 0);
        }
    }

    public static class CounterTable {
        String keyspaceName;
        String tableName;

        public CounterTable(String keyspaceName, String tableName)
        {
            this.keyspaceName = keyspaceName;
            this.tableName = tableName;
        }

        public void persist(CounterData counterData, Session session) {
            String cql = getCql();
            executeCqlWithCounterData(counterData, session, cql);
        }

        private static ResultSet executeCqlWithCounterData(CounterData counterData, Session session, String cql)
        {
            SimpleStatement simpleStatement = new SimpleStatement(cql, counterData.id,
                                                                  counterData.counter1,
                                                                  counterData.counter2,
                                                                  counterData.counter3,
                                                                  counterData.counter4,
                                                                  counterData.counter5);
            simpleStatement.setConsistencyLevel(ConsistencyLevel.ALL);
            return session.execute(simpleStatement);
        }

        public Optional<CounterData> findById(UUID id, Session session) {
            String cqlQuery = String.format("SELECT * FROM %s.%s WHERE id = ?", keyspaceName, tableName);
            SimpleStatement simpleStatement = new SimpleStatement(cqlQuery, id);
            simpleStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
            ResultSet resultSet = session.execute(simpleStatement);
            return readRows(resultSet).stream().findFirst();
        }

        public static List<CounterData> readRows(ResultSet resultSet)
        {
            return StreamSupport.stream(resultSet.spliterator(), false).map(row -> {
                UUID id = row.getUUID("id");
                int counter1 = row.getInt("counter1");
                int counter2 = row.getInt("counter2");
                int counter3 = row.getInt("counter3");
                int counter4 = row.getInt("counter4");
                int counter5 = row.getInt("counter5");

                return new CounterData(id, counter1, counter2, counter3, counter4, counter5);
            }).collect(Collectors.toList());
        }

        private String getCql()
        {
            return String.format("INSERT INTO %s.%s (id, counter1, counter2, counter3, counter4, counter5 ) VALUES (?, ?, ?, ?, ?, ?)", keyspaceName, tableName);
        }

        public TransactionState persistUsingTransaction(TransactionState transactionState, CounterData counterData, Session session) {
            String cql = String.format("INSERT INTO %s.%s (id, counter1, counter2, counter3, counter4, counter5 ) VALUES (?, ?, ?, ?, ?, ?) USING TRANSACTION %s", keyspaceName, tableName, transactionState.getTransactionId());
            ResultSet resultSet = executeCqlWithCounterData(counterData, session, cql);
            return transactionState.merge(MppTestingUtilities.mapResultToTransactionState(resultSet));
        }
    }
}
