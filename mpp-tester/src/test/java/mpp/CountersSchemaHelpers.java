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
import com.datastax.driver.core.Row;
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

    public abstract static class BaseCounterData<ID> {
        protected ID id;

        int counter1;
        int counter2;
        int counter3;
        int counter4;
        int counter5;

        public BaseCounterData(ID id, int counter1, int counter2, int counter3, int counter4, int counter5)
        {
            this.id = id;
            this.counter1 = counter1;
            this.counter2 = counter2;
            this.counter3 = counter3;
            this.counter4 = counter4;
            this.counter5 = counter5;
        }

        public ID getId()
        {
            return id;
        }


        public void setCounter1(int counter1)
        {
            this.counter1 = counter1;
        }

        public void setCounter2(int counter2)
        {
            this.counter2 = counter2;
        }

        public void setCounter3(int counter3)
        {
            this.counter3 = counter3;
        }

        public void setCounter4(int counter4)
        {
            this.counter4 = counter4;
        }

        public void setCounter5(int counter5)
        {
            this.counter5 = counter5;
        }
    }

    public static class CounterData extends BaseCounterData<UUID> {

        public CounterData(UUID id, int counter1, int counter2, int counter3, int counter4, int counter5)
        {
            super(id, counter1, counter2, counter3, counter4, counter5);

        }

        public static CounterData newUsingId(UUID counterId)
        {
            return new CounterData(counterId, 0, 0, 0, 0, 0);
        }

    }

    public static class NamedCounterData extends BaseCounterData<String> {

        public NamedCounterData(String s, int counter1, int counter2, int counter3, int counter4, int counter5)
        {
            super(s, counter1, counter2, counter3, counter4, counter5);
        }

        public static NamedCounterData newUsingName(String counterId)
        {
            return new NamedCounterData(counterId, 0, 0, 0, 0, 0);
        }
    }

    public abstract static class BaseCounterTable<CD extends BaseCounterData<ID>, ID> {
        String keyspaceName;
        String tableName;

        public BaseCounterTable(String keyspaceName, String tableName)
        {
            this.keyspaceName = keyspaceName;
            this.tableName = tableName;
        }

        public void persist(CD counterData, Session session) {
            String cql = getCql();
            executeCqlWithCounterData(counterData, session, cql);
        }

        private ResultSet executeCqlWithCounterData(CD counterData, Session session, String cql)
        {
            SimpleStatement simpleStatement = new SimpleStatement(cql, counterData.getId(),
                                                                  counterData.counter1,
                                                                  counterData.counter2,
                                                                  counterData.counter3,
                                                                  counterData.counter4,
                                                                  counterData.counter5);
            simpleStatement.setConsistencyLevel(ConsistencyLevel.ALL);
            return session.execute(simpleStatement);
        }

        private ResultSet executeCqlWithCounterColumn(CD counterData, Session session, String cql, String columnName)
        {
            // TODO [MPP] This is stupid right now
            final int counterDataToSelect;
            if ("counter1".equals(columnName))
            {
                counterDataToSelect = counterData.counter1;
            }
            else if ("counter2".equals(columnName))
            {
                counterDataToSelect = counterData.counter2;
            }
            else if ("counter3".equals(columnName))
            {
                counterDataToSelect = counterData.counter3;
            }
            else if ("counter4".equals(columnName))
            {
                counterDataToSelect = counterData.counter4;
            }
            else if ("counter5".equals(columnName))
            {
                counterDataToSelect = counterData.counter5;
            }
            else
            {
                throw new RuntimeException("BAAD column name");
            }

            SimpleStatement simpleStatement = new SimpleStatement(cql, counterData.getId(),
                                                                  counterDataToSelect);
            simpleStatement.setConsistencyLevel(ConsistencyLevel.ALL);
            return session.execute(simpleStatement);
        }

        abstract protected ID readId(Row row);

        abstract protected CD createCounterData(ID id, int counter1, int counter2, int counter3, int counter4, int counter5);

        public Optional<CD> findById(ID id, Session session) {
            String cqlQuery = String.format("SELECT * FROM %s.%s WHERE id = ?", keyspaceName, tableName);
            SimpleStatement simpleStatement = new SimpleStatement(cqlQuery, id);
            simpleStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
            ResultSet resultSet = session.execute(simpleStatement);
            return readRows(resultSet).stream().findFirst();
        }

        public List<CD> readRows(ResultSet resultSet)
        {
            return StreamSupport.stream(resultSet.spliterator(), false).map(row -> {
                ID id = readId(row);
                int counter1 = row.getInt("counter1");
                int counter2 = row.getInt("counter2");
                int counter3 = row.getInt("counter3");
                int counter4 = row.getInt("counter4");
                int counter5 = row.getInt("counter5");

                return createCounterData(id, counter1, counter2, counter3, counter4, counter5);
            }).collect(Collectors.toList());
        }

        private String getCql()
        {
            return String.format("INSERT INTO %s.%s (id, counter1, counter2, counter3, counter4, counter5 ) VALUES (?, ?, ?, ?, ?, ?)", keyspaceName, tableName);
        }

        public TransactionState persistUsingTransaction(TransactionState transactionState, CD counterData, Session session) {
            String cql = String.format("INSERT INTO %s.%s (id, counter1, counter2, counter3, counter4, counter5 ) VALUES (?, ?, ?, ?, ?, ?) USING TRANSACTION %s", keyspaceName, tableName, transactionState.getTransactionId());
            ResultSet resultSet = executeCqlWithCounterData(counterData, session, cql);
            return transactionState.merge(MppTestingUtilities.mapResultToTransactionState(resultSet));
        }

        public TransactionState updateCounterColumn(TransactionState transactionState, CD counterData, String counterColumn, Session session) {
            String cql = String.format("INSERT INTO %s.%s (id, %s) VALUES (?, ?) USING TRANSACTION %s", keyspaceName, tableName, counterColumn, transactionState.getTransactionId());
            ResultSet resultSet = executeCqlWithCounterColumn(counterData, session, cql, counterColumn);
            return transactionState.merge(MppTestingUtilities.mapResultToTransactionState(resultSet));
        }
    }

    public static class CounterTable extends BaseCounterTable<CounterData, UUID> {

        public CounterTable(String keyspaceName, String tableName)
        {
            super(keyspaceName, tableName);
        }

        protected UUID readId(Row row)
        {
            return row.getUUID("id");
        }

        protected CounterData createCounterData(UUID id, int counter1, int counter2, int counter3, int counter4, int counter5)
        {
            return new CounterData(id, counter1, counter2, counter3, counter4, counter5);
        }
    }

    public static class NamedCounterTable extends BaseCounterTable<NamedCounterData, String> {

        public NamedCounterTable(String keyspaceName, String tableName)
        {
            super(keyspaceName, tableName);
        }

        protected String readId(Row row)
        {
            return row.getString("id");
        }

        protected NamedCounterData createCounterData(String s, int counter1, int counter2, int counter3, int counter4, int counter5)
        {
            return new NamedCounterData(s, counter1, counter2, counter3, counter4, counter5);
        }

        public NamedCounterData createCounterData(String s) {
            return new NamedCounterData(s, 0, 0, 0, 0, 0);
        }
    }
}
