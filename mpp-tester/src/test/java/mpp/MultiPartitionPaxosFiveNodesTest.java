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

import java.util.Optional;

import org.junit.Test;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import junit.framework.Assert;
import org.apache.cassandra.mpp.transaction.client.TransactionState;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 07/04/16
 */
public class MultiPartitionPaxosFiveNodesTest extends FiveNodesClusterTest
{

    public static class CounterAndItsTable<ID, CD extends CountersSchemaHelpers.BaseCounterData<ID>, CT extends CountersSchemaHelpers.BaseCounterTable<CD, ID>> {
        private CD counter;
        private final CT table;

        public CounterAndItsTable(CD counter, CT table)
        {
            this.counter = counter;
            this.table = table;
        }

        public Optional<CD> findById(Session session) {
            return table.findById(counter.id, session);
        }

        public TransactionState persistUsingTransaction(TransactionState transactionState, Session session) {
            return table.persistUsingTransaction(transactionState, counter, session);
        }

        public void persist(Session session) {
            table.persist(counter, session);
        }

        public void refresh(Session session)
        {
            counter = findById(session).get();
        }
    }

    static class CounterTestData {
        CounterAndItsTable<?,?,?> counter1;
        CounterAndItsTable<?,?,?> counter2;
        CounterAndItsTable<?,?,?> counter3;

        public CounterTestData(CounterAndItsTable<?, ?, ?> counter1, CounterAndItsTable<?, ?, ?> counter2, CounterAndItsTable<?, ?, ?> counter3)
        {
            this.counter1 = counter1;
            this.counter2 = counter2;
            this.counter3 = counter3;
        }

        public void persist(Session session) {
            counter1.persist(session);
            counter2.persist(session);
            counter3.persist(session);
        }

        public TransactionState persistUsingTransaction(TransactionState transactionState, Session session)
        {
            transactionState = counter1.persistUsingTransaction(transactionState, session);
            transactionState = counter2.persistUsingTransaction(transactionState, session);
            transactionState = counter3.persistUsingTransaction(transactionState, session);
            return transactionState;
        }

        public void refresh(Session session) {
            this.counter1.refresh(session);
            this.counter2.refresh(session);
            this.counter3.refresh(session);
        }
    }

    @Test
    public void insertCounterData() {
        CountersSchemaHelpers.CounterTable countersCounter1 = MppCountersTestSchema.countersCounter1;
        CountersSchemaHelpers.CounterTable otherCounters = MppCountersTestSchema.otherCounters;

        CountersSchemaHelpers.CounterData counterData1 = createCounter1();
        CountersSchemaHelpers.CounterData counterData2 = createCounter2();
        CountersSchemaHelpers.NamedCounterData counterData3 = createCounter3();
        CounterTestData counterTestData = new CounterTestData(new CounterAndItsTable(counterData1, MppCountersTestSchema.countersCounter1), new CounterAndItsTable(counterData2, MppCountersTestSchema.otherCounters), new CounterAndItsTable(counterData3, MppCountersTestSchema.otherCountersNamed));

        TransactionState transactionState = beginTransaction(getSessionN1());

        transactionState = MppCountersTestSchema.countersCounter1.persistUsingTransaction(transactionState, counterData1, getSessionN1());
        transactionState = MppCountersTestSchema.otherCounters.persistUsingTransaction(transactionState, counterData2, getSessionN1());
        transactionState = MppCountersTestSchema.otherCountersNamed.persistUsingTransaction(transactionState, counterData3, getSessionN1());

        Integer replicaGroupsCount = getNodeProbe1().getMppProxy().countReplicaGroupsForTransaction(transactionStateToJson(transactionState));
        String info = String.format("Replica groups count is %s Counter1 ID %s Counter2 ID %s Counter3 ID %s", replicaGroupsCount.toString(), counterData1.id.toString(), counterData2.id.toString(), counterData3.id);

        System.out.println(info);
    }

    @Test
    public void testInsertData() {
        CountersSchemaHelpers.CounterData counterData1 = createCounter1();
        CountersSchemaHelpers.CounterData counterData2 = createCounter2();
        CountersSchemaHelpers.NamedCounterData counterData3 = createCounter3();
        CounterTestData counterTestData = new CounterTestData(new CounterAndItsTable(counterData1, MppCountersTestSchema.countersCounter1), new CounterAndItsTable(counterData2, MppCountersTestSchema.otherCounters), new CounterAndItsTable(counterData3, MppCountersTestSchema.otherCountersNamed));

        Session sessionN1 = getSessionN1();
        TransactionState transactionState = beginTransaction(sessionN1);
        transactionState = counterTestData.persistUsingTransaction(transactionState, sessionN1);

        commitTransaction(sessionN1, transactionState);

        counterTestData.refresh(sessionN1);

        Assert.assertEquals(1, counterTestData.counter1.counter.counter1);
        Assert.assertEquals(2, counterTestData.counter1.counter.counter2);
        Assert.assertEquals(3, counterTestData.counter1.counter.counter3);
        Assert.assertEquals(4, counterTestData.counter1.counter.counter4);
        Assert.assertEquals(5, counterTestData.counter1.counter.counter5);

        Assert.assertEquals(1, counterTestData.counter2.counter.counter1);
        Assert.assertEquals(1, counterTestData.counter2.counter.counter2);
        Assert.assertEquals(1, counterTestData.counter2.counter.counter3);
        Assert.assertEquals(1, counterTestData.counter2.counter.counter4);
        Assert.assertEquals(1, counterTestData.counter2.counter.counter5);

        Assert.assertEquals(1, counterTestData.counter3.counter.counter1);
        Assert.assertEquals(2, counterTestData.counter3.counter.counter2);
        Assert.assertEquals(1, counterTestData.counter3.counter.counter3);
        Assert.assertEquals(2, counterTestData.counter3.counter.counter4);
        Assert.assertEquals(4, counterTestData.counter3.counter.counter5);
    }

    private CountersSchemaHelpers.NamedCounterData createCounter3()
    {
        CountersSchemaHelpers.NamedCounterData counterData3 = CountersSchemaHelpers.NamedCounterData.newUsingName("this_is_very_long_name_for_counter");
        counterData3.setCounter1(1);
        counterData3.setCounter2(2);
        counterData3.setCounter3(1);
        counterData3.setCounter4(2);
        counterData3.setCounter5(1);
        return counterData3;
    }

    private CountersSchemaHelpers.CounterData createCounter2()
    {
        CountersSchemaHelpers.CounterData counterData2 = CountersSchemaHelpers.CounterData.newUsingId(UUIDs.random());
        counterData2.setCounter1(1);
        counterData2.setCounter2(1);
        counterData2.setCounter3(1);
        counterData2.setCounter4(1);
        counterData2.setCounter5(1);
        return counterData2;
    }

    private CountersSchemaHelpers.CounterData createCounter1()
    {
        CountersSchemaHelpers.CounterData counterData1 = CountersSchemaHelpers.CounterData.newUsingId(UUIDs.random());
        counterData1.setCounter1(1);
        counterData1.setCounter2(2);
        counterData1.setCounter3(3);
        counterData1.setCounter4(4);
        counterData1.setCounter5(5);
        return counterData1;
    }

}
