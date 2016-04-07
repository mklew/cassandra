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

import org.junit.Test;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.mpp.transaction.client.TransactionState;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 07/04/16
 */
public class MultiPartitionPaxosFiveNodesTest extends FiveNodesClusterTest
{

    @Test
    public void insertCounterData() {
        CountersSchemaHelpers.CounterTable countersCounter1 = MppCountersTestSchema.countersCounter1;
        CountersSchemaHelpers.CounterTable otherCounters = MppCountersTestSchema.otherCounters;

        CountersSchemaHelpers.CounterData counterData1 = CountersSchemaHelpers.CounterData.newUsingId(UUIDs.random());
        counterData1.setCounter1(1);
        counterData1.setCounter2(2);
        counterData1.setCounter3(3);
        counterData1.setCounter4(4);
        counterData1.setCounter5(5);

        CountersSchemaHelpers.CounterData counterData2 = CountersSchemaHelpers.CounterData.newUsingId(UUIDs.random());
        counterData2.setCounter1(1);
        counterData2.setCounter2(1);
        counterData2.setCounter3(1);
        counterData2.setCounter4(1);
        counterData2.setCounter5(1);

        CountersSchemaHelpers.NamedCounterData counterData3 = CountersSchemaHelpers.NamedCounterData.newUsingName("this_is_very_long_name_for_counter");
        counterData3.setCounter1(1);
        counterData3.setCounter2(2);
        counterData3.setCounter3(1);
        counterData3.setCounter4(2);
        counterData3.setCounter5(1);

        TransactionState transactionState = beginTransaction(getSessionN1());

        transactionState = countersCounter1.persistUsingTransaction(transactionState, counterData1, getSessionN1());
        transactionState = otherCounters.persistUsingTransaction(transactionState, counterData2, getSessionN1());
        transactionState = MppCountersTestSchema.otherCountersNamed.persistUsingTransaction(transactionState, counterData3, getSessionN1());

        Integer replicaGroupsCount = getNodeProbe1().getMppProxy().countReplicaGroupsForTransaction(transactionStateToJson(transactionState));
        String info = String.format("Replica groups count is %s Counter1 ID %s Counter2 ID %s Counter3 ID %s", replicaGroupsCount.toString(), counterData1.id.toString(), counterData2.id.toString(), counterData3.id);

        System.out.println(info);
    }


//    @Test
//    public void shouldFindThatIdsThatBelongToDifferentReplicaSets() {
//
//    }
}
