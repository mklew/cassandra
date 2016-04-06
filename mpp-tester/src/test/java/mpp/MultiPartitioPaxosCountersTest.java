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

import java.util.UUID;

import org.junit.Test;

import com.datastax.driver.core.Session;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 06/04/16
 */
public class MultiPartitioPaxosCountersTest extends BaseClusterTest
{

    abstract class BaseCounterIncrementer {

        abstract void incrementCounter(CountersSchemaHelpers.CounterData counterData);
    }

    interface CounterIncrementer {
        void incrementCounter(CountersSchemaHelpers.CounterData counterData);
    }

    static class Counter1Incrementer implements CounterIncrementer {
        public void incrementCounter(CountersSchemaHelpers.CounterData counterData)
        {
            counterData.counter1 += 1;
        }
    }

    @Test
    public void shouldProperlyIncrementCounters() {
        UUID c1ID_1 = UUID.randomUUID();
        UUID c1ID_2 = UUID.randomUUID();
        UUID c2ID_1 = UUID.randomUUID();
        UUID c3ID_1 = UUID.randomUUID();
        UUID c4ID_1 = UUID.randomUUID();
        UUID coID_1 = UUID.randomUUID();

        Session sessionN1 = getSessionN1();

        CountersSchemaHelpers.CounterData counter1 = MppCountersTestSchema.inititateCounterData(MppCountersTestSchema.countersCounter1, c1ID_1, sessionN1);
        CountersSchemaHelpers.CounterData counter2 = MppCountersTestSchema.inititateCounterData(MppCountersTestSchema.countersCounter1, c1ID_2, sessionN1);
        CountersSchemaHelpers.CounterData counter3 = MppCountersTestSchema.inititateCounterData(MppCountersTestSchema.countersCounter2, c2ID_1, sessionN1);
        CountersSchemaHelpers.CounterData counter4 = MppCountersTestSchema.inititateCounterData(MppCountersTestSchema.countersCounter3, c3ID_1, sessionN1);
        CountersSchemaHelpers.CounterData counter5 = MppCountersTestSchema.inititateCounterData(MppCountersTestSchema.countersCounter4, c4ID_1, sessionN1);
        CountersSchemaHelpers.CounterData counter6 = MppCountersTestSchema.inititateCounterData(MppCountersTestSchema.otherCounters, coID_1, sessionN1);
    }

    @Test
    public void shouldIncrementCounterByOneTransactionOrTheOther() {
        UUID c1ID_1 = UUID.randomUUID();
        Session sessionN1 = getSessionN1();
        CountersSchemaHelpers.CounterData counter1 = MppCountersTestSchema.inititateCounterData(MppCountersTestSchema.countersCounter1, c1ID_1, sessionN1);

        // 1. Create two transactions.

        // 2. Each of them, reads counter with ID c1ID_1

        // 3. One increments column counter1

        // 4. Second increments column counter2
    }
}
