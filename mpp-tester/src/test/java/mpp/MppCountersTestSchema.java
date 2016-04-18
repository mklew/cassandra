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

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 06/04/16
 */
public class MppCountersTestSchema
{
    static String counters = "mpptest_counters";

    static Set<String> TABLES_WHICH_HAVE_UUID = new HashSet<>();

    static {
        TABLES_WHICH_HAVE_UUID.add("counter1");
        TABLES_WHICH_HAVE_UUID.add("counter2");
        TABLES_WHICH_HAVE_UUID.add("counter3");
        TABLES_WHICH_HAVE_UUID.add("counter4");
        TABLES_WHICH_HAVE_UUID.add("stop_after_proposed");
        TABLES_WHICH_HAVE_UUID.add("counter_one_for_all");
        TABLES_WHICH_HAVE_UUID.add("counter_two_slices");
    }

    static CountersSchemaHelpers.CounterTable countersCounter1 = new CountersSchemaHelpers.CounterTable(counters, "counter1");
    static CountersSchemaHelpers.CounterTable countersCounter2 = new CountersSchemaHelpers.CounterTable(counters, "counter2");
    static CountersSchemaHelpers.CounterTable countersCounter3 = new CountersSchemaHelpers.CounterTable(counters, "counter3");
    static CountersSchemaHelpers.CounterTable countersCounter4 = new CountersSchemaHelpers.CounterTable(counters, "counter4");
    static CountersSchemaHelpers.CounterTable stopAfterProposedTable = new CountersSchemaHelpers.CounterTable(counters, "stop_after_proposed");
    static CountersSchemaHelpers.NamedCounterTable countersNamedCounter1 = new CountersSchemaHelpers.NamedCounterTable(counters, "named_counters_counter1");
    static CountersSchemaHelpers.NamedCounterTable countersNamedCounter2 = new CountersSchemaHelpers.NamedCounterTable(counters, "named_counters_counter2");
//    static CountersSchemaHelpers.CounterTable countersCounter5 = new CountersSchemaHelpers.CounterTable(counters, "counter5");

    static String counterKeyspace2 = "mpptest_counters_other";

    static String boundsKeyspace = "mpptest_counters_bounds";

    static CountersSchemaHelpers.CounterTable otherCounters = new CountersSchemaHelpers.CounterTable(counterKeyspace2, "counter1");
    static CountersSchemaHelpers.NamedCounterTable otherCountersNamed = new CountersSchemaHelpers.NamedCounterTable(counterKeyspace2, "named_counter1");

    static CountersSchemaHelpers.CounterTable counterOneForAll = new CountersSchemaHelpers.CounterTable(boundsKeyspace, "counter_one_for_all");
    static CountersSchemaHelpers.CounterTable counterTwoSlices = new CountersSchemaHelpers.CounterTable(boundsKeyspace, "counter_two_slices");


    public static CountersSchemaHelpers.CounterData inititateCounterData(CountersSchemaHelpers.CounterTable counterTable, UUID counterId, Session session) {

        CountersSchemaHelpers.CounterData data = CountersSchemaHelpers.CounterData.newUsingId(counterId);
        counterTable.persist(data, session, ConsistencyLevel.ALL);
        return counterTable.findById(counterId, session).get();
    }
}
