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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.utils.UUIDs;
import junit.framework.Assert;
import org.apache.cassandra.mpp.transaction.TransactionId;
import org.apache.cassandra.mpp.transaction.TransactionTimeUUID;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.utils.Pair;

import static java.util.stream.Collectors.toList;

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

        public void persist(Session session, ConsistencyLevel consistencyLevel) {
            table.persist(counter, session, consistencyLevel);
        }

        public void persist(Session session) {
            table.persist(counter, session, ConsistencyLevel.ALL);
        }

        public void refresh(Session session)
        {
            counter = findById(session).get();
        }

        public CounterAndItsTable<ID, CD, CT> copy(Session session) {
            return new CounterAndItsTable<>(findById(session).get(), table);
        }

        public TransactionState persistUsingTransactionOnlyColumn(TransactionState transactionState, Session session, String columnName)
        {
            return table.updateCounterColumn(transactionState, counter, columnName, session);
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

    private static class KeyspaceAndTable {
        private final String keyspace;

        private final String table;


        public KeyspaceAndTable(String keyspace, String table)
        {
            this.keyspace = keyspace;
            this.table = table;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            KeyspaceAndTable that = (KeyspaceAndTable) o;

            if (!keyspace.equals(that.keyspace)) return false;
            if (!table.equals(that.table)) return false;

            return true;
        }

        public int hashCode()
        {
            int result = keyspace.hashCode();
            result = 31 * result + table.hashCode();
            return result;
        }
    }

    private static class CounterKey {
        private final KeyspaceAndTable ksAndTable;
        private final String id;

        private CounterKey(KeyspaceAndTable ksAndTable, String id)
        {
            this.ksAndTable = ksAndTable;
            this.id = id;
        }

        static CounterKey fromResult(CounterExpectedResult result) {
            KeyspaceAndTable keyspaceAndTable = new KeyspaceAndTable(result.keyspace, result.table);
            return new CounterKey(keyspaceAndTable, result.counterId);
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CounterKey that = (CounterKey) o;

            if (!id.equals(that.id)) return false;
            if (!ksAndTable.equals(that.ksAndTable)) return false;

            return true;
        }

        public int hashCode()
        {
            int result = ksAndTable.hashCode();
            result = 31 * result + id.hashCode();
            return result;
        }
    }

    private static class CounterExpectedResult {

        private final String keyspace;

        private final String table;

        private final String counterId;

        private final Map<String, Integer> counterColumnToExpectedCount;

        private CounterExpectedResult(String keyspace, String table, String counterId, String counterColumn, int expectedCount)
        {
            this.keyspace = keyspace;
            this.table = table;
            this.counterId = counterId;
            this.counterColumnToExpectedCount = new HashMap<>();
            counterColumnToExpectedCount.put(counterColumn, expectedCount);
        }

        private CounterExpectedResult(String keyspace, String table, String counterId, Map<String, Integer> counterColumnToExpectedCount)
        {
            this.keyspace = keyspace;
            this.table = table;
            this.counterId = counterId;
            this.counterColumnToExpectedCount = counterColumnToExpectedCount;
        }

        public static CounterExpectedResult merge(CounterExpectedResult r1, CounterExpectedResult r2) {
//            System.out.println("Merge r1 " + r1 + " with r2 " + r2);
            Preconditions.checkArgument(r1.keyspace.equals(r2.keyspace));
            Preconditions.checkArgument(r1.table.equals(r2.table));
            Preconditions.checkArgument(r1.counterId.equals(r2.counterId));

            HashMap<String, Integer> copyOfR1 = new HashMap<>(r1.counterColumnToExpectedCount);

            r2.counterColumnToExpectedCount.forEach((counterCoulumn, expectedCount) -> {
                copyOfR1.merge(counterCoulumn, expectedCount, (previousExpected, newExpected) -> previousExpected + newExpected);
            });

            CounterExpectedResult afterMerge = new CounterExpectedResult(r1.keyspace, r1.table, r1.counterId, copyOfR1);
//            System.out.println("After merge " + afterMerge);
            return afterMerge;
        }

        public String toString()
        {
            return "CounterExpectedResult{" +
                   "keyspace='" + keyspace + '\'' +
                   ", table='" + table + '\'' +
                   ", counterId='" + counterId + '\'' +
                   ", counterColumnToExpectedCount=" + counterColumnToExpectedCount +
                   '}';
        }
    }

    private static class CounterExecutorResults {
        private final String resultsFromExecutorName;

        private final List<CounterExpectedResult> expectedCounts;

        private final List<TransactionId> committed;

        private final List<TransactionId> rolledBack;

        private CounterExecutorResults(String resultsFromExecutorName, List<CounterExpectedResult> expectedCounts,
                                       List<TransactionId> committed, List<TransactionId> rolledBack)
        {
            this.resultsFromExecutorName = resultsFromExecutorName;
            this.expectedCounts = expectedCounts;
            this.committed = committed;
            this.rolledBack = rolledBack;
        }
    }

    /**
     * Increment by one
     */
    private static class IncrementOf {
        private final String keyspace;

        private final String table;

        private final String counterId;

        private final String column;

        private IncrementOf(String keyspaceName, String tableName, String counterId, String column)
        {
            this.keyspace = keyspaceName;
            this.table = tableName;
            this.counterId = counterId;
            this.column = column;
        }
    }

    private static class IterationResult {
        final int iterationNumber;
        final boolean successfullyCommitted;
        final List<IncrementOf> incrementOfs;
        final TransactionId txId;

        private IterationResult(int iterationNumber, TransactionId id, boolean successfullyCommitted, List<IncrementOf> incrementOfs)
        {
            this.iterationNumber = iterationNumber;
            this.txId = id;
            this.successfullyCommitted = successfullyCommitted;
            this.incrementOfs = incrementOfs;
        }

        public TransactionId getTxId()
        {
            return txId;
        }
    }

    private static class IterationExpectations {
        final List<IncrementOf> incrementOfs;

        private IterationExpectations(List<IncrementOf> incrementOfs)
        {
            this.incrementOfs = incrementOfs;
        }
    }

    /**
     * keyspace: mpptest_counters
     * cf: "named_counters_counter1"
     */
    static final String ks1NamedCounter1_1 = "i like pancakes";

    /**
     * keyspace: mpptest_counters
     * cf: "named_counters_counter1"
     */
    static final String ks1NamedCounter1_2 = "awesome counter";

    /**
     * keyspace: mpptest_counters
     * cf: "named_counters_counter2"
     */
    static final String ks1NamedCounter2_1 = "qwerty";

    /**
     * keyspace: mpptest_counters_other
     * cf: "named_counter1"
     */
    static final String ks2NamedCounter1_1 = "other counter 1";

    /**
     * keyspace: mpptest_counters_other
     * cf: "named_counter1"
     */
    static final String ks2NamedCounter1_2 = "other counter 2";

    public static Collection<CounterAndItsTable> createSampleOfNamedCounters() {
        String postfix = String.valueOf(System.currentTimeMillis());
        CountersSchemaHelpers.NamedCounterData counterData = MppCountersTestSchema.countersNamedCounter1.createCounterData(ks1NamedCounter1_1 + postfix);
        CountersSchemaHelpers.NamedCounterData counterData2 = MppCountersTestSchema.countersNamedCounter1.createCounterData(ks1NamedCounter1_2 + postfix);
        CountersSchemaHelpers.NamedCounterData counterData3 = MppCountersTestSchema.countersNamedCounter2.createCounterData(ks1NamedCounter2_1 + postfix);
        CountersSchemaHelpers.NamedCounterData counterData4 = MppCountersTestSchema.otherCountersNamed.createCounterData(ks2NamedCounter1_1 + postfix);
        CountersSchemaHelpers.NamedCounterData counterData5 = MppCountersTestSchema.otherCountersNamed.createCounterData(ks2NamedCounter1_2 + postfix);

        CounterAndItsTable ks1NamedCounter1_1C = new CounterAndItsTable(counterData, MppCountersTestSchema.countersNamedCounter1);
        CounterAndItsTable ks1NamedCounter1_2C = new CounterAndItsTable(counterData2, MppCountersTestSchema.countersNamedCounter1);
        CounterAndItsTable ks1NamedCounter2_1C = new CounterAndItsTable(counterData3, MppCountersTestSchema.countersNamedCounter2);
        CounterAndItsTable ks2NamedCounter1_1C = new CounterAndItsTable(counterData4, MppCountersTestSchema.otherCountersNamed);
        CounterAndItsTable ks2NamedCounter1_2C = new CounterAndItsTable(counterData5, MppCountersTestSchema.otherCountersNamed);

        return Arrays.asList(ks1NamedCounter1_1C, ks1NamedCounter1_2C, ks1NamedCounter2_1C, ks2NamedCounter1_1C, ks2NamedCounter1_2C);
    }

    public List<CounterColumnIncrementerExecutor> createCounterExecutors(int iterations, Collection<CounterAndItsTable> countersThatExist) {
        CounterColumnIncrementerExecutor counter1Executor = new CounterColumnIncrementerExecutor(iterations, "Counter1Exe", countersThatExist, "counter1");
        CounterColumnIncrementerExecutor counter2Executor = new CounterColumnIncrementerExecutor(iterations, "Counter2Exe", countersThatExist, "counter2");
        CounterColumnIncrementerExecutor counter3Executor = new CounterColumnIncrementerExecutor(iterations, "Counter3Exe", countersThatExist, "counter3");
        CounterColumnIncrementerExecutor counter4Executor = new CounterColumnIncrementerExecutor(iterations, "Counter4Exe", countersThatExist, "counter4");
        CounterColumnIncrementerExecutor counter5Executor = new CounterColumnIncrementerExecutor(iterations, "Counter5Exe", countersThatExist, "counter5");

        // TODO [MPP] Returning only single executor to test if test works just as single transaction inserting some other data.
//        return Arrays.asList(counter1Executor, counter2Executor, counter3Executor, counter4Executor, counter5Executor);
        return Arrays.asList(counter1Executor, counter2Executor, counter4Executor, counter5Executor);
//        return Arrays.asList(counter1Executor, counter2Executor);
//        return Arrays.asList(counter1Executor);
    }

    private static <T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> futures) {
        CompletableFuture<Void> allDoneFuture =
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
        return allDoneFuture.thenApply(v ->
                                       futures.stream().
                                                       map(future -> future.join()).
                                                                                   collect(Collectors.<T>toList())
        );
    }


    public static boolean areTransactionListsEqual(List<TransactionId> l1, List<TransactionId> l2) {
        List<String> l1strs = l1.stream().map(txId -> txId.toString()).sorted().collect(toList());
        List<String> l2strs = l2.stream().map(txId -> txId.toString()).sorted().collect(toList());
        return l1strs.equals(l2strs);
    }

    /**
     *  @return transactions that are in l1, but not in l2
     */
    public static List<TransactionId> findDifference(List<TransactionId> l1, List<TransactionId> l2) {
        return l1.stream().filter(tx -> !l2.contains(tx)).collect(toList());
    }

    @Test
    public void runTestUsingCounters() throws Throwable {
        boolean checkForConverganceOfCommitsAndRollbacks = false;
        int iterations = 10;
        // There counters exist from previous test because they use named keys. Need to reset them
        Collection<CounterAndItsTable> countersToPersist = createSampleOfNamedCounters();
        Session anySession = getAnySession();
        // reset counters & refresh counters
        Collection<CounterAndItsTable> counters = persistInitialCounterValues(anySession, countersToPersist);

        // TODO [MPP] Modify number of counter executors
        List<CounterColumnIncrementerExecutor> counterExecutors = createCounterExecutors(iterations, counters);

        ExecutorService executorService = Executors.newFixedThreadPool(counterExecutors.size());

        // Prepare all counter executors
        counterExecutors.forEach(counterExecutor -> counterExecutor.prepare());

        // Collect futures of results.
        List<CompletableFuture<CounterExecutorResults>> futureResults = counterExecutors.stream().map(counterExecutor -> counterExecutor.getCounterExecutorResultsFuture()).collect(toList());

        CompletableFuture<List<CounterExecutorResults>> allResults = sequence(futureResults);

        // Acutal execution on seperate thead pool
        counterExecutors.forEach(counterExecutor -> executorService.execute(counterExecutor));

        CountDownLatch latch = new CountDownLatch(1);

        allResults.thenAccept(results -> {
            displaySummaryOfCommits();

            List<ReplicaTransactionsSummary> summaries = getSummaryOfTransactionsPerReplica();

            List<TransactionId> allTransactionsSeenByReplicas = getAllTransactionIdsSeenByReplicas(summaries);
            List<TransactionId> beganTransactions = this.beganTransactions.stream().collect(toList());
            if(!areTransactionListsEqual(allTransactionsSeenByReplicas, beganTransactions)) {
                System.out.println("ERROR ! ! ! Replicas have seen different number of transactions than began ones. Began transactions count " + beganTransactions.size() + ", but replicas have seen " + allTransactionsSeenByReplicas.size() + " transactions");
                List<TransactionId> difference = findDifference(allTransactionsSeenByReplicas, beganTransactions);
                System.out.println("Replicas have seen extra transactions: " + difference);
            }

            System.out.println("before check that replicas agree");
            checkThatReplicasAgreeAboutCommittedAndRolledBackTransactions(summaries);
            // From results of executors
            System.out.println("before committed transaction ids from results");
            List<TransactionId> committedTransactions = getCommittedTransactionIdsFromResults(results);
            System.out.println("before get rolled back transactio ids from results");
            List<TransactionId> rolledBackTransactions = getRolledBackTransactionIdsFromResults(results);

            // Actual recorded results from replicas. It assumes that results are converged
            if(checkForConverganceOfCommitsAndRollbacks) {
                List<TransactionId> actuallyCommitted = getCommittedTransactionsAccordingToReplicas(summaries);
                List<TransactionId> actuallyRolledBack = getRolledBackTransactionsAccordingToReplicas(summaries);

                if(!areTransactionListsEqual(actuallyCommitted, committedTransactions)) {
                    System.out.println("ERROR ! ! ! Executors do not have same results as actual results about committed transactions");

                    List<TransactionId> transactionsCommittedButNotSeenByExecutors = findDifference(actuallyCommitted, committedTransactions);
                    System.out.println("Transactions committed but not seen by executors: " + transactionsCommittedButNotSeenByExecutors);
                }
                else {
                    System.out.println("Executors and replicas agree on committed transactions");
                }

                if(!areTransactionListsEqual(actuallyRolledBack, rolledBackTransactions)) {
                    System.out.println("ERROR ! ! ! Executors do not have same results as actual results about rolled back transactions");

                    List<TransactionId> transactionsRolledBackButNotSeenByExecutors = findDifference(actuallyRolledBack, rolledBackTransactions);
                    System.out.println("Transactions rolled back but not seen by executors: " + transactionsRolledBackButNotSeenByExecutors);
                }
                else {
                    System.out.println("Executors and replicas agree on rolledback transactions");
                }
            }
            latch.countDown();
        }).get();

        allResults.thenAccept(results -> {
            try
            {
                latch.await();

                System.out.println("Counters after operations");
                counters.stream().map(counter -> {
                    counter.refresh(anySession);

                    return counter.counter.toString();
                }).forEach(System.out::println);

                results.forEach(expectedResult -> {
                    System.out.println("Checking expected results by executor: " + expectedResult.resultsFromExecutorName);

                    expectedResult.expectedCounts.forEach(expectedCounterCount -> {

                        expectedCounterCount.counterColumnToExpectedCount.entrySet().forEach(kv -> {
                            String columnName = kv.getKey();
                            Integer expectedCounterValue = kv.getValue();

                            String cql = String.format("SELECT %s FROM %s.%s WHERE id = ?", columnName,
                                                       expectedCounterCount.keyspace,
                                                       expectedCounterCount.table);

                            SimpleStatement statement = new SimpleStatement(cql, expectedCounterCount.counterId);
                            statement.setConsistencyLevel(ConsistencyLevel.ALL);

                            ResultSet counterQueryResult = anySession.execute(statement);
                            int actualCounterValue = counterQueryResult.one().getInt(columnName);

                            String msg = String.format("Expecting counter with ID %s from table %s.%s to have counter column %s with count=%s, actual count=%s",
                                                       expectedCounterCount.counterId,
                                                       expectedCounterCount.keyspace,
                                                       expectedCounterCount.table,
                                                       columnName,
                                                       expectedCounterValue.toString(),
                                                       String.valueOf(actualCounterValue));

                            System.out.println(msg);
                            Assert.assertEquals(msg, expectedCounterValue, Integer.valueOf(actualCounterValue));
                        });
                    });
                });
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }).get();

        executorService.awaitTermination(Math.max(5,(int)(iterations * 1.5)), TimeUnit.SECONDS);
        anySession.close();
    }

    private List<TransactionId> getCommittedTransactionIdsFromResults(List<CounterExecutorResults> results)
    {
        return results.stream().flatMap(r -> r.committed.stream()).distinct().collect(toList());
    }

    private List<TransactionId> getRolledBackTransactionIdsFromResults(List<CounterExecutorResults> results)
    {
        return results.stream().flatMap(r -> r.rolledBack.stream()).distinct().collect(toList());
    }

    public static Collection<CounterAndItsTable> persistInitialCounterValues(Session session, Collection<CounterAndItsTable> counters) {
        counters.forEach(counterAndTable -> {
            counterAndTable.persist(session);
        });

        counters.forEach(counterAndTable -> {
            counterAndTable.refresh(session);
        });

        return counters;
    }

    /**
     * Executor modifies Counters.
     *
     * Counter has ID and counter1 .. counterN int columns.
     *
     * If Executors modify different counter columns then they will not override their increments.
     * There can be only 1 executor of given type, otherwise they would override and increment results would lie.
     *
     * So right now I am limited to only 5 executors because I created 5 columns, but I could create table
     * with 100 counters and have generic enough solution to modify only concrete tables.
     *
     * Executors should modify counters with same ids.
     *
     * Therefore I can have 5 executor types.
     *
     * 1 that modifies Counter.counter1 column
     * 2 that modifies Counter.counter2 column
     * and so on.
     *
     * Or other disjoint executors such as
     *
     * Type 1 modifies Counter.{counter1, counter2, counter3}
     * Type 2 modifies Counter.{counter4, counter5}
     *
     */
    private abstract class CounterExecutor implements Runnable {
        private final int iterations;

        /**
         * Type of executor, which counter column in counter row it modifies.
         */
        private final String name;

        private final CompletableFuture<CounterExecutorResults> resultsF;

        private final List<IterationResult> iterationResults;

        protected Collection<CounterAndItsTable> counters;

        private CounterExecutor(int iterations, String name, Collection<CounterAndItsTable> countersThatExist)
        {
            this.iterations = iterations;
            this.name = name;
            resultsF = new CompletableFuture<>();
            iterationResults = new ArrayList<>(iterations);
            this.counters = countersThatExist;
        }

        public void prepare() {
            Session session = getAnySession();
            this.counters = counters.stream().map(counter -> counter.copy(session)).collect(toList());
            session.close();
        }

        public CompletableFuture<CounterExecutorResults> getCounterExecutorResultsFuture()
        {
            return resultsF;
        }

        private CounterExecutorResults computeResults() {
            List<CounterExpectedResult> counterExpectedResults = iterationResults
             .stream()
             .flatMap(iterationResult ->
                      iterationResult.incrementOfs.stream()
                                                  .map(incrementOf -> new CounterExpectedResult(incrementOf.keyspace,
                                                                                                incrementOf.table,
                                                                                                incrementOf.counterId,
                                                                                                incrementOf.column, 1))
             ).collect(Collectors.groupingBy(CounterKey::fromResult, Collectors.reducing(CounterExpectedResult::merge))).values().stream().map(Optional::get).collect(toList());

            List<TransactionId> successfullyCommittedTransactionsAccordingToExecutor = iterationResults.stream().filter(ir -> ir.successfullyCommitted).map(IterationResult::getTxId).collect(toList());
            List<TransactionId> rolledBackTransactionsAccordingToExecutor = iterationResults.stream().filter(ir -> !ir.successfullyCommitted).map(IterationResult::getTxId).collect(toList());


            return new CounterExecutorResults(name, counterExpectedResults, successfullyCommittedTransactionsAccordingToExecutor, rolledBackTransactionsAccordingToExecutor);
        }

        protected Collection<CounterAndItsTable> getCounters()
        {
            return counters;
        }

        public void run()
        {
            int currentIteration = 1;
            while(currentIteration <= iterations) {
                runIteration(currentIteration);
                currentIteration++;
            }
            CounterExecutorResults computedResults = computeResults();
            resultsF.complete(computedResults);
        }

        protected abstract IterationResult toIterationResult(TransactionState transactionState, IterationExpectations expectations, boolean successfullyCommitted, Session session, int iteration);

        protected abstract Pair<IterationExpectations, TransactionState> doInTransaction(TransactionState transactionState, Session session);

        protected abstract boolean checkIfItReallyWasCommitted(TransactionState transactionState, Session session);

        private void runIteration(int iteration)
        {
            Session session = getAnySession();
            getCounters().forEach(counter -> counter.refresh(session));

            TransactionState transactionState = beginTransaction(session);
            UUID transactionId = transactionState.getTransactionId();

            System.out.println("Transaction " + transactionId + " runs");

            Pair<IterationExpectations, TransactionState> expectationsAndState = doInTransaction(transactionState, session);
            transactionState = expectationsAndState.right;
            Boolean committed = null;

            try
            {
                ResultSet resultSet = commitTransaction(session, transactionState);

                Row one = resultSet.one();
                UUID txId = one.getUUID("[tx_id]");
                Preconditions.checkState(txId.equals(transactionState.getTransactionId()));
                committed = one.getBool("[committed]");

                if (committed) {
                    System.out.println("Transaction with ID " + txId + " was committed");
                }
                else {
                    System.out.println("Transaction with ID " + txId + " was rolled back");
                }
            }
            catch (Exception e)
            {
                System.err.println("Exception occurred during commit of transaction" + e);
                committed = false;
            }

            // Ignoring result of commitTransaction and checking it with query.
            // TODO this should only happen when timeout occurred, if there is no timeout then result of commitTransaction should be valid

            boolean reallyCommitted = checkIfItReallyWasCommitted(transactionState, session);

            if(committed != reallyCommitted)
            {
                // log it
                System.out.println("Wrong commit result. Transaction with ID " + transactionState.getTransactionId() + " returned with committed=" + committed + " but in reality after checking with query it is committed=" + reallyCommitted);
            }
            committed = reallyCommitted;

            // TODO [MPP] Check in JMX whether this transaction was really committed or not.

            if(committed)  {
                IterationResult iterationResult = toIterationResult(transactionState, expectationsAndState.left, true, session, iteration);
                iterationResults.add(iterationResult);
            }
            else {
                IterationResult iterationResult = toIterationResult(transactionState, expectationsAndState.left, false, session, iteration);
                iterationResults.add(iterationResult);
            }

            session.close();
        }
    }

    private class CounterColumnIncrementerExecutor extends CounterExecutor {

        private final String columnName;

        private int currentCount = 0;

        private int expectedCount = 0;

        private CounterColumnIncrementerExecutor(int iterations, String name, Collection<CounterAndItsTable> countersThatExist, String columnName)
        {
            super(iterations, name, countersThatExist);
            this.columnName = columnName;
        }

        protected IterationResult toIterationResult(TransactionState transactionState, IterationExpectations expectations, boolean successfullyCommitted, Session session, int iteration)
        {
            if(successfullyCommitted) {
                return new IterationResult(iteration, transactionState.id(), successfullyCommitted, expectations.incrementOfs);
            }
            else {
                return new IterationResult(iteration, transactionState.id(), successfullyCommitted, Collections.emptyList());
            }
        }

        protected Pair<IterationExpectations, TransactionState> doInTransaction(TransactionState transactionState, Session session)
        {
            // For each counter increment counter 1 table
            expectedCount = currentCount + 1;
            List<IncrementOf> expectedIncrements = getCounters().stream().map(counterAndTable -> {
                // TODO [MPP] Stupid version first.
                if ("counter1".equals(columnName))
                {
                    counterAndTable.counter.counter1 = expectedCount;
                }
                else if ("counter2".equals(columnName))
                {
                    counterAndTable.counter.counter2 = expectedCount;
                }
                else if ("counter3".equals(columnName))
                {
                    counterAndTable.counter.counter3 = expectedCount;
                }
                else if ("counter4".equals(columnName))
                {
                    counterAndTable.counter.counter4 = expectedCount;
                }
                else if ("counter5".equals(columnName))
                {
                    counterAndTable.counter.counter5 = expectedCount;
                }
                else
                {
                    throw new RuntimeException("BAAD column name");
                }
                return new IncrementOf(counterAndTable.table.keyspaceName, counterAndTable.table.tableName, counterAndTable.counter.id.toString(), columnName);
            }).collect(toList());

            // Persist changes using transaction
            TransactionState changedTransactionState = getCounters().stream().map(counterAndTable -> {
                return counterAndTable.persistUsingTransactionOnlyColumn(transactionState, session, columnName);
            }).reduce(TransactionState::merge).get();

            IterationExpectations iterationExpectations = new IterationExpectations(expectedIncrements);

            return Pair.create(iterationExpectations, changedTransactionState);
        }

        protected boolean checkIfItReallyWasCommitted(TransactionState transactionState, Session session)
        {
            // if we do query on counters and their counter matches expected counter, then transaction was committed.

            // We can also check that all counters are in sync, because there is a single counter executor that increments single counter column
            Set<Integer> setOfCounterValues = getCounters().stream().map(counterAndItsTable -> {
                SimpleStatement selectCounterById = new SimpleStatement(String.format("SELECT %s FROM %s.%s WHERE id = ?", columnName, counterAndItsTable.table.keyspaceName,
                                                                                      counterAndItsTable.table.tableName), counterAndItsTable.counter.id);
                selectCounterById.setConsistencyLevel(ConsistencyLevel.LOCAL_TRANSACTIONAL);
                ResultSet resultSet = session.execute(selectCounterById);
                int currentCount = resultSet.one().getInt(columnName);
                setCounterValue(counterAndItsTable, currentCount);
                if (currentCount != expectedCount)
                {
                    String msg = String.format("Current count of counter column %s with ID %s from %s.%s is %s and exepcted count is %s",
                                                  columnName, counterAndItsTable.counter.id, counterAndItsTable.table.keyspaceName,
                                                  counterAndItsTable.table.tableName, String.valueOf(currentCount), String.valueOf(expectedCount));
                    System.out.println(msg);
                }
                return currentCount;
            }).collect(Collectors.toSet());

            Preconditions.checkState(setOfCounterValues.size() == 1, "Transaction " + transactionState.getTransactionId() + " run by executor which increments column " + columnName
                                                                     + " has partially committed results. Results are " + setOfCounterValues);

            currentCount = setOfCounterValues.iterator().next();

            return getCounters().stream().allMatch(counter -> {
                int currentCounterColumnValue = getCounterValue(counter);

                return currentCounterColumnValue == expectedCount;
            });
        }

        void setCounterValue(CounterAndItsTable<?,?,?> counterAndTable, int currentCount) {
            if ("counter1".equals(columnName))
            {
                counterAndTable.counter.counter1 = currentCount;
            }
            else if ("counter2".equals(columnName))
            {
                counterAndTable.counter.counter2 = currentCount;
            }
            else if ("counter3".equals(columnName))
            {
                counterAndTable.counter.counter3 = currentCount;
            }
            else if ("counter4".equals(columnName))
            {
                counterAndTable.counter.counter4 = currentCount;
            }
            else if ("counter5".equals(columnName))
            {
                counterAndTable.counter.counter5 = currentCount;
            }
            else
            {
                throw new RuntimeException("BAAD column name");
            }
        }

        int getCounterValue(CounterAndItsTable<?,?,?> counterAndTable) {
            if ("counter1".equals(columnName))
            {
                return counterAndTable.counter.counter1;
            }
            else if ("counter2".equals(columnName))
            {
                return counterAndTable.counter.counter2;
            }
            else if ("counter3".equals(columnName))
            {
                return counterAndTable.counter.counter3;
            }
            else if ("counter4".equals(columnName))
            {
                return counterAndTable.counter.counter4;
            }
            else if ("counter5".equals(columnName))
            {
                return counterAndTable.counter.counter5;
            }
            else
            {
                throw new RuntimeException("BAAD column name");
            }
        }
    }

    @Before
    public void clearListsOfCommittedAndRolledBack() {
        getNodeProbesStream().forEach(nodeProbe -> nodeProbe.getMppProxy().clearLists());
    }

    private static class ReplicaTransactionsSummary {
        private final String replicaName;

        private final List<TransactionId> committed;

        private final List<TransactionId> rolledBack;

        public ReplicaTransactionsSummary(String replicaName, List<TransactionId> committed, List<TransactionId> rolledBack)
        {
            this.replicaName = replicaName;
            this.committed = committed;
            this.rolledBack = rolledBack;
        }

        public static ReplicaTransactionsSummary create(String replicaName, List<String> committed, List<String> rolledBack)
        {
            return new ReplicaTransactionsSummary(replicaName, transformToTransactionIds(committed), transformToTransactionIds(rolledBack));
        }

        public static List<TransactionId> transformToTransactionIds(List<String> strings) {
            return strings.stream().map(s -> (TransactionId)new TransactionTimeUUID(UUID.fromString(s))).collect(toList());
        }
    }

    public static class ReplicaTransactionConverganceResult {
        private final String replicaName;

        private final TransactionId transactionId;

        private final boolean committed;

        public ReplicaTransactionConverganceResult(String replicaName, TransactionId transactionId, boolean committed)
        {
            this.replicaName = replicaName;
            this.transactionId = transactionId;
            this.committed = committed;
        }
    }

    public List<TransactionId> getCommittedTransactionsAccordingToReplicas(List<ReplicaTransactionsSummary> summaries) {
        return getTransactionsAccordingToReplicas(summaries, true);
    }

    public List<TransactionId> getRolledBackTransactionsAccordingToReplicas(List<ReplicaTransactionsSummary> summaries) {
        return getTransactionsAccordingToReplicas(summaries, false);
    }

    public List<TransactionId> getTransactionsAccordingToReplicas(List<ReplicaTransactionsSummary> summaries, boolean committed) {
        List<TransactionId> allTransactionsSeenByReplicas = getAllTransactionIdsSeenByReplicas(summaries);
        return allTransactionsSeenByReplicas.stream().map(transactionId -> {
            List<Optional<ReplicaTransactionConverganceResult>> results = summaries.stream().map(getReplicaConverganceResult(transactionId)).collect(toList());

            List<ReplicaTransactionConverganceResult> replicaConverganceResults = results.stream().filter(Optional::isPresent).map(Optional::get).collect(toList());
            Assert.assertEquals(1, replicaConverganceResults.stream().map(r -> r.committed).distinct().count());

            ReplicaTransactionConverganceResult converganceResult = replicaConverganceResults.iterator().next();
            if (converganceResult.committed == committed)
            {
                return Optional.of(converganceResult.transactionId);
            }
            else
            {
                return Optional.<TransactionId>empty();
            }
        }).filter(Optional::isPresent).map(Optional::get).collect(toList());
    }

    public void checkThatReplicasAgreeAboutCommittedAndRolledBackTransactions(List<ReplicaTransactionsSummary> summaries) {
        List<TransactionId> allTransactionsSeenByReplicas = getAllTransactionIdsSeenByReplicas(summaries);
        System.out.println("Replicas have seen " + allTransactionsSeenByReplicas.size() + " transactions");

        // Each transaction ID should be either COMMITTED or ROLLED back. Never both.
        Optional<String> maybeDisconvergence = allTransactionsSeenByReplicas.stream().map(transactionId -> {
            List<Optional<ReplicaTransactionConverganceResult>> results = summaries.stream().map(getReplicaConverganceResult(transactionId)).collect(toList());

            List<ReplicaTransactionConverganceResult> replicaConverganceResults = results.stream().filter(Optional::isPresent).map(Optional::get).collect(toList());

            Assert.assertTrue("Tx " + transactionId + " should have some convergance results", !replicaConverganceResults.isEmpty());


            long count = replicaConverganceResults.stream().map(r -> r.committed).distinct().count();
            if (count != 1)
            {
                // Some replicas didn't converge on status of that transaction ID.

                List<String> replicasThatThinkTxCommitted = replicaConverganceResults.stream().filter(r -> r.committed).map(r -> r.replicaName).collect(toList());
                List<String> replicasThatThinkTxRolledBack = replicaConverganceResults.stream().filter(r -> !r.committed).map(r -> r.replicaName).collect(toList());

                String msg = String.format("There is disconvergence for transaction %s. Committed by: %s Rolled back by: %s", transactionId.unwrap().toString(),
                                           replicasThatThinkTxCommitted,
                                           replicasThatThinkTxRolledBack);
                return Optional.of(msg);
            }
            else
            {
                return Optional.<String>empty();
            }
        }).filter(Optional::isPresent).map(Optional::get).reduce(String::concat);

        // TODO [MPP] Removing assert
//        Assert.assertTrue(maybeDisconvergence.orElse(""), !maybeDisconvergence.isPresent());
        if(!maybeDisconvergence.isPresent()) {
            System.out.println("Replicas DO NOT agree on state of transactions");
            System.err.println("ERROR " + maybeDisconvergence.orElse(""));
        }
    }

    private Function<ReplicaTransactionsSummary, Optional<ReplicaTransactionConverganceResult>> getReplicaConverganceResult(TransactionId transactionId)
    {
        return summary -> {
            boolean committedInReplica = summary.committed.contains(transactionId);
            boolean rolledBackInReplica = summary.rolledBack.contains(transactionId);

            // If not committed nor rolled back then it is fine
            if (!committedInReplica && !rolledBackInReplica)
            {
                return Optional.<ReplicaTransactionConverganceResult>empty();
            }
            else if (committedInReplica && !rolledBackInReplica)
            {
                // was committed
                return Optional.of(new ReplicaTransactionConverganceResult(summary.replicaName, transactionId, true));
            }
            else if (!committedInReplica && rolledBackInReplica)
            {
                // was rolled back
                return Optional.of(new ReplicaTransactionConverganceResult(summary.replicaName, transactionId, false));
            }
            else if(committedInReplica && rolledBackInReplica)
            {
                System.err.println("ERROR " + "Tx " + transactionId + " was committed and rolled back at same replica " + summary.replicaName);
                return Optional.empty();
            }
            else
            {
                throw new RuntimeException("Unexpected condition in replicas transaction status convergence check");
            }
        };
    }

    private List<TransactionId> getAllTransactionIdsSeenByReplicas(List<ReplicaTransactionsSummary> summaries)
    {
        return summaries.stream().flatMap(summary -> {
            return Stream.concat(summary.committed.stream(), summary.rolledBack.stream());
        }).distinct().collect(toList());
    }

    private List<ReplicaTransactionsSummary> getSummaryOfTransactionsPerReplica()
    {
        return getNodeProbesNamedStream().map(namedProbe -> {
            NodeProbe nodeProbe = namedProbe.nodeProbe;
            String [] committed1 = nodeProbe.getMppProxy().listOfCommittedTransactions();
            List<String> committed = getListOf(committed1);
            List<String> rolledBack = getListOf(nodeProbe.getMppProxy().listOfRolledBackTransactions());
            return ReplicaTransactionsSummary.create(namedProbe.name, committed, rolledBack);
        }).collect(toList());
    }

    private void displaySummaryOfCommits()
    {
        System.out.println("Printing committed and rolled back transactions at each node");
        getNodeProbesNamedStream().forEach(namedProbe -> {
            String info = "For node " + namedProbe.name;
            NodeProbe nodeProbe = namedProbe.nodeProbe;
            String [] committed1 = nodeProbe.getMppProxy().listOfCommittedTransactions();
            List<String> committed = getListOf(committed1);
            info = info + "\n" + "Committed transactions: " + committed;
            List<String> rolledBack = getListOf(nodeProbe.getMppProxy().listOfRolledBackTransactions());
            info = info + "\n" + "Rolled back transactions: " + rolledBack;
            List<String> historyOfCommitRollback = getListOf(nodeProbe.getMppProxy().listOfCommittedAndRolledBack());
            info = info + "\n" + "Order: " + historyOfCommitRollback;

            info = info + "\n" + "Rolled back count: " + rolledBack.size() + " committed count: " + committed.size();

            System.out.println(info);
        });
    }

    @Test
    public void testSelectWithMultiPartitionPaxosShouldReturnDataWhenThereAreNoInProgressTransactions() {
        try(Session session = getAnySession()) {
            String postfix = String.valueOf(System.currentTimeMillis());
            CountersSchemaHelpers.NamedCounterData counterData = MppCountersTestSchema.countersNamedCounter1.createCounterData(ks1NamedCounter1_1 + postfix);
            CounterAndItsTable counterAndItsTable = new CounterAndItsTable(counterData, MppCountersTestSchema.countersNamedCounter1);
            // Initial data, all ones
            counterAndItsTable.counter.setCounter1(1);
            counterAndItsTable.counter.setCounter2(1);
            counterAndItsTable.counter.setCounter3(1);
            counterAndItsTable.counter.setCounter4(1);
            counterAndItsTable.counter.setCounter5(1);
            counterAndItsTable.persist(session, ConsistencyLevel.QUORUM);

            // Do normal select, but use ConsistencyLevel=LOCAL_TRANSACTIONAL or TRANSACTIONAL
            SimpleStatement selectCounterById = new SimpleStatement(String.format("SELECT * FROM %s.%s WHERE id = ?", counterAndItsTable.table.keyspaceName,
                                                                                counterAndItsTable.table.tableName), counterAndItsTable.counter.id);
            selectCounterById.setConsistencyLevel(ConsistencyLevel.LOCAL_TRANSACTIONAL);
            ResultSet resultSet = session.execute(selectCounterById);

            List<CountersSchemaHelpers.NamedCounterData> nameds = counterAndItsTable.table.readRows(resultSet);
            CountersSchemaHelpers.NamedCounterData actualCounterData = nameds.iterator().next();

            Assert.assertEquals(actualCounterData.counter1, 1);
            Assert.assertEquals(actualCounterData.counter2, 1);
            Assert.assertEquals(actualCounterData.counter3, 1);
            Assert.assertEquals(actualCounterData.counter4, 1);
            Assert.assertEquals(actualCounterData.counter5, 1);
        }
    }

    @Test
    public void testSelectWithMultiPartitionPaxosWhenThereIsInProgressProposal() {
        try(Session session = getAnySession()) {
            CountersSchemaHelpers.CounterData counterData = MppCountersTestSchema.stopAfterProposedTable.createCounterData(UUIDs.random(), 1, 1, 1, 1, 1);
            CounterAndItsTable counterAndItsTable = new CounterAndItsTable(counterData, MppCountersTestSchema.stopAfterProposedTable);
            counterAndItsTable.persist(session, ConsistencyLevel.QUORUM);

            TransactionState transactionState = beginTransaction(session);

            // Modifications within transaction
            counterAndItsTable.counter.setCounter1(1);
            counterAndItsTable.counter.setCounter2(2);
            counterAndItsTable.counter.setCounter3(3);
            counterAndItsTable.counter.setCounter4(4);
            counterAndItsTable.counter.setCounter5(5);

            transactionState = counterAndItsTable.persistUsingTransaction(transactionState, session);

            try {
                // It should fail, because of special table name "stop_after_proposed", but before it fails it should propose that transaction
                // successfully.
                ResultSet rows = commitTransaction(session, transactionState);
                System.out.println("Commit transaction results" + rows);
            }
            catch (Exception e) {
                System.out.println("Commit transaction exception" + e);
            }

            // After transaction was stopped, query should return counter with data: 1,1,1,1,1
            counterAndItsTable.refresh(session);
            Assert.assertEquals("It should have failed on commit", 1, counterAndItsTable.counter.counter1);
            Assert.assertEquals("It should have failed on commit", 1, counterAndItsTable.counter.counter2);
            Assert.assertEquals("It should have failed on commit", 1, counterAndItsTable.counter.counter3);
            Assert.assertEquals("It should have failed on commit", 1, counterAndItsTable.counter.counter4);
            Assert.assertEquals("It should have failed on commit", 1, counterAndItsTable.counter.counter5);

            // But doing Select with CL=LOCAL_TRANSACTIONAL it should finish transaction and return with modified state.

            SimpleStatement selectCounterById = new SimpleStatement(String.format("SELECT * FROM %s.%s WHERE id = ?", counterAndItsTable.table.keyspaceName,
                                                                                  counterAndItsTable.table.tableName), counterAndItsTable.counter.id);
            selectCounterById.setConsistencyLevel(ConsistencyLevel.LOCAL_TRANSACTIONAL);
            ResultSet resultSet = session.execute(selectCounterById);

            List<CountersSchemaHelpers.CounterData> nameds = counterAndItsTable.table.readRows(resultSet);
            CountersSchemaHelpers.CounterData actualCounterDataAfterTransactionWasFinished = nameds.iterator().next();

            Assert.assertEquals(1, actualCounterDataAfterTransactionWasFinished.counter1);
            Assert.assertEquals(2, actualCounterDataAfterTransactionWasFinished.counter2);
            Assert.assertEquals(3, actualCounterDataAfterTransactionWasFinished.counter3);
            Assert.assertEquals(4, actualCounterDataAfterTransactionWasFinished.counter4);
            Assert.assertEquals(5, actualCounterDataAfterTransactionWasFinished.counter5);
        }
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

        System.out.println("Tested transaction ID is: " + transactionState.getTransactionId());

        commitTransaction(sessionN1, transactionState);


        displaySummaryOfCommits();

        counterTestData.refresh(sessionN1);

        Assert.assertEquals(1, counterTestData.counter1.counter.counter1);
        Assert.assertEquals(2, counterTestData.counter1.counter.counter2);
        Assert.assertEquals(3, counterTestData.counter1.counter.counter3);
        Assert.assertEquals(4, counterTestData.counter1.counter.counter4);
        Assert.assertEquals(5, counterTestData.counter1.counter.counter5);

        Assert.assertEquals(1, counterTestData.counter2.counter.counter1);
        Assert.assertEquals(1, counterTestData.counter2.counter.counter2);
        Assert.assertEquals(5, counterTestData.counter2.counter.counter3);
        Assert.assertEquals(1, counterTestData.counter2.counter.counter4);
        Assert.assertEquals(1, counterTestData.counter2.counter.counter5);

        Assert.assertEquals(1, counterTestData.counter3.counter.counter1);
        Assert.assertEquals(2, counterTestData.counter3.counter.counter2);
        Assert.assertEquals(1, counterTestData.counter3.counter.counter3);
        Assert.assertEquals(2, counterTestData.counter3.counter.counter4);
        Assert.assertEquals(1, counterTestData.counter3.counter.counter5);
    }

    private CountersSchemaHelpers.NamedCounterData createCounter3()
    {
        String counterName = "this_is_very_long_name_for_counter";
        CountersSchemaHelpers.NamedCounterData counterData3 = createNamedCounter(counterName);
        counterData3.setCounter1(1);
        counterData3.setCounter2(2);
        counterData3.setCounter3(1);
        counterData3.setCounter4(2);
        counterData3.setCounter5(1);
        return counterData3;
    }

    private static CountersSchemaHelpers.NamedCounterData createNamedCounter(String counterName)
    {
        CountersSchemaHelpers.NamedCounterData counterData3 = CountersSchemaHelpers.NamedCounterData.newUsingName(counterName);

        return counterData3;
    }

    private CountersSchemaHelpers.CounterData createCounter2()
    {
        CountersSchemaHelpers.CounterData counterData2 = CountersSchemaHelpers.CounterData.newUsingId(UUIDs.random());
        counterData2.setCounter1(1);
        counterData2.setCounter2(1);
        counterData2.setCounter3(5);
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
