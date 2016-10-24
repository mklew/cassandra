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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.mpp.transaction.MppTestingUtilities;
import org.apache.cassandra.mpp.transaction.TransactionId;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.client.dto.TransactionStateDto;
import org.apache.cassandra.tools.NodeProbe;

import static org.apache.cassandra.mpp.transaction.MppTestingUtilities.START_TRANSACTION;
import static org.apache.cassandra.mpp.transaction.MppTestingUtilities.mapResultToTransactionState;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 20/02/16
 */
public abstract class BaseClusterTest
{
    private NodeProbe nodeProbe1;

    private NodeProbe nodeProbe2;

    private NodeProbe nodeProbe3;

    Queue<TransactionId> beganTransactions;

    protected TransactionState beginTransaction(Session session)
    {
        TransactionState transactionState = MppTestingUtilities.mapResultToTransactionState(session.execute("START TRANSACTION"));
        beganTransactions.add(transactionState.id());
        return transactionState;
    }

    private Collection<Session> sessionsOpended;

    public static String transactionStateToJson(TransactionState transactionState)
    {
        final TransactionStateDto transactionStateDto = TransactionStateDto.fromTransactionState(transactionState);
        final ObjectMapper objectMapper = new ObjectMapper();
        try
        {
            return objectMapper.writeValueAsString(transactionStateDto);
        }
        catch (JsonProcessingException e)
        {
            throw new RuntimeException(e);
        }
    }

    protected static ResultSet commitTransaction(Session session, TransactionState transactionState)
    {
        final TransactionStateDto transactionStateDto = TransactionStateDto.fromTransactionState(transactionState);
        String result;
        final ObjectMapper objectMapper = new ObjectMapper();
        try
        {
            result = objectMapper.writeValueAsString(transactionStateDto);
        }
        catch (JsonProcessingException e)
        {
            throw new RuntimeException(e);
        }
        final String txStateJson = result;

        // TODO [MPP] I get operation time out when trying to use prepared statement. None of hosts can handle that prepared statement.

//        PreparedStatement preparedCommitTransactionStmt = sessionN1.prepare("COMMIT TRANSACTION AS JSON ?");
//        BoundStatement boundStatement = preparedCommitTransactionStmt.bind("'" + txStateJson + "'");
//        sessionN1.execute(boundStatement);

        // Json had to be wrapped in single quotes
        return session.execute("COMMIT TRANSACTION AS JSON '" + txStateJson + "'");
    }

    protected static ResultSet commitTransactionWithCond(Session session, TransactionState transactionState, String cond)
    {
        final TransactionStateDto transactionStateDto = TransactionStateDto.fromTransactionState(transactionState);
        String result;
        final ObjectMapper objectMapper = new ObjectMapper();
        try
        {
            result = objectMapper.writeValueAsString(transactionStateDto);
        }
        catch (JsonProcessingException e)
        {
            throw new RuntimeException(e);
        }
        final String txStateJson = result;

        // TODO [MPP] I get operation time out when trying to use prepared statement. None of hosts can handle that prepared statement.

//        PreparedStatement preparedCommitTransactionStmt = sessionN1.prepare("COMMIT TRANSACTION AS JSON ?");
//        BoundStatement boundStatement = preparedCommitTransactionStmt.bind("'" + txStateJson + "'");
//        sessionN1.execute(boundStatement);

        // Json had to be wrapped in single quotes
        String query = "COMMIT TRANSACTION AS JSON '" + txStateJson + "' " + cond;
        System.out.println("About to commit transaction:");
        System.out.println(query);
        return session.execute(query);
    }

    protected static ResultSetFuture commitTransactionAsync(Session session, TransactionState transactionState)
    {
        final TransactionStateDto transactionStateDto = TransactionStateDto.fromTransactionState(transactionState);
        String result;
        final ObjectMapper objectMapper = new ObjectMapper();
        try
        {
            result = objectMapper.writeValueAsString(transactionStateDto);
        }
        catch (JsonProcessingException e)
        {
            throw new RuntimeException(e);
        }
        final String txStateJson = result;

        // TODO [MPP] I get operation time out when trying to use prepared statement. None of hosts can handle that prepared statement.

//        PreparedStatement preparedCommitTransactionStmt = sessionN1.prepare("COMMIT TRANSACTION AS JSON ?");
//        BoundStatement boundStatement = preparedCommitTransactionStmt.bind("'" + txStateJson + "'");
//        sessionN1.execute(boundStatement);

        // Json had to be wrapped in single quotes
        return session.executeAsync("COMMIT TRANSACTION AS JSON '" + txStateJson + "'");
    }

    @Before
    public void setUpTest() {
        sessionsOpended = new ArrayList<>();
    }

    @Before
    public void resetStartedTransactions() {
        beganTransactions = new ConcurrentLinkedQueue<>();
    }

    @After
    public void afterTest() {
        sessionsOpended.forEach(session -> {

            if(!session.isClosed()) {
                session.close();
            }
        });
    }

    protected NodeProbe getNodeProbe1()
    {

        if (nodeProbe1 == null)
        {
            nodeProbe1 = initNodeProbe(7100);
            return nodeProbe1;
        }
        else
        {
            return nodeProbe1;
        }
    }

    protected NodeProbe getNodeProbe2()
    {

        if (nodeProbe2 == null)
        {
            nodeProbe2 = initNodeProbe(7200);
            return nodeProbe2;
        }
        else
        {
            return nodeProbe2;
        }
    }

    protected NodeProbe getNodeProbe3()
    {

        if (nodeProbe3 == null)
        {
            nodeProbe3 = initNodeProbe(7300);
            return nodeProbe3;
        }
        else
        {
            return nodeProbe3;
        }
    }

    protected NodeProbe initNodeProbe(int jmxPort)
    {
        try
        {
            return new NodeProbe("127.0.0.1", jmxPort);
        }
        catch (IOException e)
        {
            // throw new RuntimeException(e);
            return null;
        }
    }

    static class NamedNodeProbe {
        NodeProbe nodeProbe;

        String name;

        public NamedNodeProbe(NodeProbe nodeProbe, String name)
        {
            this.nodeProbe = nodeProbe;
            this.name = name;
        }
    }

    protected Stream<NodeProbe> getNodeProbesStream() {
        return Stream.of(getNodeProbe1(), getNodeProbe2(), getNodeProbe3());
    }

    protected Stream<NamedNodeProbe> getNodeProbesNamedStream() {
        return Stream.of(new NamedNodeProbe(getNodeProbe1(), "Node1"), new NamedNodeProbe(getNodeProbe2(), "Node2"), new NamedNodeProbe(getNodeProbe3(), "Node3"));
    }

    protected Session getSessionN1()
    {
        String address = "127.0.0.1";
        return getSessionWithMppTest(address);
    }

    protected Session getSessionN2()
    {
        String address = "127.0.0.2";
        return getSessionWithMppTest(address);
    }

    protected Session getSessionN3()
    {
        String address = "127.0.0.3";
        return getSessionWithMppTest(address);
    }

    protected Session getSessionWithMppTest(String address)
    {
        Cluster cluster = getSessionForNode(address);
        Session session = cluster.connect("mpptest");
        sessionsOpended.add(session);
        return session;
    }

    private Cluster getSessionForNode(String address)
    {
        return Cluster.builder().addContactPoint(address)
                      .withRetryPolicy(new LoggingRetryPolicy(Policies.defaultRetryPolicy()))
                      .withLoadBalancingPolicy(AlwaysSameNodeLoadBalancingPolicy.create(address))
                      .withPort(DatabaseDescriptor.getNativeTransportPort()).build();
    }

    protected String [] getContactPoints() {
        return new String [] { "127.0.0.1", "127.0.0.2" };
    }

    protected Session getAnySession() {
        Cluster cluster = Cluster.builder().addContactPoints(getContactPoints())
                                 .withRetryPolicy(new LoggingRetryPolicy(Policies.defaultRetryPolicy()))
                                 .withLoadBalancingPolicy(new RoundRobinPolicy())
                                 .withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        Session session = cluster.connect();
        sessionsOpended.add(session);
        return session;
    }

    protected TransactionState startTransaction(Session session) throws Throwable
    {
        return mapResultToTransactionState(session.execute(START_TRANSACTION));
    }

    protected List<String> getListOf(Object committed)
    {
        String [] committedArr = (String[])committed;
        return Stream.of(committedArr).collect(Collectors.toList());
    }
}

