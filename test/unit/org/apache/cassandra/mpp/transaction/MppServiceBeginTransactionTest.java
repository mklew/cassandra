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

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.utils.UUIDs;
import junit.framework.Assert;
import org.apache.cassandra.mpp.MppExtensionServices;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.network.MppResponseMessage;
import org.apache.cassandra.mpp.transaction.network.SingleMppMessageResponseExpectations;
import org.apache.cassandra.mpp.transaction.network.messages.MppStartTransactionMessageRequest;
import org.apache.cassandra.mpp.transaction.network.messages.MppTransactionStateResponse;
import org.apache.cassandra.mpp.transaction.testutils.NsServiceLookup;
import org.apache.cassandra.mpp.transaction.testutils.NsServiceProducer;
import org.apache.cassandra.mpp.transaction.testutils.NsServiceRef;
import org.apache.cassandra.mpp.transaction.testutils.TestWithNsServices;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 20/01/16
 */
public class MppServiceBeginTransactionTest
{
    public static final MppExtensionServices MPP_EXTENSION_SERVICES = new MppExtensionServices();

    private static ExecutorService backgroundTestExecutor;

    @BeforeClass
    public static void setupMppExtension()
    {
        backgroundTestExecutor = Executors.newFixedThreadPool(2);
        MPP_EXTENSION_SERVICES.start();
    }

    @AfterClass
    public static void shutdownMppExtension()
    {
        backgroundTestExecutor.shutdown();
        MPP_EXTENSION_SERVICES.stop();
    }

    @Test
    public void shouldBeginNewTransaction() throws Exception
    {
        MPP_EXTENSION_SERVICES.getNetworkService().setTimeoutHandlingEnabled(false);
        final long startTime = System.currentTimeMillis();
        CompletableFuture<Object> isTestDone = new CompletableFuture<>();
        final TestWithNsServices testCase = new TestWithNsServices()
        {
            protected void setup(NsServiceProducer nsServiceProducer)
            {
                nsServiceProducer.createNextNsService("client").setDefaultTimeout(10_000).setTimeoutHandlingEnabled(false);
            }

            protected CompletableFuture<Object> runTest(NsServiceLookup nsServiceLookup)
            {
                final NsServiceRef client = nsServiceLookup.getByName("client");

                final CompletableFuture<MppResponseMessage> responseFuture = client.sendMessage(new MppStartTransactionMessageRequest(), new SingleMppMessageResponseExpectations(), Collections.singleton(MPP_EXTENSION_SERVICES.getNsServicePortRef())).getResponseFuture();

                responseFuture.thenAcceptAsync(response -> {
                    final MppTransactionStateResponse transactionStateResponse = (MppTransactionStateResponse) response;
                    final TransactionState transactionState = transactionStateResponse.getTransactionState();
                    Assert.assertTrue("There are no transaction items", transactionState.getTransactionItems().isEmpty());
                    long responseTime = System.currentTimeMillis();
                    final UUID transactionId = transactionState.getTransactionId();

                    final long txTimestamp = UUIDs.unixTimestamp(transactionId);

                    Assert.assertTrue("Transaction timestamp is after timestamp from begining of this test", txTimestamp > startTime);
                    Assert.assertTrue("Transaction timestamp is before or equal to timestamp when response has been received", txTimestamp <= responseTime);
                    isTestDone.complete(null);
                });

                return isTestDone;
            }
        };
        backgroundTestExecutor.submit(testCase);
        testCase.getHasShutdown().get();
    }
}
