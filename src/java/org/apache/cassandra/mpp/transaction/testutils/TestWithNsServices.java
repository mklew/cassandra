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

package org.apache.cassandra.mpp.transaction.testutils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.mpp.transaction.network.MessageResult;
import org.apache.cassandra.mpp.transaction.network.MppMessage;
import org.apache.cassandra.mpp.transaction.network.MppMessageResponseExpectations;

/**
* @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
* @since 20/01/16
*/
public abstract class TestWithNsServices implements Runnable
{
    private final static Logger logger = LoggerFactory.getLogger(TestWithNsServices.class);

    final NsServiceProducer nsServiceProducer = new NsServiceProducer();

    CompletableFuture<Object> hasShutdown = new CompletableFuture<>();

    @Override
    public void run()
    {
        setup(nsServiceProducer);
        nsServiceProducer.initServices();
        Exception ex = null;
        try
        {
            try
            {
                final CompletableFuture<Object> isTestDone = runTest(nsServiceProducer);
                isTestDone.get(10_000, TimeUnit.MILLISECONDS);
            }
            catch (Exception e)
            {
                logger.error("Error during runTest", e);
                ex = e;
            }
        }
        finally
        {
            nsServiceProducer.shutdownServices();
            if(ex != null) {
                hasShutdown.completeExceptionally(ex);
            }
            else {
                hasShutdown.complete(null);
            }
        }
    }

    public CompletableFuture<Object> getHasShutdown()
    {
        return hasShutdown;
    }

    protected <T> MessageResult<T> sendMessage(String from,
                                                  MppMessage message,
                                                  MppMessageResponseExpectations<T> mppMessageResponseExpectations,
                                                  String ... receipients) {
        final NsServiceRef ref = nsServiceProducer.getByName(from);

        Collection<NsServicePortRef> receipientRefs = new ArrayList<>();
        for (String receipient : receipients)
        {
            receipientRefs.add(nsServiceProducer.getByName(receipient));
        }

        return ref.sendMessage(message, mppMessageResponseExpectations, receipientRefs);
    }


    abstract protected void setup(NsServiceProducer nsServiceProducer);

    abstract protected CompletableFuture<Object> runTest(NsServiceLookup nsServiceLookup) throws Exception;
}
