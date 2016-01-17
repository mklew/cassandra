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

package org.apache.cassandra.mpp.transaction.network;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Preconditions;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 06/12/15
 */
public class SingleMppMessageResponseExpectations implements MppMessageResponseExpectations<MppResponseMessage>
{
    @Override
    public boolean expectsResponse()
    {
        return true;
    }

    private static class SingleResponseDataHolder implements MppMessageResponseDataHolder<MppResponseMessage> {

        final CompletableFuture<MppResponseMessage> future;

        final MppMessageReceipient receipient;

        private SingleResponseDataHolder(CompletableFuture<MppResponseMessage> future, MppMessageReceipient receipient)
        {
            this.future = future;
            this.receipient = receipient;
        }

        public CompletableFuture<MppResponseMessage> getFuture()
        {
            return future;
        }
    }

    @Override
    public MppMessageResponseDataHolder<MppResponseMessage> createDataHolder(MppMessage message, Collection<MppMessageReceipient> receipients)
    {
        Preconditions.checkArgument(receipients.size() == 1, "Expected single receipient, but had %s", receipients);
        return new SingleResponseDataHolder(new CompletableFuture<>(), receipients.iterator().next());
    }

    public void timeoutHasOccurred(MppMessageResponseDataHolder dataHolder, long messageId, MppMessageReceipient receipient)
    {
        SingleResponseDataHolder singleResponseDataHolder = (SingleResponseDataHolder) dataHolder;
        singleResponseDataHolder.future.completeExceptionally(new TimeoutException("Timeout occurred for receipient" + receipient));
    }

    @Override
    public boolean maybeCompleteResponse(MppMessageResponseDataHolder dataHolder, MppMessage incomingMessage, MppMessageReceipient from)
    {
        SingleResponseDataHolder singleResponseDataHolder = (SingleResponseDataHolder) dataHolder;
        Preconditions.checkArgument(checkReceipient(singleResponseDataHolder.receipient, from));
        Preconditions.checkArgument(Objects.equals(singleResponseDataHolder.receipient.port(), from.port()));
        dataHolder.getFuture().complete(incomingMessage);
        return true;
    }

    public static boolean checkReceipient(MppMessageReceipient exepctedReceipient, MppMessageReceipient actualReceipient)
    {
        return exepctedReceipient.host().getHostAddress().equals(actualReceipient.host().getHostAddress());
    }
}
