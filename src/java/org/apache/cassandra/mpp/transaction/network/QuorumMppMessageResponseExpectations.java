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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import com.google.common.base.Preconditions;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 06/12/15
 */
public class QuorumMppMessageResponseExpectations implements MppMessageResponseExpectations<Collection<MppResponseMessage>>
{
    private final int replicationFactor;

    public QuorumMppMessageResponseExpectations(int replicationFactor)
    {
        this.replicationFactor = replicationFactor;
    }

    private static class QuorumDataHolder implements MppMessageResponseDataHolder<Collection<MppResponseMessage>> {

        private final CompletableFuture<Collection<MppResponseMessage>> future;

        private final Map<MppNetworkService.MessageReceipient, MppResponseMessage> messagesReceivedSoFar;

        private final Set<MppNetworkService.MessageReceipient> expectedReceipients;

        private QuorumDataHolder(CompletableFuture<Collection<MppResponseMessage>> future, Map<MppNetworkService.MessageReceipient, MppResponseMessage> messagesReceivedSoFar, Set<MppNetworkService.MessageReceipient> expectedReceipients)
        {
            this.future = future;
            this.messagesReceivedSoFar = messagesReceivedSoFar;
            this.expectedReceipients = expectedReceipients;
        }

        public CompletableFuture<Collection<MppResponseMessage>> getFuture()
        {
            return future;
        }

    }

    private boolean isQuorum(QuorumDataHolder q) {
        final int quoroum = replicationFactor / 2 + 1;
        return quoroum <= q.messagesReceivedSoFar.keySet().size();
    }


    public boolean expectsResponse()
    {
        return true;
    }

    public MppMessageResponseDataHolder<Collection<MppResponseMessage>> createDataHolder(MppMessage message, Collection<MppNetworkService.MessageReceipient> receipients)
    {
        CompletableFuture<Collection<MppResponseMessage>> f = new CompletableFuture<>();
        return new QuorumDataHolder(f, new HashMap<>((receipients.size() * 3 / 4 ) + 1), new HashSet<>(receipients));
    }

    public void timeoutHasOccurred(MppMessageResponseDataHolder dataHolder, long messageId, MppNetworkService.MessageReceipient receipient)
    {
         // TODO [MPP] count how many timeouts occurred
        QuorumDataHolder q = (QuorumDataHolder) dataHolder;
        q.future.completeExceptionally(new TimeoutException("Timeout occurred"));
    }

    public boolean maybeCompleteResponse(MppMessageResponseDataHolder dataHolder, MppMessage incomingMessage, MppNetworkService.MessageReceipient from)
    {
        QuorumDataHolder q = (QuorumDataHolder) dataHolder;
        Preconditions.checkArgument(q.expectedReceipients.stream().anyMatch(r -> SingleMppMessageResponseExpectations.checkReceipient(r, from)), "Recevied message from unexpected receipient %s. Expected one of %s", from, q.expectedReceipients);
        q.messagesReceivedSoFar.put(from, (MppResponseMessage) incomingMessage);
        if(isQuorum(q)) {
            if(!q.future.isDone()) {
                q.future.complete(q.messagesReceivedSoFar.values());
                return true;
            }
        }
        return false;
    }
}
