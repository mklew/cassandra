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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.cassandra.mpp.transaction.MppMessageHandler;

/**
* @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
* @since 20/01/16
*/
public class ExpectingMessageHandlerWithResponse implements MppMessageHandler
{

    interface ResponseCallback
    {
        MppResponseMessage accept(MppRequestMessage message);
    }

    List<MppRequestMessage> receivedMessages = new ArrayList<>();

    ResponseCallback callback;

    CompletableFuture<Object> awaitMessageFuture = new CompletableFuture<>();

    public ExpectingMessageHandlerWithResponse(ResponseCallback callback)
    {
        this.callback = callback;
    }

    public CompletableFuture<MppResponseMessage> handleMessage(MppRequestMessage requestMessage)
    {
        receivedMessages.add(requestMessage);
        final MppResponseMessage response = callback.accept(requestMessage);
        awaitMessageFuture.complete(requestMessage);
        final CompletableFuture<MppResponseMessage> f = new CompletableFuture<>();
        f.complete(response);
        return f;
    }

    public CompletableFuture<Object> getAwaitMessageFuture()
    {
        return awaitMessageFuture;
    }

    boolean receivedAnything()
    {
        return !receivedMessages.isEmpty();
    }
}
