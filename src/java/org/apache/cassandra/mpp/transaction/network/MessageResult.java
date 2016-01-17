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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.cassandra.utils.Pair;

/**
* @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
* @since 17/01/16
*/
public class MessageResult<T>
{
    Optional<CompletableFuture<T>> expectedResponseFuture;

    List<CompletableFuture<Pair<MppNetworkService.MessageReceipient, Optional<Throwable>>>> messageSentIntoNetwork;

    public MessageResult(CompletableFuture<T> future, List<CompletableFuture<Pair<MppNetworkService.MessageReceipient, Optional<Throwable>>>> messageSentIntoNetwork)
    {
        this.expectedResponseFuture = Optional.ofNullable(future);
        this.messageSentIntoNetwork = messageSentIntoNetwork;

    }

    public CompletableFuture<T> getResponseFuture() {
        return expectedResponseFuture.get();
    }

    public List<CompletableFuture<Pair<MppNetworkService.MessageReceipient, Optional<Throwable>>>> getMessageSentIntoNetwork()
    {
        return messageSentIntoNetwork;
    }

    public CompletableFuture<Pair<MppNetworkService.MessageReceipient, Optional<Throwable>>> singleMessageSentIntoNetwork()
    {
        return messageSentIntoNetwork.iterator().next();
    }
}
