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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 06/12/15
 */
public class MppNetworkServiceImpl implements MppNetworkService
{

    private static class ResponseHolder<T>
    {
        MppMessageResponseExpectations<T> expectations;

        MppMessageResponseExpectations.MppMessageResponseDataHolder dataHolder;

        private ResponseHolder(MppMessageResponseExpectations<T> expectations, MppMessageResponseExpectations.MppMessageResponseDataHolder dataHolder)
        {
            this.expectations = expectations;
            this.dataHolder = dataHolder;
        }
    }

    private Map<Long, ResponseHolder> idToResponseHolder = new ConcurrentHashMap<>();

    private <T, R extends MppMessageResponseExpectations<T>> void registerOutgoingMessage(MppMessage<T, R> message,
                                                                                          MppMessageResponseExpectations<T> mppMessageResponseExpectations,
                                                                                          MppMessageResponseExpectations.MppMessageResponseDataHolder dataHolder)
    {
        idToResponseHolder.put(message.id(), new ResponseHolder<T>(mppMessageResponseExpectations, dataHolder));
    }

    private <T, R extends MppMessageResponseExpectations<T>>  void sendMessageOverNetwork(MppMessage<T, R> message, MessageReceipient receipient) {
        // TODO [MPP] implementation over netty.
        // TODO [MPP] handle timeout
        // TODO [MPP] Just open channel; send; close channel ?
    }

    public <T, R extends MppMessageResponseExpectations<T>> CompletableFuture<T> sendMessage(MppMessage<T, R> message, MessageReceipient receipient)
    {
        final MppMessageResponseExpectations<T> mppMessageResponseExpectations = message.responseExpectations();
        final MppMessageResponseExpectations.MppMessageResponseDataHolder dataHolder = mppMessageResponseExpectations.createDataHolder(message, Collections.singletonList(receipient));
        if (mppMessageResponseExpectations.expectsResponse())
        {
            registerOutgoingMessage(message, mppMessageResponseExpectations, dataHolder);
        }
        sendMessageOverNetwork(message, receipient);
        return dataHolder.getFuture();
    }

    public <T, R extends MppMessageResponseExpectations<T>> void handleIncomingMessage(MppMessage<T, R> incommingMessage, MessageReceipient from)
    {
        if (incommingMessage.isRequest())
        {
            // TODO [MPP] We are supposed to fulfil this request.
        }
        else
        {
            // It is response to one of previous messages.
            final long id = incommingMessage.id();
            final ResponseHolder responseHolder = idToResponseHolder.get(id);
            if(responseHolder == null) {
                // TODO [MPP] It can be null if timeout has occured and response holder was already removed.
                // TODO [MPP] It can be bug
                // TODO [MPP] Log this message.
            }
            else {
                final MppMessageResponseExpectations.MppMessageResponseDataHolder dataHolder = responseHolder.dataHolder;
                boolean futureHasCompleted;
                synchronized (dataHolder) {
                    futureHasCompleted = responseHolder.expectations.maybeCompleteResponse(dataHolder, incommingMessage, from);
                }

                if(futureHasCompleted) {
                    // TODO [MPP] Log that message with ID has completed response.
                    unregisterResponseHolder(id);
                }
            }
        }
    }

    private void unregisterResponseHolder(long id)
    {
        idToResponseHolder.remove(id);
    }
}
