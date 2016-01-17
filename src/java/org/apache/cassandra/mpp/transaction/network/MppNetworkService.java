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

import java.net.InetAddress;
import java.util.Collection;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 06/12/15
 */
public interface MppNetworkService
{
    interface MessageReceipient {
        InetAddress host();

        int port();
    }

    /**
     *
     * @param message to be sent
     * @param mppMessageResponseExpectations
     *@param receipients @return future or {@code null}
     */
    <T> MessageResult<T> sendMessage(MppMessage message, MppMessageResponseExpectations<T> mppMessageResponseExpectations, Collection<MessageReceipient> receipients);

    void handleIncomingMessage(MppMessageEnvelope envelope, MessageReceipient from);

    MessageReceipient createReceipient(InetAddress addr);

    MessageReceipient createReceipient(InetAddress addr, int port);

    void initialize();

    void shutdown() throws Exception;
}
