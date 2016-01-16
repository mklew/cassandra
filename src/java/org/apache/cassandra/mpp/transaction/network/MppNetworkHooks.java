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

/**
 * Hooks executed during execution of {@link MppNetworkService}
 *
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 16/01/16
 */
public interface MppNetworkHooks
{
    /**
     * @param message to be sent
     * @param receipient which receives message
     */
    void outgoingMessageBeforeSending(MppMessageEnvelope message, MppNetworkService.MessageReceipient receipient);

    /**
     *  @param message that was just sent
     * @param receipient which receives message
     */
    void outgoingMessageHasBeenSent(MppMessageEnvelope message, MppNetworkService.MessageReceipient receipient);

    /**
     *
     * @param message message for which timeout has occurred
     * @param receipient receipient which was supposed to handle message and send response within timeout limits
     */
    void messageHasTimedOut(MppMessage message, MppNetworkService.MessageReceipient receipient);

    /**
     *  @param messageId that was handled successfully
     * @param receipients that handled message
     */
    void messageHasBeenHandledSuccessfully(long messageId, Collection<MppNetworkService.MessageReceipient> receipients);
}
