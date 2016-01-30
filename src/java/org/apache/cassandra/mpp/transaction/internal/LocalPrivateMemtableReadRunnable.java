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

package org.apache.cassandra.mpp.transaction.internal;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.mpp.transaction.network.messages.PrivateMemtableReadCommand;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.FBUtilities;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 30/01/16
 */
class LocalPrivateMemtableReadRunnable extends StorageProxy.DroppableRunnable
{
    private final PrivateMemtableReadCommand command;
    private final NativeReadTransactionDataRequestExecutor.PrivateMemtableReadCallback handler;
    private final long start = System.nanoTime();

    LocalPrivateMemtableReadRunnable(PrivateMemtableReadCommand command, NativeReadTransactionDataRequestExecutor.PrivateMemtableReadCallback handler)
    {
        super(MessagingService.Verb.PRIVATE_MEMTABLE_READ);
        this.command = command;
        this.handler = handler;
    }

    protected void runMayThrow()
    {
        try
        {
            handler.response(command.execute());
            MessagingService.instance().addLatency(FBUtilities.getBroadcastAddress(), TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        }
        catch (Throwable t)
        {
            handler.onFailure(FBUtilities.getBroadcastAddress());
            throw t;
        }
    }
}
