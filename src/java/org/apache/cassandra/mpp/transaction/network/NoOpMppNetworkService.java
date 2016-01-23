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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 23/01/16
 */
public class NoOpMppNetworkService implements MppNetworkService
{
    private static final Logger logger = LoggerFactory.getLogger(NoOpMppNetworkService.class);

    public <T> MessageResult<T> sendMessage(MppMessage message, MppMessageResponseExpectations<T> mppMessageResponseExpectations, Collection<MppMessageReceipient> receipients)
    {
        logger.debug("NoOpMppNetworkService.sendMessage  message {}, expectations {}, receipients {}", message, mppMessageResponseExpectations, receipients);
        return null;
    }

    public void handleIncomingMessage(MppMessageEnvelope envelope, MppMessageReceipient from)
    {
        logger.debug("NoOpMppNetworkService.handleIncomingMessage envelope {} from {}", envelope, from);
    }

    public MppMessageReceipient createReceipient(InetAddress addr)
    {
        logger.debug("NoOpMppNetworkService.createReceipient addr {}", addr);
        return null;
    }

    public MppMessageReceipient createReceipient(InetAddress addr, int port)
    {
        logger.debug("NoOpMppNetworkService.createReceipient addr {}, port {}", addr, port);
        return null;
    }

    public void start()
    {
        logger.debug("NoOpMppNetworkService.start");
    }

    public void shutdown() throws Exception
    {
        logger.debug("NoOpMppNetworkService.shutdown");
    }

    public boolean isRunning()
    {
        logger.debug("NoOpMppNetworkService.isRunning");
        return false;
    }

    public int getListeningPort()
    {
        logger.debug("NoOpMppNetworkService.getListeningPort");
        return 0;
    }

    public void setTimeoutHandlingEnabled(boolean enabled)
    {
        logger.debug("NoOpMppNetworkService.setTimeoutHandlingEnabled enabled {}", enabled);
    }
}
