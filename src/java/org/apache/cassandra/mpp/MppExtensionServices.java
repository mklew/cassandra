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

package org.apache.cassandra.mpp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.mpp.transaction.MppMessageHandler;
import org.apache.cassandra.mpp.transaction.internal.LoggingMessageHandler;
import org.apache.cassandra.mpp.transaction.network.MppNetworkService;
import org.apache.cassandra.mpp.transaction.network.MppNetworkServiceImpl;
import org.apache.cassandra.service.NativeTransportService;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 17/01/16
 */
public class MppExtensionServices
{
    private static final Logger logger = LoggerFactory.getLogger(NativeTransportService.class);

    private MppNetworkService networkService;

    public void start()
    {
        logger.info("[MPP] MppExtensionServices are starting...");
        initialize();
        startInternal();
        logger.info("[MPP] MppExtensionServices have started!");
    }

    private void startInternal()
    {
        networkService.start();
    }

    private void initialize()
    {
        // TODO [MPP] Change it to real message handler after.
        // TODO [MPP] It also allows for dynamic runtime changes which might be useful for testing
        MppMessageHandler messageHandler = new LoggingMessageHandler();

        final MppNetworkServiceImpl mppNetworkService = new MppNetworkServiceImpl();
        networkService = mppNetworkService;

        int nativePort = DatabaseDescriptor.getNativeTransportPort();
        // TODO [MPP] Maybe move it to yaml config.
        mppNetworkService.setListeningPort(nativePort + 1);
        mppNetworkService.setMessageHandler(messageHandler);
    }

    public void stop()
    {
        logger.info("[MPP] MppExtensionServices are stopping...");
        stopMppNetworkService();
    }

    private void stopMppNetworkService()
    {
        try
        {
            networkService.shutdown();
        }
        catch (Exception e)
        {
            logger.error("Exception during MppNetworkService.shutdown()", e);
        }
    }

    public boolean isRunning()
    {
        return networkService.isRunning();
    }
}
