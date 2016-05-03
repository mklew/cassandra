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

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.mpp.transaction.MppService;
import org.apache.cassandra.mpp.transaction.internal.MppMessageHandlerImpl;
import org.apache.cassandra.mpp.transaction.network.MppNetworkService;
import org.apache.cassandra.mpp.transaction.network.MppNetworkServiceImpl;
import org.apache.cassandra.mpp.transaction.network.NoOpMppNetworkService;
import org.apache.cassandra.mpp.transaction.testutils.NsServicePortRef;
import org.apache.cassandra.mpp.transaction.testutils.NsServicePortRefImpl;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 17/01/16
 */
public class MppExtensionServices
{
    private static final Logger logger = LoggerFactory.getLogger(MppExtensionServices.class);

    private MppNetworkService networkService;

    private MppModule mppModule;

    public void start()
    {
        logger.info("[MPP] MppExtensionServices are starting...");
        initialize();
        startInternal();
        logger.info("[MPP] MppExtensionServices have started!");
    }

    public void startWithoutNetwork()
    {
        logger.info("[MPP] MppExtensionServices are starting without network...");
        initializeMppModule(new NoOpMppNetworkService());
        logger.info("[MPP] MppExtensionServices have started!");
    }

    private void startInternal()
    {
        networkService.start();
    }

    private void initialize()
    {


        MppMessageHandlerImpl messageHandler = new MppMessageHandlerImpl();

        final MppNetworkServiceImpl mppNetworkService = new MppNetworkServiceImpl();
        networkService = mppNetworkService;

        initializeMppModule(mppNetworkService);

        registerMppServiceAsMBean(mppModule);

        messageHandler.setMppService(mppModule.getMppService());
//        messageHandler.setPrivateMemtableStorage(privateMemtableStorage);
//        messageHandler.setReadTransactionDataService(readTransactionDataService);

        int nativePort = DatabaseDescriptor.getNativeTransportPort();
        // TODO [MPP] Maybe move it to yaml config.
        mppNetworkService.setListeningPort(nativePort + 1);
        mppNetworkService.setMessageHandler(messageHandler);
    }

    private static void registerMppServiceAsMBean(MppModule mppModule)
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(mppModule.getMppService(), new ObjectName(MppService.MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private void initializeMppModule(MppNetworkService mppNetworkService)
    {
        mppModule = MppModule.createModule(mppNetworkService);
        MppServicesLocator.setInstance(mppModule.getMppService());
        MppServicesLocator.setTransactionLog(mppModule.getTransactionLog());
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
            if(networkService != null) {
                networkService.shutdown();
            }
        }
        catch (Exception e)
        {
            logger.error("Exception during MppNetworkService.shutdown()", e);
        }
    }

    public NsServicePortRef getNsServicePortRef()
    {
        return new NsServicePortRefImpl(networkService.getListeningPort());
    }

    public boolean isRunning()
    {
        return networkService.isRunning();
    }

    public MppNetworkService getNetworkService()
    {
        return networkService;
    }

    public MppModule getMppModule()
    {
        return mppModule;
    }
}
