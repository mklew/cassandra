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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ReadRepairDecision;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.mpp.transaction.ReadTransactionDataRequestExecutor;
import org.apache.cassandra.mpp.transaction.network.messages.PrivateMemtableReadCommand;
import org.apache.cassandra.mpp.transaction.network.messages.PrivateMemtableReadResponse;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

/**
 * This executor uses {@link org.apache.cassandra.net.MessagingService} to perform requests.
 *
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 30/01/16
 */
public class NativeReadTransactionDataRequestExecutor implements ReadTransactionDataRequestExecutor
{
    MessagingService messagingService;

    private static final Logger logger = LoggerFactory.getLogger(NativeReadTransactionDataRequestExecutor.class);

    public NativeReadTransactionDataRequestExecutor(MessagingService messagingService)
    {
        this.messagingService = messagingService;
    }

    public static class PrivateMemtableReadCallback implements IAsyncCallbackWithFailure<PrivateMemtableReadResponse> {

        private final SingleReadRequestRecipe readRequestRecipe;

        private final ConsistencyLevel consistencyLevel;

        private final List<InetAddress> endpoints;

        private final SimpleCondition condition = new SimpleCondition();

        private final ReentrantLock lock = new ReentrantLock();

        private final Collection<PartitionUpdate> partitionUpdates = new ArrayList<>();

        private final long start;

        final int blockfor;

        private static final AtomicIntegerFieldUpdater<PrivateMemtableReadCallback> recievedUpdater
            = AtomicIntegerFieldUpdater.newUpdater(PrivateMemtableReadCallback.class, "received");

        private volatile int received = 0;
        private static final AtomicIntegerFieldUpdater<PrivateMemtableReadCallback> failuresUpdater
            = AtomicIntegerFieldUpdater.newUpdater(PrivateMemtableReadCallback.class, "failures");
        private volatile int failures = 0;

        public PrivateMemtableReadCallback(SingleReadRequestRecipe readRequestRecipe, ConsistencyLevel consistencyLevel, List<InetAddress> endpoints)
        {
            this.readRequestRecipe = readRequestRecipe;
            this.endpoints = endpoints;
            this.consistencyLevel = consistencyLevel;
            blockfor = endpoints.size();
            this.start = System.nanoTime();
        }

        /**
         * @return true if the message counts towards the blockfor threshold
         */
        private boolean waitingFor(InetAddress from)
        {
            return consistencyLevel.isDatacenterLocal()
                   ? DatabaseDescriptor.getLocalDataCenter().equals(DatabaseDescriptor.getEndpointSnitch().getDatacenter(from))
                   : true;
        }

        public void onFailure(InetAddress from)
        {
            int n = waitingFor(from)
                    ? failuresUpdater.incrementAndGet(this)
                    : failures;

            if (blockfor + n > endpoints.size())
                condition.signalAll();
        }

        public boolean await(long timePastStart, TimeUnit unit)
        {
            long time = unit.toNanos(timePastStart) - (System.nanoTime() - start);
            try
            {
                return condition.await(time, TimeUnit.NANOSECONDS);
            }
            catch (InterruptedException ex)
            {
                throw new AssertionError(ex);
            }
        }

        static long getTimeout() {
            return DatabaseDescriptor.getTimeout(MessagingService.Verb.PRIVATE_MEMTABLE_READ);
        }

        public void awaitResults() throws ReadFailureException, ReadTimeoutException
        {
            boolean signaled = await(getTimeout(), TimeUnit.MILLISECONDS);
            boolean failed = blockfor + failures > endpoints.size();
            if (signaled && !failed)
                return;

//            if (Tracing.isTracing())
//            {
//                String gotData = received > 0 ? (resolver.isDataPresent() ? " (including data)" : " (only digests)") : "";
//                Tracing.trace("{}; received {} of {} responses{}", new Object[]{ (failed ? "Failed" : "Timed out"), received, blockfor, gotData });
//            }
            else if (logger.isDebugEnabled())
            {
//                String gotData = received > 0 ? (resolver.isDataPresent() ? " (including data)" : " (only digests)") : "";
                logger.debug("{}; received {} of {} ", new Object[]{ (failed ? "Failed" : "Timed out"), received, blockfor });
            }

            // Same as for writes, see AbstractWriteResponseHandler
            throw failed
                  ? new ReadFailureException(consistencyLevel, received, failures, blockfor, false)
                  : new ReadTimeoutException(consistencyLevel, received, blockfor, false);
        }

        public void response(PrivateMemtableReadResponse result)
        {
            MessageIn<PrivateMemtableReadResponse> message = MessageIn.create(FBUtilities.getBroadcastAddress(),
                                                               result,
                                                               Collections.<String, byte[]>emptyMap(),
                                                               MessagingService.Verb.INTERNAL_RESPONSE,
                                                               MessagingService.current_version);
            response(message);
        }

        public void response(MessageIn<PrivateMemtableReadResponse> message)
        {
            final Optional<PartitionUpdate> partitionUpdateOpt = message.payload.getPartitionUpdateOpt();

            if(partitionUpdateOpt.isPresent()) {
                lock.lock();  // block until condition holds
                try {
                    partitionUpdates.add(partitionUpdateOpt.get());
                } finally {
                    lock.unlock();
                }
            }
            else {
                // TODO maybe do more requests...
            }

            int n = waitingFor(message.from)
                    ? recievedUpdater.incrementAndGet(this)
                    : received;

            if (n >= blockfor && readRequestRecipe.isQuorum(partitionUpdates)) {
                condition.signalAll();
            }
        }

        public boolean isLatencyForSnitch()
        {
            return false;
        }

        public List<PartitionUpdate> get()
        {
            awaitResults();

            return Lists.newArrayList(partitionUpdates);
        }
    }

    /**
     *  Analogy to {@link org.apache.cassandra.service.AbstractReadExecutor}
     */
    private static abstract class ReadTransactionDataExecutor {

        protected final SingleReadRequestRecipe readRequestRecipe;

        protected final PrivateMemtableReadCallback handler;

        protected final ConsistencyLevel consistencyLevel;

        protected final List<InetAddress> targetReplicas;

        protected ReadTransactionDataExecutor(SingleReadRequestRecipe readRequestRecipe, ConsistencyLevel consistencyLevel, List<InetAddress> targetReplicas)
        {
            this.readRequestRecipe = readRequestRecipe;
            this.consistencyLevel = consistencyLevel;
            this.targetReplicas = targetReplicas;
            this.handler = new PrivateMemtableReadCallback(readRequestRecipe, consistencyLevel, targetReplicas);
        }

        /**
         * Perform additional requests if it looks like the original will time out.  May block while it waits
         * to see if the original requests are answered first.
         */
        public abstract void maybeTryAdditionalReplicas();

        /**
         * send the initial set of requests
         */
        public abstract void executeAsync();

        /**
         * wait for an answer.  Blocks until success or timeout, so it is caller's
         * responsibility to call maybeTryAdditionalReplicas first.
         */
        public List<PartitionUpdate> get() throws ReadFailureException, ReadTimeoutException
        {
            return handler.get();
        }


        /**
         * Copy of {@link org.apache.cassandra.service.AbstractReadExecutor#makeRequests}
         */
        protected void makeRequests(PrivateMemtableReadCommand privateMemtableReadCommand, Iterable<InetAddress> endpoints)
        {
            MessageOut<PrivateMemtableReadCommand> message = null;
            boolean hasLocalEndpoint = false;

            for (InetAddress endpoint : endpoints)
            {
                if (StorageProxy.canDoLocalRequest(endpoint))
                {
                    hasLocalEndpoint = true;
                    continue;
                }

                logger.trace("reading data from {}", endpoint);
                if (message == null)
                    message = privateMemtableReadCommand.createMessage(); // TODO [MPP] I don't differentiate on versions here
                MessagingService.instance().sendRRWithFailure(message, endpoint, handler);
            }

            // We delay the local (potentially blocking) read till the end to avoid stalling remote requests.
            if (hasLocalEndpoint)
            {
                logger.trace("reading data locally");
                StageManager.getStage(Stage.PRIVATE_MEMTABLES_WRITE).maybeExecuteImmediately(new LocalPrivateMemtableReadRunnable(privateMemtableReadCommand, handler));
            }
        }

        /**
         * @return an executor appropriate for the configured speculative read policy
         */
        public static ReadTransactionDataExecutor getReadExecutor(SingleReadRequestRecipe recipe, ConsistencyLevel consistencyLevel) throws UnavailableException
        {
            Keyspace keyspace = Keyspace.open(recipe.getTransactionItem().getKsName());

//            ReadRepairDecision repairDecision = command.metadata().newReadRepairDecision();
            List<InetAddress> targetReplicas = consistencyLevel.filterForQuery(keyspace, recipe.getReceipients(), ReadRepairDecision.NONE);

            // Throw UAE early if we don't have enough replicas.
            consistencyLevel.assureSufficientLiveNodes(keyspace, targetReplicas);

            return new DumbExecutor(recipe, consistencyLevel, targetReplicas);

//            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.metadata().cfId);
//            SpeculativeRetryParam retry = cfs.metadata.params.speculativeRetry;

            // Speculative retry is disabled *OR* there are simply no extra replicas to speculate.
//            if (retry.equals(SpeculativeRetryParam.NONE) || consistencyLevel.blockFor(keyspace) == allReplicas.size())
//                return new AbstractReadExecutor.NeverSpeculatingReadExecutor(keyspace, command, consistencyLevel, targetReplicas);

//            if (targetReplicas.size() == allReplicas.size())
//            {
//                // CL.ALL, RRD.GLOBAL or RRD.DC_LOCAL and a single-DC.
//                // We are going to contact every node anyway, so ask for 2 full data requests instead of 1, for redundancy
//                // (same amount of requests in total, but we turn 1 digest request into a full blown data request).
//                return new AlwaysSpeculatingReadExecutor(keyspace, cfs, command, consistencyLevel, targetReplicas);
//            }

            // RRD.NONE or RRD.DC_LOCAL w/ multiple DCs.
//            InetAddress extraReplica = allReplicas.get(targetReplicas.size());
            // With repair decision DC_LOCAL all replicas/target replicas may be in different order, so
            // we might have to find a replacement that's not already in targetReplicas.
//            if (repairDecision == ReadRepairDecision.DC_LOCAL && targetReplicas.contains(extraReplica))
//            {
//                for (InetAddress address : allReplicas)
//                {
//                    if (!targetReplicas.contains(address))
//                    {
//                        extraReplica = address;
//                        break;
//                    }
//                }
//            }
//            targetReplicas.add(extraReplica);

//            if (retry.equals(SpeculativeRetryParam.ALWAYS))
//                return new AlwaysSpeculatingReadExecutor(keyspace, cfs, command, consistencyLevel, targetReplicas);
//            else // PERCENTILE or CUSTOM.
//                return new SpeculatingReadExecutor(keyspace, cfs, command, consistencyLevel, targetReplicas);
        }
    }

    private static class DumbExecutor extends ReadTransactionDataExecutor {

        protected DumbExecutor(SingleReadRequestRecipe readRequestRecipe, ConsistencyLevel consistencyLevel, List<InetAddress> targetReplicas)
        {
            super(readRequestRecipe, consistencyLevel, targetReplicas);
        }

        public void maybeTryAdditionalReplicas()
        {
            // Does nothing
        }

        public void executeAsync()
        {
            final PrivateMemtableReadCommand command = readRequestRecipe.createCommand();
            makeRequests(command, targetReplicas);
        }
    }
//
//    final List<PartitionUpdate> partitionUpdates = privateMemtableReadResponses.stream()
//                                                                               .filter(Optional::isPresent)
//                                                                               .map(Optional::get)
//                                                                               .collect(Collectors.toList());


    /**
     *  It will use simplest possible strategy and send message to every other live host.
     *  TODO MPP actually at the moment it sends only to natural live endpoints that are required to get quorum.
     */
    public List<PartitionUpdate> executeRecipe(SingleReadRequestRecipe singleReadRequestRecipe, ConsistencyLevel consistencyLevel)
    {
        final ReadTransactionDataExecutor readExecutor = ReadTransactionDataExecutor.getReadExecutor(singleReadRequestRecipe, consistencyLevel);

        // Sends to all, because it uses DumbExecutor
        readExecutor.executeAsync();

        // TODO [MPP] there could be an lifecycle object wrapping executor that could call these methods: executeAsync, tryAdditionalReplicas etc.

        final List<PartitionUpdate> partitionUpdates = readExecutor.get();

        return partitionUpdates;
    }


}
