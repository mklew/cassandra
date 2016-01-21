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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

/**
* @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
* @since 14/01/16
*/
class MppNettyClientHandler extends ChannelOutboundHandlerAdapter
{
    private static Logger logger = LoggerFactory.getLogger(MppNettyClientHandler.class);

    private CompletableFuture<Optional<Throwable>> successOrFailure;

    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
    {
        Preconditions.checkNotNull(successOrFailure);
        try
        {
            super.write(ctx, msg, promise);
            promise.addListener(new ChannelFutureListener()
            {
                public void operationComplete(ChannelFuture future) throws Exception
                {
                    if(!future.isSuccess())
                    {
                        successOrFailure.completeExceptionally(future.cause());
                        logger.error("MppNettyClientHandler write listener exception", future.cause());
                    }
                }
            });
        }
        catch (Exception e)
        {
            successOrFailure.completeExceptionally(e);
            logger.error("MppNettyClientHandler write exception", e);
        }
    }

    public void read(ChannelHandlerContext ctx) throws Exception
    {
        super.read(ctx);
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
    {
        logger.error("MppNettyClientHandler exceptionCaught", cause);
        successOrFailure.completeExceptionally(cause);
        super.exceptionCaught(ctx, cause);
    }

    public void setTryFuture(CompletableFuture<Optional<Throwable>> successOrFailure)
    {
        this.successOrFailure = successOrFailure;
    }
}
