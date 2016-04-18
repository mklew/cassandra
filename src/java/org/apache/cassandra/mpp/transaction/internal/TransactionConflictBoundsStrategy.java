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

/**
 * Strategy is defined per Column Family.
 *
 *
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 18/04/16
 */
public enum TransactionConflictBoundsStrategy
{
    /**
     * Creates paxos participants per transaction item.
     * If two transactions share same transaction item then they have conflict.
     */
    COMMON_TX_ITEMS,
    /**
     * Token range is slices on N slices (configurable parameter).
     *
     * Transactions that have items belonging to same slice, are considered to have conflict.
     *
     * Implementation assumes Murmur3Partitioner
     */
    TOKEN_RANGE_SLICES,
    /**
     * Special case of TOKEN_RANGE_SLICES with single slice.
     */
    ONE_FOR_ALL
}
