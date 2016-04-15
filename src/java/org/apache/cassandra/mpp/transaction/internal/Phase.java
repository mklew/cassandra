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
* @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
* @since 07/04/16
*/
public enum Phase
{
    NO_PHASE,
    ROLLBACK_PHASE,
    PRE_PREPARE_PHASE,
    BEGIN_AND_REPAIR_PHASE,
    PREPARE_PHASE,
    PROPOSE_PHASE,
    COMMIT_PHASE,
    AFTER_COMMIT_PHASE;
}
