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

package org.apache.cassandra.service.mppaxos;

import java.io.IOException;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.mpp.transaction.client.TransactionState;
import org.apache.cassandra.mpp.transaction.serialization.TransactionStateSerializer;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 04/04/16
 */
public class MpPrePrepare
{
    TransactionState transactionState;

    public MpPrePrepare(TransactionState transactionState)
    {
        this.transactionState = transactionState;
    }

    public static final MpPrePrepareSerializer serializer = new MpPrePrepareSerializer();

    public static class MpPrePrepareSerializer implements IVersionedSerializer<MpPrePrepare> {

        public void serialize(MpPrePrepare mpPrePrepare, DataOutputPlus out, int version) throws IOException
        {
            TransactionStateSerializer.instance.serialize(mpPrePrepare.transactionState, out, version);
        }

        public MpPrePrepare deserialize(DataInputPlus in, int version) throws IOException
        {
            TransactionState txState = TransactionStateSerializer.instance.deserialize(in, version);
            return new MpPrePrepare(txState);
        }

        public long serializedSize(MpPrePrepare mpPrePrepare, int version)
        {
            return TransactionStateSerializer.instance.serializedSize(mpPrePrepare.transactionState, version);
        }
    }

    public TransactionState getTransactionState()
    {
        return transactionState;
    }
}
