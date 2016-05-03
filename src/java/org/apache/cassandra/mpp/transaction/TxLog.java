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

package org.apache.cassandra.mpp.transaction;

import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
* @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
* @since 03/05/16
*/
public enum TxLog
{
    ROLLED_BACK, COMMITTED, UNKNOWN;

    public static TxLogSerializer serializer = new TxLogSerializer();

    public static class TxLogSerializer implements IVersionedSerializer<TxLog> {

        public void serialize(TxLog txLog, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(txLog.ordinal());
        }

        public TxLog deserialize(DataInputPlus in, int version) throws IOException
        {
            int ordinal = in.readInt();
            TxLog[] values = TxLog.values();
            return values[ordinal];
        }

        public long serializedSize(TxLog txLog, int version)
        {
            return TypeSizes.sizeof(txLog.ordinal());
        }
    }
}
