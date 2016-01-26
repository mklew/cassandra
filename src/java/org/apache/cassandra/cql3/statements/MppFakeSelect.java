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

package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 26/01/16
 */
public class MppFakeSelect extends SelectStatement
{
    private static final Parameters defaultParameters = new Parameters(Collections.<ColumnIdentifier.Raw, Boolean>emptyMap(), false, false, false);

    public MppFakeSelect(CFMetaData cfm, Selection selection, StatementRestrictions restrictions, boolean isReversed, Comparator<List<ByteBuffer>> orderingComparator, Term limit)
    {
        super(cfm, 0, defaultParameters, selection, restrictions, isReversed, orderingComparator, limit);
    }

    public static MppFakeSelect create(CFMetaData cfm) {
        Selection selection = Selection.wildcard(cfm);
        StatementRestrictions restrictions = StatementRestrictions.empty(StatementType.SELECT, cfm);
        boolean isReversed = false;
        return new MppFakeSelect(cfm, selection, restrictions, isReversed, null, null);
    }

    public ResultMessage createResultMessage(PartitionIterator partitionIterator) {
        final ResultSet results = createResults(partitionIterator);
        return new ResultMessage.Rows(results);
    }


    public ResultSet createResults(PartitionIterator partitionIterator) {
        final int nowInSeconds = FBUtilities.nowInSeconds();
        return process(partitionIterator, QueryOptions.DEFAULT_TRANSACTIONAL, nowInSeconds);
    }

}
