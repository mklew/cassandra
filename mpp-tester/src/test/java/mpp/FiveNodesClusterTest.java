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

package mpp;

import java.util.stream.Stream;

import com.datastax.driver.core.Session;
import org.apache.cassandra.tools.NodeProbe;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 07/04/16
 */
public class FiveNodesClusterTest extends BaseClusterTest
{
    protected NodeProbe nodeProbe4;

    protected NodeProbe nodeProbe5;

    protected NodeProbe getNodeProbe4()
    {

        if (nodeProbe4 == null)
        {
            nodeProbe4 = initNodeProbe(7400);
            return nodeProbe4;
        }
        else
        {
            return nodeProbe4;
        }
    }

    protected NodeProbe getNodeProbe5()
    {

        if (nodeProbe5 == null)
        {
            nodeProbe5 = initNodeProbe(7500);
            return nodeProbe5;
        }
        else
        {
            return nodeProbe5;
        }
    }

    protected Session getSessionN4()
    {
        String address = "127.0.0.4";
        return getSessionWithMppTest(address);
    }

    protected Session getSessionN5()
    {
        String address = "127.0.0.5";
        return getSessionWithMppTest(address);
    }

    protected Stream<NodeProbe> getNodeProbesStream()
    {
        return Stream.concat(super.getNodeProbesStream(), Stream.of(getNodeProbe4(), getNodeProbe5()));
    }

    protected Stream<NamedNodeProbe> getNodeProbesNamedStream()
    {
        return Stream.concat(super.getNodeProbesNamedStream(), Stream.of(new NamedNodeProbe(getNodeProbe4(), "Node4"),
                                                                         new NamedNodeProbe(getNodeProbe5(), "Node5")));
    }
}
