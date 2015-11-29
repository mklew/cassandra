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

import org.apache.cassandra.mpp.transaction.internal.PrivateMemtableStorageImpl;

/**
 * Access through locator allows to break static link and ability to introduce some checks during tests.
 *
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 29/11/15
 */
public class PrivateMemtableStorageLocator
{
    private final PrivateMemtableStorage realStorage;

    private PrivateMemtableStorage fakeStorage;

    private PrivateMemtableStorageLocator()
    {
        realStorage = new PrivateMemtableStorageImpl();
    }

    /**
     * Can be used for tests.
     */
    public void setFakePrivateMemtableStorage(PrivateMemtableStorage fakeStorage)
    {
        this.fakeStorage = fakeStorage;
    }

    public static final PrivateMemtableStorageLocator instance = new PrivateMemtableStorageLocator();

    public PrivateMemtableStorage getStorage()
    {
        if (fakeStorage == null)
        {
            return realStorage;
        }
        else
        {
            return fakeStorage;
        }
    }
}
