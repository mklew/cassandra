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

package org.apache.cassandra.mpp.transaction.client.dto;

import java.io.Serializable;

/**
 * @author Marek Lewandowski <marek.m.lewandowski@gmail.com>
 * @since 27/01/16
 */
public class TransactionItemDto implements Serializable
{
    Long token;

    String ksName;

    String cfName;

    public Long getToken()
    {
        return token;
    }

    public void setToken(Long token)
    {
        this.token = token;
    }

    public String getKsName()
    {
        return ksName;
    }

    public void setKsName(String ksName)
    {
        this.ksName = ksName;
    }

    public String getCfName()
    {
        return cfName;
    }

    public void setCfName(String cfName)
    {
        this.cfName = cfName;
    }
}
