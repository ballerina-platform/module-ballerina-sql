/*
 * Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.stdlib.sql.datasource;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Class to hold the static cachedThreadPool for SQL.
 */
public class SQLWorkerThreadPool {

    private SQLWorkerThreadPool(){
    }

    public static final ExecutorService SQL_EXECUTOR_SERVICE = Executors.newCachedThreadPool(new SQLThreadFactory());

    static class SQLThreadFactory implements ThreadFactory {
        @Override
        public Thread newThread(Runnable r) {
            Thread ballerinaSql = new Thread(r);
            ballerinaSql.setName("bal-sql-thread");
            return ballerinaSql;
        }
    }
}
