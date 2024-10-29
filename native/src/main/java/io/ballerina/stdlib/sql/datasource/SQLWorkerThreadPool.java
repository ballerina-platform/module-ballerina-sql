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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static io.ballerina.stdlib.sql.Constants.BALLERINA_SQL_MAX_POOL_SIZE;

/**
 * Class to hold the static cachedThreadPool for SQL.
 */
public class SQLWorkerThreadPool {

    private SQLWorkerThreadPool() {
    }

    static final int MAX_POOL_SIZE = Integer.parseInt(
            System.getenv(BALLERINA_SQL_MAX_POOL_SIZE) != null ?
                    System.getenv(BALLERINA_SQL_MAX_POOL_SIZE) : "50"
    );

    // This is similar to cachedThreadPool util from Executors.newCachedThreadPool(..); but with upper cap on threads
    public static final ExecutorService SQL_EXECUTOR_SERVICE = new ThreadPoolExecutor(0, MAX_POOL_SIZE,
            60L, TimeUnit.SECONDS, new BlockingTaskQueue(), new SQLThreadFactory(),
            new RetryTaskRejectionPolicy());

    static class SQLThreadFactory implements ThreadFactory {
        @Override
        public Thread newThread(Runnable r) {
            Thread ballerinaSql = new Thread(r);
            ballerinaSql.setName("bal-sql-thread");
            return ballerinaSql;
        }
    }


    static class BlockingTaskQueue extends LinkedBlockingQueue<Runnable> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean offer(Runnable task) {
            // By returning false, we signal the ThreadPoolExecutor to bypass this queue and attempt to
            // spawn a new thread if it hasn't reached the maximum pool size. This approach favors creating
            // new threads over queuing tasks, thereby enabling more aggressive parallelism.
            return false;
        }

        public void retryTask(Runnable task) {
            if (!super.offer(task)) {
                throw new IllegalStateException("Failed to requeue task: " + task);
            }
        }
    }

    static class RetryTaskRejectionPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
            if (executor.getQueue() instanceof BlockingTaskQueue cbq) {
                cbq.retryTask(task);
            }
        }
    }
}
