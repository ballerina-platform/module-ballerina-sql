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

package io.ballerina.stdlib.sql.testutils;

import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.sql.datasource.SQLDatasource;
import io.ballerina.stdlib.sql.nativeimpl.ClientProcessor;

/**
 * Initialize test client.
 */
public class ClientTestUtils {
    
    private ClientTestUtils() {
    }

    public static Object createSqlClient(BObject client, BMap<BString, Object> sqlDatasourceParams,
                                         BMap<BString, Object> globalConnectionPool) {
        return ClientProcessor.createClient(client,
                SQLDatasource.createSQLDatasourceParams(sqlDatasourceParams, globalConnectionPool), true, true);
    }

    public static Object close(BObject client) {
        return ClientProcessor.close(client);
    }
}
