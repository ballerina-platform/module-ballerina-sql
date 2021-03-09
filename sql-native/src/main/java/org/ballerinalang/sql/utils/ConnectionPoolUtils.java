/*
 *  Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.ballerinalang.sql.utils;

import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import org.ballerinalang.sql.datasource.SQLDatasource;

import java.util.concurrent.ConcurrentHashMap;

/**
 * This is the util class for handing connection pool.
 *
 * @since 1.2.0
 */
public class ConnectionPoolUtils {

    public static void initGlobalPoolContainer(BMap<BString, Object> poolConfig) {
        SQLDatasource.putDatasourceContainer(poolConfig, new ConcurrentHashMap<>());
    }
}
