/*
 * Copyright (c) 2025, WSO2 LLC. (http://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
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

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BStream;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
import io.ballerina.stdlib.sql.datasource.SQLDatasource;
import io.ballerina.stdlib.sql.nativeimpl.CallProcessor;
import io.ballerina.stdlib.sql.nativeimpl.ClientProcessor;
import io.ballerina.stdlib.sql.nativeimpl.ExecuteProcessor;
import io.ballerina.stdlib.sql.nativeimpl.QueryProcessor;
import io.ballerina.stdlib.sql.parameterprocessor.DefaultResultParameterProcessor;
import io.ballerina.stdlib.sql.parameterprocessor.DefaultStatementParameterProcessor;

public final class TestUtils {
    private TestUtils() {
    }

    public static Object nativeCall(Environment env, BObject client, BObject paramSQLString, BArray recordTypes) {
        return CallProcessor.nativeCall(env, client, paramSQLString, recordTypes,
                DefaultStatementParameterProcessor.getInstance(), DefaultResultParameterProcessor.getInstance());
    }

    public static Object createSqlClient(BObject client, BMap<BString, Object> sqlDatasourceParams,
                                         BMap<BString, Object> globalConnectionPool) {
        return ClientProcessor.createClient(client,
                SQLDatasource.createSQLDatasourceParams(sqlDatasourceParams, globalConnectionPool), true, true);
    }

    public static Object close(BObject client) {
        return ClientProcessor.close(client);
    }

    public static Object nativeExecute(Environment env, BObject client, BObject paramSQLString) {
        return ExecuteProcessor.nativeExecute(env, client, paramSQLString,
                DefaultStatementParameterProcessor.getInstance());
    }

    public static Object nativeBatchExecute(Environment env, BObject client, BArray paramSQLStrings) {
        return ExecuteProcessor.nativeBatchExecute(env, client, paramSQLStrings,
                DefaultStatementParameterProcessor.getInstance());
    }

    public static BStream nativeQuery(Environment environment, BObject client, BObject paramSQLString,
                                      BTypedesc recordType) {
        DefaultStatementParameterProcessor statementParametersProcessor = DefaultStatementParameterProcessor
                .getInstance();
        DefaultResultParameterProcessor resultParametersProcessor = DefaultResultParameterProcessor
                .getInstance();
        return QueryProcessor.nativeQuery(environment, client, paramSQLString, recordType, statementParametersProcessor,
                resultParametersProcessor);
    }

    public static Object nativeQueryRow(Environment env, BObject client, BObject paramSQLString, BTypedesc recordType) {
        DefaultStatementParameterProcessor statementParametersProcessor = DefaultStatementParameterProcessor
                .getInstance();
        DefaultResultParameterProcessor resultParametersProcessor = DefaultResultParameterProcessor
                .getInstance();
        return QueryProcessor.nativeQueryRow(env, client, paramSQLString, recordType, statementParametersProcessor,
                resultParametersProcessor);
    }
}
