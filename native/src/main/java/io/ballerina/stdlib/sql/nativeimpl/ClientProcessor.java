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

package io.ballerina.stdlib.sql.nativeimpl;

import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.stdlib.sql.Constants;
import io.ballerina.stdlib.sql.datasource.SQLDatasource;

import java.util.UUID;
import java.util.logging.LogManager;

/**
 * This class implements the utility methods for the clients to be used.
 *
 * @since 0.5.6
 */
public class ClientProcessor {

    private ClientProcessor() {
    }

    public static Object close(BObject client) {
        LogManager.getLogManager().reset();
        Object datasourceObj = client.getNativeData(Constants.DATABASE_CLIENT);
        // When an exception is thrown during database endpoint init (eg: driver not present) stop operation
        // of the endpoint is automatically called. But at this point, datasource is null therefore to handle that
        // situation following null check is needed.
        if (datasourceObj != null) {
            ((SQLDatasource) datasourceObj).decrementClientCounterAndAttemptPoolShutdown();
            client.addNativeData(Constants.DATABASE_CLIENT_ACTIVE_STATUS, Boolean.FALSE);
        }
        return null;
    }
    
    /**
     * Create the client used to connect with the database.
     * @param client client object
     * @param sqlDatasourceParams datasource parameters required to retrieve the JDBC URL for datasource lookup and
     *                            initialization of the newly created datasource if it doesn't exists
     * @return null if client is successfully created else error         
     */
    public static Object createClient(BObject client, SQLDatasource.SQLDatasourceParams sqlDatasourceParams,
                                      boolean executeGKFlag, boolean batchExecuteGKFlag) {
        try {
            LogManager.getLogManager().reset();
            SQLDatasource sqlDatasource = SQLDatasource.retrieveDatasource(sqlDatasourceParams, executeGKFlag,
                                                                           batchExecuteGKFlag);
            client.addNativeData(Constants.DATABASE_CLIENT, sqlDatasource);
            client.addNativeData(Constants.SQL_CONNECTOR_TRANSACTION_ID, UUID.randomUUID().toString());
            client.addNativeData(Constants.DATABASE_CLIENT_ACTIVE_STATUS, Boolean.TRUE);
            return null;
        } catch (BError errorValue) {
            return errorValue;
        }
    }
}
