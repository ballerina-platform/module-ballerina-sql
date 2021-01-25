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

import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.StructureType;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BStream;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
import io.ballerina.runtime.transactions.TransactionResourceManager;
import org.ballerinalang.sql.Constants;
import org.ballerinalang.sql.datasource.SQLDatasource;
import org.ballerinalang.sql.exception.ApplicationError;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.ballerinalang.sql.utils.Utils.closeResources;
import static org.ballerinalang.sql.utils.Utils.getColumnDefinitions;
import static org.ballerinalang.sql.utils.Utils.getDefaultRecordType;
import static org.ballerinalang.sql.utils.Utils.getDefaultStreamConstraint;
import static org.ballerinalang.sql.utils.Utils.getSqlQuery;
import static org.ballerinalang.sql.utils.Utils.setParams;

/**
 * This class provides the util implementation which executes sql queries.
 *
 * @since 1.2.0
 */
public class QueryUtils {

    public static BStream nativeQuery(BObject client, Object paramSQLString,
                                      Object recordType) {
        Object dbClient = client.getNativeData(Constants.DATABASE_CLIENT);
        TransactionResourceManager trxResourceManager = TransactionResourceManager.getInstance();
        if (dbClient != null) {
            SQLDatasource sqlDatasource = (SQLDatasource) dbClient;
            Connection connection = null;
            PreparedStatement statement = null;
            ResultSet resultSet = null;
            String sqlQuery = null;
            try {
                if (paramSQLString instanceof BString) {
                    sqlQuery = ((BString) paramSQLString).getValue();
                } else {
                    sqlQuery = getSqlQuery((BObject) paramSQLString);
                }
                connection = SQLDatasource.getConnection(trxResourceManager, client, sqlDatasource);
                statement = connection.prepareStatement(sqlQuery);
                if (paramSQLString instanceof BObject) {
                    setParams(connection, statement, (BObject) paramSQLString);
                }
                resultSet = statement.executeQuery();
                List<ColumnDefinition> columnDefinitions;
                StructureType streamConstraint;
                if (recordType == null) {
                    columnDefinitions = getColumnDefinitions(resultSet, null);
                    streamConstraint = getDefaultRecordType(columnDefinitions);
                } else {
                    streamConstraint = (StructureType) ((BTypedesc) recordType).getDescribingType();
                    columnDefinitions = getColumnDefinitions(resultSet, streamConstraint);
                }
                return ValueCreator.createStreamValue(TypeCreator.createStreamType(streamConstraint),
                        Utils.createRecordIterator(resultSet, statement, connection, columnDefinitions,
                                streamConstraint));
            } catch (SQLException e) {
                closeResources(trxResourceManager, resultSet, statement, connection);
                BError errorValue = ErrorGenerator.getSQLDatabaseError(e,
                        "Error while executing SQL query: " + sqlQuery + ". ");
                return ValueCreator.createStreamValue(TypeCreator.createStreamType(getDefaultStreamConstraint()),
                        createRecordIterator(errorValue));
            } catch (ApplicationError applicationError) {
                closeResources(trxResourceManager, resultSet, statement, connection);
                BError errorValue = ErrorGenerator.getSQLApplicationError(applicationError.getMessage());
                return getErrorStream(recordType, errorValue);
            } catch (Throwable e) {
                closeResources(trxResourceManager, resultSet, statement, connection);
                String message = e.getMessage();
                if (message == null) {
                    message = e.getClass().getName();
                }
                BError errorValue = ErrorGenerator.getSQLApplicationError(
                        "Error while executing SQL query: " + sqlQuery + ". " + message);
                return getErrorStream(recordType, errorValue);
            }
        } else {
            BError errorValue = ErrorGenerator.getSQLApplicationError("Client is not properly initialized!");
            return getErrorStream(recordType, errorValue);
        }
    }

    private static BStream getErrorStream(Object recordType, BError errorValue) {
        if (recordType == null) {
            return ValueCreator.createStreamValue(
                    TypeCreator.createStreamType(getDefaultStreamConstraint()), createRecordIterator(errorValue));
        } else {
            return ValueCreator.createStreamValue(
                    TypeCreator.createStreamType(((BTypedesc) recordType).getDescribingType()),
                    createRecordIterator(errorValue));
        }
    }

    private static BObject createRecordIterator(BError errorValue) {
        return ValueCreator.createObjectValue(ModuleUtils.getModule(), Constants.RESULT_ITERATOR_OBJECT,
                errorValue);
    }

}
