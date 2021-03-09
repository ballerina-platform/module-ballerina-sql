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

package org.ballerinalang.sql.nativeimpl;

import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.transactions.TransactionResourceManager;
import org.ballerinalang.sql.Constants;
import org.ballerinalang.sql.datasource.SQLDatasource;
import org.ballerinalang.sql.exception.ApplicationError;
import org.ballerinalang.sql.parameterprocessor.DefaultStatementParameterProcessor;
import org.ballerinalang.sql.utils.ErrorGenerator;
import org.ballerinalang.sql.utils.ModuleUtils;

import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.ballerinalang.sql.utils.Utils.closeResources;
import static org.ballerinalang.sql.utils.Utils.getGeneratedKeys;
import static org.ballerinalang.sql.utils.Utils.getSqlQuery;


/**
 * This class contains methods for executing SQL queries.
 *
 * @since 0.5.6
 */
public class ExecuteProcessor {
    private ExecuteProcessor() {
    }

    /**
     * Execute an SQL statement.
     * @param client client object
     * @param paramSQLString array of SQL string for the execute statement
     * @param statementParameterProcessor pre-processor of the statement
     * @return execution result or error
     */
    public static Object nativeExecute(BObject client, Object paramSQLString,
                     DefaultStatementParameterProcessor statementParameterProcessor) {
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
                statement = connection.prepareStatement(sqlQuery, Statement.RETURN_GENERATED_KEYS);
                if (paramSQLString instanceof BObject) {
                    statementParameterProcessor.setParams(connection, statement, (BObject) paramSQLString);
                }
                int count = statement.executeUpdate();
                Object lastInsertedId = null;
                if (!isDdlStatement(sqlQuery)) {
                    resultSet = statement.getGeneratedKeys();
                    if (resultSet.next()) {
                        lastInsertedId = getGeneratedKeys(resultSet);
                    }
                }
                Map<String, Object> resultFields = new HashMap<>();
                resultFields.put(Constants.AFFECTED_ROW_COUNT_FIELD, count);
                resultFields.put(Constants.LAST_INSERTED_ID_FIELD, lastInsertedId);
                return ValueCreator.createRecordValue(ModuleUtils.getModule(),
                        Constants.EXECUTION_RESULT_RECORD, resultFields);
            } catch (SQLException e) {
                return ErrorGenerator.getSQLDatabaseError(e,
                        "Error while executing SQL query: " + sqlQuery + ". ");
            } catch (ApplicationError | IOException e) {
                return ErrorGenerator.getSQLApplicationError("Error while executing SQL query: "
                        + sqlQuery + ". " + e.getMessage());
            } finally {
                closeResources(trxResourceManager, resultSet, statement, connection);
            }
        } else {
            return ErrorGenerator.getSQLApplicationError(
                    "Client is not properly initialized!");
        }
    }

    /**
     * Execute a batch of SQL statements.
     * @param client client object
     * @param paramSQLStrings array of SQL string for the execute statement
     * @param statementParameterProcessor pre-processor of the statement
     * @return execution result or error
     */
    public static Object nativeBatchExecute(BObject client, BArray paramSQLStrings,
                             DefaultStatementParameterProcessor statementParameterProcessor) {
        Object dbClient = client.getNativeData(Constants.DATABASE_CLIENT);
        if (dbClient != null) {
            SQLDatasource sqlDatasource = (SQLDatasource) dbClient;
            Connection connection = null;
            PreparedStatement statement = null;
            ResultSet resultSet = null;
            TransactionResourceManager trxResourceManager = TransactionResourceManager.getInstance();
            String sqlQuery = null;
            List<BObject> parameters = new ArrayList<>();
            List<BMap<BString, Object>> executionResults = new ArrayList<>();
            try {
                Object[] paramSQLObjects = paramSQLStrings.getValues();
                BObject parameterizedQuery = (BObject) paramSQLObjects[0];
                sqlQuery = getSqlQuery(parameterizedQuery);
                parameters.add(parameterizedQuery);
                for (int i = 1; i < paramSQLStrings.size(); i++) {
                    parameterizedQuery = (BObject) paramSQLObjects[i];
                    String paramSQLQuery = getSqlQuery(parameterizedQuery);

                    if (sqlQuery.equals(paramSQLQuery)) {
                        parameters.add(parameterizedQuery);
                    } else {
                        return ErrorGenerator.getSQLApplicationError("Batch Execute cannot contain different SQL " +
                                "commands. These has to be executed in different function calls");
                    }
                }
                connection = SQLDatasource.getConnection(trxResourceManager, client, sqlDatasource);
                statement = connection.prepareStatement(sqlQuery, Statement.RETURN_GENERATED_KEYS);
                for (BObject param : parameters) {
                    statementParameterProcessor.setParams(connection, statement, param);
                    statement.addBatch();
                }
                int[] counts = statement.executeBatch();

                if (!isDdlStatement(sqlQuery)) {
                    resultSet = statement.getGeneratedKeys();
                }
                for (int count : counts) {
                    Map<String, Object> resultField = new HashMap<>();
                    resultField.put(Constants.AFFECTED_ROW_COUNT_FIELD, count);
                    Object lastInsertedId = null;
                    if (resultSet != null && resultSet.next()) {
                        lastInsertedId = getGeneratedKeys(resultSet);
                    }
                    resultField.put(Constants.LAST_INSERTED_ID_FIELD, lastInsertedId);
                    executionResults.add(ValueCreator.createRecordValue(ModuleUtils.getModule(),
                            Constants.EXECUTION_RESULT_RECORD, resultField));
                }
                return ValueCreator.createArrayValue(executionResults.toArray(), TypeCreator.createArrayType(
                        TypeCreator.createRecordType(
                                Constants.EXECUTION_RESULT_RECORD, ModuleUtils.getModule(), 0, false, 0)));
            } catch (BatchUpdateException e) {
                int[] updateCounts = e.getUpdateCounts();
                for (int count : updateCounts) {
                    Map<String, Object> resultField = new HashMap<>();
                    resultField.put(Constants.AFFECTED_ROW_COUNT_FIELD, count);
                    resultField.put(Constants.LAST_INSERTED_ID_FIELD, null);
                    executionResults.add(ValueCreator.createRecordValue(ModuleUtils.getModule(),
                            Constants.EXECUTION_RESULT_RECORD, resultField));
                }
                return ErrorGenerator.getSQLBatchExecuteError(e, executionResults,
                        "Error while executing batch command starting with: '" + sqlQuery + "'.");
            } catch (SQLException e) {
                return ErrorGenerator.getSQLDatabaseError(e, "Error while executing SQL batch " +
                        "command starting with : " + sqlQuery + ". ");
            } catch (ApplicationError | IOException e) {
                return ErrorGenerator.getSQLApplicationError("Error while executing SQL query: "
                        + e.getMessage());
            } finally {
                closeResources(trxResourceManager, resultSet, statement, connection);
            }
        } else {
            return ErrorGenerator.getSQLApplicationError(
                    "Client is not properly initialized!");
        }
    }

    private static boolean isDdlStatement(String query) {
        String upperCaseQuery = query.trim().toUpperCase(Locale.ENGLISH);
        return Arrays.stream(DdlKeyword.values()).anyMatch(ddlKeyword -> upperCaseQuery.startsWith(ddlKeyword.name()));
    }

    private enum DdlKeyword {
        CREATE, ALTER, DROP, TRUNCATE, COMMENT, RENAME
    }
}
