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

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.Future;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.transactions.TransactionResourceManager;
import io.ballerina.stdlib.sql.Constants;
import io.ballerina.stdlib.sql.datasource.SQLDatasource;
import io.ballerina.stdlib.sql.exception.ApplicationError;
import io.ballerina.stdlib.sql.parameterprocessor.AbstractStatementParameterProcessor;
import io.ballerina.stdlib.sql.utils.ErrorGenerator;
import io.ballerina.stdlib.sql.utils.ModuleUtils;
import io.ballerina.stdlib.sql.utils.Utils;

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

import static io.ballerina.stdlib.sql.datasource.SQLWorkerThreadPool.SQL_EXECUTOR_SERVICE;
import static io.ballerina.stdlib.sql.utils.Utils.closeResources;
import static io.ballerina.stdlib.sql.utils.Utils.getGeneratedKeys;
import static io.ballerina.stdlib.sql.utils.Utils.getSqlQuery;


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
    public static Object nativeExecute(Environment env, BObject client, Object paramSQLString,
                                       AbstractStatementParameterProcessor statementParameterProcessor) {
        TransactionResourceManager trxResourceManager = TransactionResourceManager.getInstance();
        if (!Utils.isWithinTrxBlock(trxResourceManager)) {
            Future balFuture = env.markAsync();
            SQL_EXECUTOR_SERVICE.execute(()-> {
                Object resultStream =
                        nativeExecuteExecutable(client, paramSQLString, statementParameterProcessor, false, null);
                balFuture.complete(resultStream);
            });
        } else {
            return nativeExecuteExecutable(client, paramSQLString, statementParameterProcessor, true,
                    trxResourceManager);
        }
        return null;
    }

    private static Object nativeExecuteExecutable(BObject client, Object paramSQLString,
                                                 AbstractStatementParameterProcessor statementParameterProcessor,
                                                 boolean isWithInTrxBlock,
                                                 TransactionResourceManager trxResourceManager) {
        Object dbClient = client.getNativeData(Constants.DATABASE_CLIENT);
        if (dbClient != null) {
            SQLDatasource sqlDatasource = (SQLDatasource) dbClient;
            if (!((Boolean) client.getNativeData(Constants.DATABASE_CLIENT_ACTIVE_STATUS))) {
                return ErrorGenerator.getSQLApplicationError(
                        "SQL Client is already closed, hence further operations are not allowed");
            }
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
                connection = SQLDatasource.getConnection(isWithInTrxBlock, trxResourceManager, client, sqlDatasource);

                if (sqlDatasource.getExecuteGKFlag()) {
                    statement = connection.prepareStatement(sqlQuery, Statement.RETURN_GENERATED_KEYS);
                } else {
                    statement = connection.prepareStatement(sqlQuery);
                }

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
                        String.format("Error while executing SQL query: %s. ", sqlQuery));
            } catch (ApplicationError e) {
                return ErrorGenerator.getSQLApplicationError(e);
            } catch (Throwable th) {
                return ErrorGenerator.getSQLError(th, String.format("Error while executing SQL query: %s. ", sqlQuery));
            } finally {
                closeResources(isWithInTrxBlock, resultSet, statement, connection);
            }
        } else {
            return ErrorGenerator.getSQLApplicationError("Client is not properly initialized!");
        }
    }

    /**
     * Execute a batch of SQL statements.
     * @param client client object
     * @param paramSQLStrings array of SQL string for the execute statement
     * @param statementParameterProcessor pre-processor of the statement
     * @return execution result or error
     */
    public static Object nativeBatchExecute(Environment env, BObject client, BArray paramSQLStrings,
                                            AbstractStatementParameterProcessor statementParameterProcessor) {
        TransactionResourceManager trxResourceManager = TransactionResourceManager.getInstance();
        if (!Utils.isWithinTrxBlock(trxResourceManager)) {
            Future balFuture = env.markAsync();
            SQL_EXECUTOR_SERVICE.execute(()-> {
                Object resultStream =
                        nativeBatchExecuteExecutable(client, paramSQLStrings, statementParameterProcessor,
                                false, null);
                balFuture.complete(resultStream);
            });
        } else {
            return nativeBatchExecuteExecutable(client, paramSQLStrings, statementParameterProcessor,
                    true, trxResourceManager);
        }
        return null;
    }

    private static Object nativeBatchExecuteExecutable(BObject client, BArray paramSQLStrings,
                                            AbstractStatementParameterProcessor statementParameterProcessor,
                                            boolean isWithinTrxBlock, TransactionResourceManager trxResourceManager) {
        Object dbClient = client.getNativeData(Constants.DATABASE_CLIENT);
        if (dbClient != null) {
            SQLDatasource sqlDatasource = (SQLDatasource) dbClient;
            if (!((Boolean) client.getNativeData(Constants.DATABASE_CLIENT_ACTIVE_STATUS))) {
                return ErrorGenerator.getSQLApplicationError(
                        "SQL Client is already closed, hence further operations are not allowed");
            }
            Connection connection = null;
            PreparedStatement statement = null;
            ResultSet resultSet = null;
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
                connection = SQLDatasource.getConnection(isWithinTrxBlock, trxResourceManager, client, sqlDatasource);

                if (sqlDatasource.getBatchExecuteGKFlag()) {
                    statement = connection.prepareStatement(sqlQuery, Statement.RETURN_GENERATED_KEYS);
                } else {
                    statement = connection.prepareStatement(sqlQuery, Statement.NO_GENERATED_KEYS);
                }

                for (BObject param : parameters) {
                    statementParameterProcessor.setParams(connection, statement, param);
                    statement.addBatch();
                }

                int[] counts = statement.executeBatch();

                if (sqlDatasource.getBatchExecuteGKFlag() && !isDdlStatement(sqlQuery)) {
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
                        String.format("Error while executing batch command starting with: '%s'.", sqlQuery));
            } catch (SQLException e) {
                return ErrorGenerator.getSQLDatabaseError(e,
                        String.format("Error while executing batch command starting with: '%s'. ", sqlQuery));
            } catch (ApplicationError e) {
                return ErrorGenerator.getSQLApplicationError(e);
            } catch (Throwable th) {
                return ErrorGenerator.getSQLError(th,
                        String.format("Error while executing batch command starting with: '%s'. ", sqlQuery));
            } finally {
                closeResources(isWithinTrxBlock, resultSet, statement, connection);
            }
        } else {
            return ErrorGenerator.getSQLApplicationError("Client is not properly initialized!");
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
