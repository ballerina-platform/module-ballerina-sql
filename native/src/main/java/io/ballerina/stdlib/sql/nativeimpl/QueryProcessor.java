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
import io.ballerina.runtime.api.PredefinedTypes;
import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BStream;
import io.ballerina.runtime.api.values.BTypedesc;
import io.ballerina.runtime.transactions.TransactionResourceManager;
import io.ballerina.stdlib.sql.Constants;
import io.ballerina.stdlib.sql.ParameterizedQuery;
import io.ballerina.stdlib.sql.datasource.SQLDatasource;
import io.ballerina.stdlib.sql.exception.ApplicationError;
import io.ballerina.stdlib.sql.parameterprocessor.AbstractResultParameterProcessor;
import io.ballerina.stdlib.sql.parameterprocessor.AbstractStatementParameterProcessor;
import io.ballerina.stdlib.sql.utils.ColumnDefinition;
import io.ballerina.stdlib.sql.utils.ErrorGenerator;
import io.ballerina.stdlib.sql.utils.ModuleUtils;
import io.ballerina.stdlib.sql.utils.PrimitiveTypeColumnDefinition;
import io.ballerina.stdlib.sql.utils.Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static io.ballerina.stdlib.sql.datasource.SQLWorkerThreadPool.SQL_EXECUTOR_SERVICE;

/**
 * This class provides the query processing implementation which executes sql queries.
 *
 * @since 0.5.6
 */
public class QueryProcessor {

    private QueryProcessor() {
    }

    /**
     * Query the database and return results.
     *
     * @param client                      client object
     * @param paramSQLString              SQL string of the query
     * @param recordType                  type description of the result record
     * @param statementParameterProcessor pre-processor of the statement
     * @param resultParameterProcessor    post-processor of the result
     * @return result stream or error
     */
    public static BStream nativeQuery(
            Environment env, BObject client, BObject paramSQLString, Object recordType,
            AbstractStatementParameterProcessor statementParameterProcessor,
            AbstractResultParameterProcessor resultParameterProcessor) {
        TransactionResourceManager trxResourceManager = TransactionResourceManager.getInstance();
        if (!Utils.isWithinTrxBlock(trxResourceManager)) {
            Future balFuture = env.markAsync();
            SQL_EXECUTOR_SERVICE.execute(() -> {
                BStream resultStream =
                        nativeQueryExecutable(client, paramSQLString, recordType, statementParameterProcessor,
                                resultParameterProcessor, false, null);
                balFuture.complete(resultStream);
            });
        } else {
            return nativeQueryExecutable(client, paramSQLString, recordType, statementParameterProcessor,
                    resultParameterProcessor, true, trxResourceManager);
        }
        return null;
    }

    private static BStream nativeQueryExecutable(
            BObject client, BObject paramSQLString, Object recordType,
            AbstractStatementParameterProcessor statementParameterProcessor,
            AbstractResultParameterProcessor resultParameterProcessor, boolean isWithInTrxBlock,
            TransactionResourceManager trxResourceManager) {
        Object dbClient = client.getNativeData(Constants.DATABASE_CLIENT);
        if (dbClient != null) {
            SQLDatasource sqlDatasource = (SQLDatasource) dbClient;
            if (!((Boolean) client.getNativeData(Constants.DATABASE_CLIENT_ACTIVE_STATUS))) {
                BError errorValue = ErrorGenerator.getSQLApplicationError(
                        "SQL Client is already closed, hence further operations are not allowed");
                return getErrorStream(recordType, errorValue);
            }
            Connection connection = null;
            PreparedStatement statement = null;
            ResultSet resultSet = null;
            String sqlQuery = null;
            try {
                ParameterizedQuery parameterizedQuery = Utils.getParameterizedSQLQuery(paramSQLString);
                sqlQuery = parameterizedQuery.getSqlQuery();
                connection = SQLDatasource.getConnection(isWithInTrxBlock, trxResourceManager, client, sqlDatasource);
                statement = connection.prepareStatement(sqlQuery);
                statementParameterProcessor.setParams(connection, statement, parameterizedQuery.getInsertions());
                resultSet = statement.executeQuery();
                RecordType streamConstraint = (RecordType) ((BTypedesc) recordType).getDescribingType();
                List<ColumnDefinition> columnDefinitions = Utils.getColumnDefinitions(resultSet, streamConstraint);
                return ValueCreator.createStreamValue(TypeCreator.createStreamType(streamConstraint,
                        PredefinedTypes.TYPE_NULL), resultParameterProcessor
                        .createRecordIterator(resultSet, statement, connection, columnDefinitions, streamConstraint));
            } catch (SQLException e) {
                Utils.closeResources(isWithInTrxBlock, resultSet, statement, connection);
                BError errorValue = ErrorGenerator.getSQLDatabaseError(e,
                        String.format("Error while executing SQL query: %s. ", sqlQuery));
                return getErrorStream(recordType, errorValue);
            } catch (ApplicationError applicationError) {
                Utils.closeResources(isWithInTrxBlock, resultSet, statement, connection);
                BError errorValue = ErrorGenerator.getSQLApplicationError(applicationError);
                return getErrorStream(recordType, errorValue);
            } catch (Throwable e) {
                Utils.closeResources(isWithInTrxBlock, resultSet, statement, connection);
                String message = e.getMessage();
                if (message == null) {
                    message = e.getClass().getName();
                }
                BError errorValue = ErrorGenerator.getSQLApplicationError(
                        String.format("Error while executing SQL query: %s. %s", sqlQuery, message));
                return getErrorStream(recordType, errorValue);
            }
        } else {
            BError errorValue = ErrorGenerator.getSQLApplicationError("Client is not properly initialized!");
            return getErrorStream(recordType, errorValue);
        }
    }

    public static Object nativeQueryRow(Environment env, BObject client, BObject paramSQLString, BTypedesc bTypedesc,
                                        AbstractStatementParameterProcessor statementParameterProcessor,
                                        AbstractResultParameterProcessor resultParameterProcessor) {
        TransactionResourceManager trxResourceManager = TransactionResourceManager.getInstance();
        if (!Utils.isWithinTrxBlock(trxResourceManager)) {
            Future balFuture = env.markAsync();
            SQL_EXECUTOR_SERVICE.execute(() -> {
                Object resultStream =
                        nativeQueryRowExecutable(client, paramSQLString, bTypedesc, statementParameterProcessor,
                                resultParameterProcessor, false, null);
                balFuture.complete(resultStream);
            });
        } else {
            return nativeQueryRowExecutable(client, paramSQLString, bTypedesc, statementParameterProcessor,
                    resultParameterProcessor, true, trxResourceManager);
        }
        return null;
    }

    private static Object nativeQueryRowExecutable(
            BObject client, BObject paramSQLString, BTypedesc ballerinaType,
            AbstractStatementParameterProcessor statementParameterProcessor,
            AbstractResultParameterProcessor resultParameterProcessor, boolean isWithInTrxBlock,
            TransactionResourceManager trxResourceManager) {
        Type describingType = ballerinaType.getDescribingType();
        if (describingType.getTag() == TypeTags.UNION_TAG) {
            return ErrorGenerator.getSQLApplicationError("Return type cannot be a union of multiple types.");
        }

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
                ParameterizedQuery parameterizedQuery = Utils.getParameterizedSQLQuery(paramSQLString);
                sqlQuery = parameterizedQuery.getSqlQuery();
                connection = SQLDatasource.getConnection(isWithInTrxBlock, trxResourceManager, client, sqlDatasource);
                statement = connection.prepareStatement(sqlQuery);
                statementParameterProcessor.setParams(connection, statement, parameterizedQuery.getInsertions());
                resultSet = statement.executeQuery();
                if (!resultSet.next()) {
                    return ErrorGenerator.getNoRowsError("Query did not retrieve any rows.");
                }

                if (describingType.getTag() == TypeTags.RECORD_TYPE_TAG &&
                        !Utils.isSupportedRecordType(describingType)) {
                    RecordType recordConstraint = (RecordType) describingType;
                    List<ColumnDefinition> columnDefinitions = Utils.getColumnDefinitions(resultSet, recordConstraint);
                    return resultParameterProcessor.createRecord(resultSet, columnDefinitions, recordConstraint);
                } else {
                    if (resultSet.getMetaData().getColumnCount() > 1) {
                        return ErrorGenerator.getTypeMismatchError(
                                String.format("Expected type to be '%s' but found 'record{}'.", describingType));
                    }
                    PrimitiveTypeColumnDefinition definition = Utils.getColumnDefinition(resultSet, 1, describingType);
                    return resultParameterProcessor.createValue(resultSet, 1, definition);
                }
            } catch (SQLException e) {
                return ErrorGenerator.getSQLDatabaseError(e,
                        String.format("Error while executing SQL query: %s. ", sqlQuery));
            } catch (ApplicationError e) {
                return ErrorGenerator.getSQLApplicationError(e);
            } catch (Throwable e) {
                String message = e.getMessage();
                if (message == null) {
                    message = e.getClass().getName();
                }
                return ErrorGenerator.getSQLApplicationError(
                        String.format("Error while executing SQL query: %s. %s", sqlQuery, message));
            } finally {
                Utils.closeResources(isWithInTrxBlock, resultSet, statement, connection);
            }
        }
        return ErrorGenerator.getSQLApplicationError("Client is not properly initialized!");
    }

    private static BStream getErrorStream(Object recordType, BError errorValue) {
        return ValueCreator.createStreamValue(
                TypeCreator.createStreamType(((BTypedesc) recordType).getDescribingType(),
                        PredefinedTypes.TYPE_NULL), createRecordIterator(errorValue));
    }

    private static BObject createRecordIterator(BError errorValue) {
        return ValueCreator.createObjectValue(ModuleUtils.getModule(), Constants.RESULT_ITERATOR_OBJECT,
                errorValue, null);
    }

}
