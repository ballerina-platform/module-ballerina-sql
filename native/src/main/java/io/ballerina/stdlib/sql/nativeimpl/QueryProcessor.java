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
import io.ballerina.runtime.api.PredefinedTypes;
import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.UnionType;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BStream;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
import io.ballerina.runtime.transactions.TransactionLocalContext;
import io.ballerina.runtime.transactions.TransactionResourceManager;
import io.ballerina.stdlib.sql.Constants;
import io.ballerina.stdlib.sql.datasource.SQLDatasource;
import io.ballerina.stdlib.sql.exception.ApplicationError;
import io.ballerina.stdlib.sql.exception.DataError;
import io.ballerina.stdlib.sql.exception.TypeMismatchError;
import io.ballerina.stdlib.sql.parameterprocessor.AbstractResultParameterProcessor;
import io.ballerina.stdlib.sql.parameterprocessor.AbstractStatementParameterProcessor;
import io.ballerina.stdlib.sql.utils.ColumnDefinition;
import io.ballerina.stdlib.sql.utils.ErrorGenerator;
import io.ballerina.stdlib.sql.utils.PrimitiveTypeColumnDefinition;
import io.ballerina.stdlib.sql.utils.Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static io.ballerina.stdlib.sql.utils.Utils.getErrorStream;

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
    public static BStream nativeQuery(Environment env, BObject client, BObject paramSQLString, Object recordType,
                                      AbstractStatementParameterProcessor statementParameterProcessor,
                                      AbstractResultParameterProcessor resultParameterProcessor) {
        TransactionResourceManager trxResourceManager = TransactionResourceManager.getInstance();
        boolean withinTrxBlock = Utils.isWithinTrxBlock(trxResourceManager);
        boolean trxManagerEnabled = trxResourceManager.getTransactionManagerEnabled();
        TransactionLocalContext currentTrxContext = trxResourceManager.getCurrentTransactionContext();
        return env.yieldAndRun(() -> nativeQueryExecutable(client, paramSQLString, recordType,
                statementParameterProcessor, resultParameterProcessor, withinTrxBlock,
                currentTrxContext, trxManagerEnabled));
    }

    private static BStream nativeQueryExecutable(BObject client, BObject paramSQLString, Object recordType,
                                                 AbstractStatementParameterProcessor statementParameterProcessor,
                                                 AbstractResultParameterProcessor resultParameterProcessor,
                                                 boolean isWithInTrxBlock, TransactionLocalContext currentTrxContext,
                                                 boolean trxManagerEnabled) {
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
                sqlQuery = Utils.getSqlQuery(paramSQLString);
                connection = SQLDatasource.getConnection(isWithInTrxBlock, client, sqlDatasource,
                        currentTrxContext, trxManagerEnabled);
                statement = connection.prepareStatement(sqlQuery);
                statementParameterProcessor.setParams(connection, statement, paramSQLString);
                resultSet = statement.executeQuery();
                RecordType streamConstraint = (RecordType) TypeUtils.getReferredType(
                        ((BTypedesc) recordType).getDescribingType());
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
        boolean withinTrxBlock = Utils.isWithinTrxBlock(trxResourceManager);
        boolean trxManagerEnabled = trxResourceManager.getTransactionManagerEnabled();
        TransactionLocalContext currentTrxContext = trxResourceManager.getCurrentTransactionContext();
        return env.yieldAndRun(() -> nativeQueryRowExecutable(client, paramSQLString, bTypedesc,
                statementParameterProcessor, resultParameterProcessor, withinTrxBlock,
                currentTrxContext, trxManagerEnabled));
    }

    private static Object nativeQueryRowExecutable(
            BObject client, BObject paramSQLString,
            BTypedesc ballerinaType,
            AbstractStatementParameterProcessor statementParameterProcessor,
            AbstractResultParameterProcessor resultParameterProcessor, boolean isWithInTrxBlock,
            TransactionLocalContext currentTrxContext, boolean trxManagerEnabled) {
        Type describingType = TypeUtils.getReferredType(ballerinaType.getDescribingType());
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
                sqlQuery = Utils.getSqlQuery(paramSQLString);
                connection = SQLDatasource.getConnection(isWithInTrxBlock, client, sqlDatasource,
                        currentTrxContext, trxManagerEnabled);
                statement = connection.prepareStatement(sqlQuery);
                statementParameterProcessor.setParams(connection, statement, paramSQLString);
                resultSet = statement.executeQuery();
                if (!resultSet.next()) {
                    return ErrorGenerator.getNoRowsError("Query did not retrieve any rows.");
                }

                if (describingType.getTag() == TypeTags.UNION_TAG) {
                    return getUnionTypeBValue((UnionType) describingType, resultSet, resultParameterProcessor);
                }

                // Return-type is either a record or a primitive
                return getRecordOrPrimitiveTypeBValue(describingType, resultSet, resultParameterProcessor);
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

    // This method iterates through each type in the union type and checks whether it is compatible with the result
    // from the query.
    private static Object getUnionTypeBValue(
            UnionType describingType, ResultSet resultSet, AbstractResultParameterProcessor resultParameterProcessor)
            throws SQLException, TypeMismatchError {
        for (Type type: describingType.getMemberTypes()) {
            try {
                Type referredType = TypeUtils.getReferredType(type);
                // If one of the types inside the union is a union, recursively check
                if (referredType.getTag() == TypeTags.UNION_TAG) {
                    return getUnionTypeBValue(describingType, resultSet, resultParameterProcessor);
                }

                // Attempt to convert the query result to the current type
                return getRecordOrPrimitiveTypeBValue(referredType, resultSet, resultParameterProcessor);
            } catch (ApplicationError e) {
                // Ignored
                // If an ApplicationError is thrown, the type is not compatible with the query result. Hence, it is
                // ignored and the next type conversion is attempted.
            }
        }

        // No valid mapping was found in the union-type
        throw new TypeMismatchError(String.format(
                "The result generated from the query cannot be mapped to type %s.", describingType));
    }

    private static Object getRecordOrPrimitiveTypeBValue(
            Type type, ResultSet resultSet, AbstractResultParameterProcessor resultParameterProcessor)
            throws SQLException, ApplicationError {
        if (type.getTag() == TypeTags.RECORD_TYPE_TAG && !Utils.isSupportedRecordType(type)) {
            RecordType recordConstraint = (RecordType) type;
            List<ColumnDefinition> columnDefinitions = Utils.getColumnDefinitions(resultSet, recordConstraint);
            return createRecord(resultSet, columnDefinitions, recordConstraint, resultParameterProcessor);
        }

        if (resultSet.getMetaData().getColumnCount() > 1) {
            throw new TypeMismatchError(String.format("Expected type to be '%s' but found 'record{}'.", type));
        }

        PrimitiveTypeColumnDefinition definition = Utils.getColumnDefinition(resultSet, 1, type);
        return createValue(resultSet, 1, definition, resultParameterProcessor);
    }

    public static BMap<BString, Object> createRecord(ResultSet resultSet, List<ColumnDefinition> columnDefinitions,
                                                     RecordType recordConstraint,
                                                     AbstractResultParameterProcessor resultParameterProcessor)
            throws SQLException, DataError {
        return Utils.createBallerinaRecord(recordConstraint, resultParameterProcessor, resultSet,
                  columnDefinitions);
    }

    public static Object createValue(ResultSet resultSet, int columnIndex,
                                     PrimitiveTypeColumnDefinition columnDefinition,
                                     AbstractResultParameterProcessor resultParameterProcessor)
            throws SQLException, DataError {
        return Utils.getResult(resultSet, columnIndex, columnDefinition, resultParameterProcessor);
    }


}
