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

import io.ballerina.runtime.api.PredefinedTypes;
import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BStream;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
import io.ballerina.runtime.transactions.TransactionResourceManager;
import io.ballerina.stdlib.sql.Constants;
import io.ballerina.stdlib.sql.datasource.SQLDatasource;
import io.ballerina.stdlib.sql.exception.ApplicationError;
import io.ballerina.stdlib.sql.parameterprocessor.AbstractResultParameterProcessor;
import io.ballerina.stdlib.sql.parameterprocessor.AbstractStatementParameterProcessor;
import io.ballerina.stdlib.sql.utils.ColumnDefinition;
import io.ballerina.stdlib.sql.utils.ErrorGenerator;
import io.ballerina.stdlib.sql.utils.ModuleUtils;
import io.ballerina.stdlib.sql.utils.Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

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
     * @param client client object
     * @param paramSQLString SQL string of the query
     * @param recordType type description of the result record       
     * @param statementParameterProcessor pre-processor of the statement
     * @param resultParameterProcessor post-processor of the result
     * @return result stream or error
     */
    public static BStream nativeQuery(
            BObject client, Object paramSQLString,
            Object recordType,
            AbstractStatementParameterProcessor statementParameterProcessor,
            AbstractResultParameterProcessor resultParameterProcessor) {
        Object dbClient = client.getNativeData(Constants.DATABASE_CLIENT);
        TransactionResourceManager trxResourceManager = TransactionResourceManager.getInstance();
        if (dbClient != null) {
            SQLDatasource sqlDatasource = (SQLDatasource) dbClient;
            if (!((Boolean) client.getNativeData(Constants.DATABASE_CLIENT_ACTIVE_STATUS))) {
                BError errorValue = ErrorGenerator.getSQLApplicationError("SQL Client is already closed, hence " +
                        "further operations are not allowed");
                return getErrorStream(recordType, errorValue);
            }
            Connection connection = null;
            PreparedStatement statement = null;
            ResultSet resultSet = null;
            String sqlQuery = null;
            try {
                if (paramSQLString instanceof BString) {
                    sqlQuery = ((BString) paramSQLString).getValue();
                } else {
                    sqlQuery = Utils.getSqlQuery((BObject) paramSQLString);
                }
                connection = SQLDatasource.getConnection(trxResourceManager, client, sqlDatasource);
                statement = connection.prepareStatement(sqlQuery);
                if (paramSQLString instanceof BObject) {
                    statementParameterProcessor.setParams(connection, statement, (BObject) paramSQLString);
                }
                resultSet = statement.executeQuery();
                RecordType streamConstraint = (RecordType) ((BTypedesc) recordType).getDescribingType();
                List<ColumnDefinition> columnDefinitions = Utils.getColumnDefinitions(resultSet, streamConstraint);
                return ValueCreator.createStreamValue(TypeCreator.createStreamType(streamConstraint,
                        PredefinedTypes.TYPE_NULL), resultParameterProcessor
                        .createRecordIterator(resultSet, statement, connection, columnDefinitions, streamConstraint));
            } catch (SQLException e) {
                Utils.closeResources(trxResourceManager, resultSet, statement, connection);
                BError errorValue = ErrorGenerator.getSQLDatabaseError(e,
                        "Error while executing SQL query: " + sqlQuery + ". ");
                return getErrorStream(recordType, errorValue);
            } catch (ApplicationError applicationError) {
                Utils.closeResources(trxResourceManager, resultSet, statement, connection);
                BError errorValue = ErrorGenerator.getSQLApplicationError(applicationError.getMessage());
                return getErrorStream(recordType, errorValue);
            } catch (Throwable e) {
                Utils.closeResources(trxResourceManager, resultSet, statement, connection);
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

    public static Object nativeQueryRow(
            BObject client, Object paramSQLString,
            BTypedesc ballerinaType,
            AbstractStatementParameterProcessor statementParameterProcessor,
            AbstractResultParameterProcessor resultParameterProcessor) {
        Type describingType = ballerinaType.getDescribingType();
        if (describingType.getTag() == TypeTags.UNION_TAG) {
            return ErrorGenerator.getSQLApplicationError("Return type cannot be a union.");
        }

        Object dbClient = client.getNativeData(Constants.DATABASE_CLIENT);
        TransactionResourceManager trxResourceManager = TransactionResourceManager.getInstance();
        if (dbClient != null) {
            SQLDatasource sqlDatasource = (SQLDatasource) dbClient;
            if (!((Boolean) client.getNativeData(Constants.DATABASE_CLIENT_ACTIVE_STATUS))) {
               return ErrorGenerator.getSQLApplicationError("SQL Client is already closed, hence " +
                        "further operations are not allowed");
            }
            Connection connection = null;
            PreparedStatement statement = null;
            ResultSet resultSet = null;
            String sqlQuery = null;
            try {
                if (paramSQLString instanceof BString) {
                    sqlQuery = ((BString) paramSQLString).getValue();
                } else {
                    sqlQuery = Utils.getSqlQuery((BObject) paramSQLString);
                }
                connection = SQLDatasource.getConnection(trxResourceManager, client, sqlDatasource);
                statement = connection.prepareStatement(sqlQuery);
                if (paramSQLString instanceof BObject) {
                    statementParameterProcessor.setParams(connection, statement, (BObject) paramSQLString);
                }
                resultSet = statement.executeQuery();
                if (!resultSet.next()) {
                    return ErrorGenerator.getNoRowsError("Query did not retrieve any rows.");
                }

                if (describingType.getTag() == TypeTags.RECORD_TYPE_TAG) {
                    RecordType recordConstraint = (RecordType) describingType;
                    List<ColumnDefinition> columnDefinitions = Utils.getColumnDefinitions(resultSet, recordConstraint);
                    return resultParameterProcessor.createRecord(resultSet, columnDefinitions, recordConstraint);
                } else {
                    if (resultSet.getMetaData().getColumnCount() > 1) {
                        return ErrorGenerator.getTypeMismatchError("Expected type to be '" + describingType +
                                "' but found 'record{}'");
                    }
                    ColumnDefinition columnDefinition = Utils.getColumnDefinition(resultSet, 1, describingType);
                    return resultParameterProcessor.createValue(resultSet, 1, columnDefinition);
                }
            } catch (SQLException e) {
                return ErrorGenerator.getSQLDatabaseError(e,
                        "Error while executing SQL query: " + sqlQuery + ". ");
            } catch (Throwable e) {
                String message = e.getMessage();
                if (message == null) {
                    message = e.getClass().getName();
                }
                return ErrorGenerator.getSQLApplicationError(
                        "Error while executing SQL query: " + sqlQuery + ". " + message);
            } finally {
                Utils.closeResources(trxResourceManager, resultSet, statement, connection);
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
                new Object[] { errorValue, null});
    }

}
