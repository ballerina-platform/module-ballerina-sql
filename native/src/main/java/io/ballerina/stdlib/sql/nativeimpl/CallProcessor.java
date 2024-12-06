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
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.PredefinedTypes;
import io.ballerina.runtime.api.types.StructureType;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BStream;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
import io.ballerina.runtime.transactions.TransactionLocalContext;
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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.ballerina.stdlib.sql.Constants.CONNECTION_NATIVE_DATA_FIELD;
import static io.ballerina.stdlib.sql.Constants.DATABASE_CLIENT;
import static io.ballerina.stdlib.sql.Constants.PARAMETER_INDEX_META_DATA;
import static io.ballerina.stdlib.sql.Constants.PROCEDURE_CALL_RESULT;
import static io.ballerina.stdlib.sql.Constants.QUERY_RESULT_FIELD;
import static io.ballerina.stdlib.sql.Constants.RESULT_PARAMETER_PROCESSOR;
import static io.ballerina.stdlib.sql.Constants.RESULT_SET_COUNT_NATIVE_DATA_FIELD;
import static io.ballerina.stdlib.sql.Constants.RESULT_SET_TOTAL_NATIVE_DATA_FIELD;
import static io.ballerina.stdlib.sql.Constants.STATEMENT_NATIVE_DATA_FIELD;
import static io.ballerina.stdlib.sql.Constants.TYPE_DESCRIPTIONS_NATIVE_DATA_FIELD;
import static io.ballerina.stdlib.sql.utils.Utils.closeResources;
import static io.ballerina.stdlib.sql.utils.Utils.getColumnDefinitions;
import static io.ballerina.stdlib.sql.utils.Utils.getDefaultStreamConstraint;
import static io.ballerina.stdlib.sql.utils.Utils.getSqlQuery;
import static io.ballerina.stdlib.sql.utils.Utils.updateProcedureCallExecutionResult;

/**
 * This class holds the utility methods involved with executing the call statements.
 *
 * @since 0.5.6
 */
public class CallProcessor {

    private CallProcessor() {
    }

    /**
     * Execute a call query and return the results.
     *
     * @param client                      client object
     * @param paramSQLString              SQL string for the call statement
     * @param recordTypes                 type description of the result record
     * @param statementParameterProcessor pre-processor of the statement
     * @param resultParameterProcessor    post-processor of the result
     * @return procedure call result or error
     */
    public static Object nativeCall(Environment env, BObject client, BObject paramSQLString, BArray recordTypes,
                                    AbstractStatementParameterProcessor statementParameterProcessor,
                                    AbstractResultParameterProcessor resultParameterProcessor) {
        TransactionResourceManager trxResourceManager = TransactionResourceManager.getInstance();
        boolean withinTrxBlock = Utils.isWithinTrxBlock(trxResourceManager);
        boolean trxManagerEnabled = trxResourceManager.getTransactionManagerEnabled();
        TransactionLocalContext currentTrxContext = trxResourceManager.getCurrentTransactionContext();
        return nativeCallExecutable(client, paramSQLString, recordTypes,
                statementParameterProcessor, resultParameterProcessor, withinTrxBlock,
                currentTrxContext, trxManagerEnabled);
    }

    private static Object nativeCallExecutable(BObject client, BObject paramSQLString, BArray recordTypes,
                                               AbstractStatementParameterProcessor statementParameterProcessor,
                                               AbstractResultParameterProcessor resultParameterProcessor,
                                               boolean isWithinTrxBlock, TransactionLocalContext currentTrxContext,
                                               boolean trxManagerEnabled) {
        Object dbClient = client.getNativeData(DATABASE_CLIENT);
        if (dbClient != null) {
            SQLDatasource sqlDatasource = (SQLDatasource) dbClient;
            if (!((Boolean) client.getNativeData(Constants.DATABASE_CLIENT_ACTIVE_STATUS))) {
                return ErrorGenerator.getSQLApplicationError(
                        "SQL Client is already closed, hence further operations are not allowed");
            }
            Connection connection = null;
            CallableStatement statement = null;
            ResultSet resultSet = null;
            String sqlQuery = null;
            try {
                sqlQuery = getSqlQuery(paramSQLString);
                connection = SQLDatasource.getConnection(isWithinTrxBlock, client, sqlDatasource, currentTrxContext,
                        trxManagerEnabled);
                statement = connection.prepareCall(sqlQuery);

                HashMap<Integer, Integer> outputParamTypes = new HashMap<>();
                setCallParameters(connection, statement, paramSQLString, outputParamTypes,
                        statementParameterProcessor);

                boolean resultType = statement.execute();

                BObject iteratorObject = resultParameterProcessor.getBalStreamResultIterator();
                BObject procedureCallResult = ValueCreator.createObjectValue(ModuleUtils.getModule(),
                        PROCEDURE_CALL_RESULT, iteratorObject);
                Object[] recordDescriptions = recordTypes.getValues();
                int resultSetCount = 0;
                if (resultType) {
                    List<ColumnDefinition> columnDefinitions;
                    StructureType streamConstraint;
                    resultSet = statement.getResultSet();
                    if (recordTypes.size() == 0) {
                        streamConstraint = getDefaultStreamConstraint();
                        columnDefinitions = getColumnDefinitions(resultSet, streamConstraint);
                    } else {
                        streamConstraint = (StructureType) TypeUtils.getReferredType(
                                ((BTypedesc) recordDescriptions[0]).getDescribingType());
                        columnDefinitions = getColumnDefinitions(resultSet, streamConstraint);
                        resultSetCount++;
                    }
                    BStream streamValue = ValueCreator.createStreamValue(TypeCreator.createStreamType(streamConstraint,
                            PredefinedTypes.TYPE_NULL),
                            resultParameterProcessor.createRecordIterator(resultSet, null, null, columnDefinitions,
                                    streamConstraint));
                    procedureCallResult.set(QUERY_RESULT_FIELD, streamValue);
                } else {
                    updateProcedureCallExecutionResult(statement, procedureCallResult);
                }

                populateOutParametersMetaData(statement, paramSQLString, outputParamTypes,
                        resultParameterProcessor, procedureCallResult);

                procedureCallResult.addNativeData(STATEMENT_NATIVE_DATA_FIELD, statement);
                procedureCallResult.addNativeData(CONNECTION_NATIVE_DATA_FIELD, connection);
                procedureCallResult.addNativeData(TYPE_DESCRIPTIONS_NATIVE_DATA_FIELD, recordDescriptions);
                procedureCallResult.addNativeData(RESULT_SET_TOTAL_NATIVE_DATA_FIELD, recordTypes.size());
                procedureCallResult.addNativeData(RESULT_SET_COUNT_NATIVE_DATA_FIELD, resultSetCount);
                return procedureCallResult;
            } catch (SQLException e) {
                closeResources(isWithinTrxBlock, resultSet, statement, connection);
                return ErrorGenerator.getSQLDatabaseError(e,
                        String.format("Error while executing SQL query: %s. ", sqlQuery));
            } catch (ApplicationError e) {
                closeResources(isWithinTrxBlock, resultSet, statement, connection);
                return ErrorGenerator.getSQLApplicationError(e);
            } catch (Throwable th) {
                closeResources(isWithinTrxBlock, resultSet, statement, connection);
                return ErrorGenerator.getSQLError(th, String.format("Error while executing SQL query: %s. ", sqlQuery));
            }
        } else {
            return ErrorGenerator.getSQLApplicationError("Client is not properly initialized!");
        }
    }

    private static void setCallParameters(Connection connection, CallableStatement statement,
                                          BObject paramString, HashMap<Integer, Integer> outputParamTypes,
                                          AbstractStatementParameterProcessor statementParameterProcessor)
            throws SQLException, ApplicationError {
        BArray arrayValue = paramString.getArrayValue(Constants.ParameterizedQueryFields.INSERTIONS);
        for (int i = 0; i < arrayValue.size(); i++) {
            Object object = arrayValue.get(i);
            int index = i + 1;
            if (object instanceof BObject) {
                BObject objectValue = (BObject) object;
                String parameterType;
                String objectType = TypeUtils.getType(objectValue).getName();
                if (objectType.equals(Constants.ParameterObject.INOUT_PARAMETER)) {
                    parameterType = Constants.ParameterObject.INOUT_PARAMETER;
                } else if (objectType.endsWith("OutParameter")) {
                    parameterType = Constants.ParameterObject.OUT_PARAMETER;
                } else {
                    parameterType = "InParameter";
                }

                int sqlType;
                switch (parameterType) {
                    case Constants.ParameterObject.INOUT_PARAMETER:
                        Object innerObject = objectValue.get(Constants.ParameterObject.IN_VALUE_FIELD);
                        sqlType = statementParameterProcessor.setSQLValueParam(connection, statement,
                                index, innerObject, true);
                        outputParamTypes.put(index, sqlType);
                        statement.registerOutParameter(index, sqlType);
                        break;
                    case Constants.ParameterObject.OUT_PARAMETER:
                        sqlType = getOutParameterType(objectValue, statementParameterProcessor);
                        outputParamTypes.put(index, sqlType);
                        statement.registerOutParameter(index, sqlType);
                        break;
                    default:
                        statementParameterProcessor.setSQLValueParam(connection, statement, index,
                                object, false);
                }
            } else {
                statementParameterProcessor.setSQLValueParam(connection, statement, index, object,
                        false);
            }
        }
    }

    private static void populateOutParametersMetaData(CallableStatement statement, BObject paramSQLString,
                                                      HashMap<Integer, Integer> outputParamTypes,
                                                      AbstractResultParameterProcessor resultParameterProcessor,
                                                      BObject procedureCallResult)
            throws SQLException, ApplicationError {
        if (outputParamTypes.size() == 0) {
            return;
        }
        BArray arrayValue = paramSQLString.getArrayValue(Constants.ParameterizedQueryFields.INSERTIONS);
        for (Map.Entry<Integer, Integer> entry : outputParamTypes.entrySet()) {
            int paramIndex = entry.getKey();
            int sqlType = entry.getValue();
            BObject parameter = (BObject) arrayValue.get(paramIndex - 1);
            parameter.addNativeData(PARAMETER_INDEX_META_DATA, paramIndex);
            parameter.addNativeData(Constants.ParameterObject.SQL_TYPE_NATIVE_DATA, sqlType);

            parameter.addNativeData(RESULT_PARAMETER_PROCESSOR, resultParameterProcessor);
            parameter.addNativeData(STATEMENT_NATIVE_DATA_FIELD, statement);
            parameter.addNativeData(PROCEDURE_CALL_RESULT, procedureCallResult);
        }
    }

    private static int getOutParameterType(BObject typedValue,
                                           AbstractStatementParameterProcessor statementParameterProcessor)
            throws ApplicationError, SQLException {
        String sqlType = TypeUtils.getType(typedValue).getName();
        int sqlTypeValue;
        switch (sqlType) {
            case Constants.OutParameterTypes.VARCHAR:
            case Constants.OutParameterTypes.TEXT:
                sqlTypeValue = Types.VARCHAR;
                break;
            case Constants.OutParameterTypes.CHAR:
                sqlTypeValue = Types.CHAR;
                break;
            case Constants.OutParameterTypes.NCHAR:
                sqlTypeValue = Types.NCHAR;
                break;
            case Constants.OutParameterTypes.NVARCHAR:
                sqlTypeValue = Types.NVARCHAR;
                break;
            case Constants.OutParameterTypes.BIT:
                sqlTypeValue = Types.BIT;
                break;
            case Constants.OutParameterTypes.BOOLEAN:
                sqlTypeValue = Types.BOOLEAN;
                break;
            case Constants.OutParameterTypes.INTEGER:
                sqlTypeValue = Types.INTEGER;
                break;
            case Constants.OutParameterTypes.BIGINT:
                sqlTypeValue = Types.BIGINT;
                break;
            case Constants.OutParameterTypes.SMALLINT:
                sqlTypeValue = Types.SMALLINT;
                break;
            case Constants.OutParameterTypes.FLOAT:
                sqlTypeValue = Types.FLOAT;
                break;
            case Constants.OutParameterTypes.REAL:
                sqlTypeValue = Types.REAL;
                break;
            case Constants.OutParameterTypes.DOUBLE:
                sqlTypeValue = Types.DOUBLE;
                break;
            case Constants.OutParameterTypes.NUMERIC:
                sqlTypeValue = Types.NUMERIC;
                break;
            case Constants.OutParameterTypes.DECIMAL:
                sqlTypeValue = Types.DECIMAL;
                break;
            case Constants.OutParameterTypes.BINARY:
                sqlTypeValue = Types.BINARY;
                break;
            case Constants.OutParameterTypes.VARBINARY:
                sqlTypeValue = Types.VARBINARY;
                break;
            case Constants.OutParameterTypes.BLOB:
                if (typedValue instanceof BArray) {
                    sqlTypeValue = Types.VARBINARY;
                } else {
                    sqlTypeValue = Types.LONGVARBINARY;
                }
                break;
            case Constants.OutParameterTypes.CLOB:
            case Constants.OutParameterTypes.NCLOB:
                if (typedValue instanceof BString) {
                    sqlTypeValue = Types.CLOB;
                } else {
                    sqlTypeValue = Types.LONGVARCHAR;
                }
                break;
            case Constants.OutParameterTypes.DATE:
                sqlTypeValue = Types.DATE;
                break;
            case Constants.OutParameterTypes.TIME:
                sqlTypeValue = Types.TIME;
                break;
            case Constants.OutParameterTypes.TIME_WITH_TIMEZONE:
                sqlTypeValue = Types.TIME_WITH_TIMEZONE;
                break;
            case Constants.OutParameterTypes.TIMESTAMP:
            case Constants.OutParameterTypes.DATE_TIME:
                sqlTypeValue = Types.TIMESTAMP;
                break;
            case Constants.OutParameterTypes.TIMESTAMP_WITH_TIMEZONE:
                sqlTypeValue = Types.TIMESTAMP_WITH_TIMEZONE;
                break;
            case Constants.OutParameterTypes.ARRAY:
            case Constants.OutParameterTypes.SMALL_INT_ARRAY:
            case Constants.OutParameterTypes.BIGINT_ARRAY:
            case Constants.OutParameterTypes.BINARY_ARRAY:
            case Constants.OutParameterTypes.BIT_ARRAY:
            case Constants.OutParameterTypes.BOOLEAN_ARRAY:
            case Constants.OutParameterTypes.CHAR_ARRAY:
            case Constants.OutParameterTypes.DATE_ARRAY:
            case Constants.OutParameterTypes.DATE_TIME_ARRAY:
            case Constants.OutParameterTypes.DECIMAL_ARRAY:
            case Constants.OutParameterTypes.DOUBLE_ARRAY:
            case Constants.OutParameterTypes.FLOAT_ARRAY:
            case Constants.OutParameterTypes.INTEGER_ARRAY:
            case Constants.OutParameterTypes.NUMERIC_ARRAY:
            case Constants.OutParameterTypes.NVARCHAR_ARRAY:
            case Constants.OutParameterTypes.TIME_WITH_TIMEZONE_ARRAY:
            case Constants.OutParameterTypes.TIMESTAMP_WITH_TIMEZONE_ARRAY:
            case Constants.OutParameterTypes.TIMESTAMP_ARRAY:
            case Constants.OutParameterTypes.REAL_ARRAY:
            case Constants.OutParameterTypes.VARBINARY_ARRAY:
            case Constants.OutParameterTypes.VARCHAR_ARRAY:
            case Constants.OutParameterTypes.TIME_ARRAY:
                sqlTypeValue = Types.ARRAY;
                break;
            case Constants.OutParameterTypes.REF:
                sqlTypeValue = Types.REF;
                break;
            case Constants.OutParameterTypes.REF_CURSOR:
                sqlTypeValue = Types.REF_CURSOR;
                break;
            case Constants.OutParameterTypes.STRUCT:
                sqlTypeValue = Types.STRUCT;
                break;
            case Constants.OutParameterTypes.ROW:
                sqlTypeValue = Types.ROWID;
                break;
            case Constants.OutParameterTypes.XML:
                sqlTypeValue = Types.SQLXML;
                break;
            default:
                sqlTypeValue = statementParameterProcessor.getCustomOutParameterType(typedValue);
        }
        return sqlTypeValue;
    }
}
