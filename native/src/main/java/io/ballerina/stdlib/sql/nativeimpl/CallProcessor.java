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
import io.ballerina.runtime.api.types.StructureType;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BStream;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
import io.ballerina.runtime.transactions.TransactionResourceManager;
import io.ballerina.stdlib.sql.Constants;
import io.ballerina.stdlib.sql.datasource.SQLDatasource;
import io.ballerina.stdlib.sql.exception.ApplicationError;
import io.ballerina.stdlib.sql.parameterprocessor.DefaultResultParameterProcessor;
import io.ballerina.stdlib.sql.parameterprocessor.DefaultStatementParameterProcessor;
import io.ballerina.stdlib.sql.utils.ColumnDefinition;
import io.ballerina.stdlib.sql.utils.ErrorGenerator;
import io.ballerina.stdlib.sql.utils.ModuleUtils;

import java.io.IOException;
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
import static io.ballerina.stdlib.sql.Constants.PROCEDURE_CALL_RESULT;
import static io.ballerina.stdlib.sql.Constants.QUERY_RESULT_FIELD;
import static io.ballerina.stdlib.sql.Constants.RESULT_SET_COUNT_NATIVE_DATA_FIELD;
import static io.ballerina.stdlib.sql.Constants.RESULT_SET_TOTAL_NATIVE_DATA_FIELD;
import static io.ballerina.stdlib.sql.Constants.STATEMENT_NATIVE_DATA_FIELD;
import static io.ballerina.stdlib.sql.Constants.TYPE_DESCRIPTIONS_NATIVE_DATA_FIELD;
import static io.ballerina.stdlib.sql.utils.Utils.getColumnDefinitions;
import static io.ballerina.stdlib.sql.utils.Utils.getDefaultRecordType;
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
     * @param client client object
     * @param paramSQLString SQL string for the call statement
     * @param recordTypes type description of the result record                
     * @param statementParameterProcessor pre-processor of the statement
     * @param resultParameterProcessor post-processor of the result
     * @return procedure call result or error
     */
    public static Object nativeCall(BObject client, Object paramSQLString, BArray recordTypes, 
            DefaultStatementParameterProcessor statementParameterProcessor, 
            DefaultResultParameterProcessor resultParameterProcessor) {
        Object dbClient = client.getNativeData(DATABASE_CLIENT);
        TransactionResourceManager trxResourceManager = TransactionResourceManager.getInstance();
        if (dbClient != null) {
            SQLDatasource sqlDatasource = (SQLDatasource) dbClient;
            if (!((Boolean) client.getNativeData(Constants.DATABASE_CLIENT_ACTIVE_STATUS))) {
                return ErrorGenerator.getSQLApplicationError("SQL Client is already closed, hence further operations" +
                        " are not allowed");
            }
            Connection connection;
            CallableStatement statement;
            ResultSet resultSet;
            String sqlQuery = null;
            try {
                if (paramSQLString instanceof BString) {
                    sqlQuery = ((BString) paramSQLString).getValue();
                } else {
                    sqlQuery = getSqlQuery((BObject) paramSQLString);
                }
                connection = SQLDatasource.getConnection(trxResourceManager, client, sqlDatasource);
                statement = connection.prepareCall(sqlQuery);

                HashMap<Integer, Integer> outputParamTypes = new HashMap<>();
                if (paramSQLString instanceof BObject) {
                    setCallParameters(connection, statement, (BObject) paramSQLString, outputParamTypes, 
                                statementParameterProcessor);
                }

                boolean resultType = statement.execute();

                if (paramSQLString instanceof BObject) {
                    populateOutParameters(statement, (BObject) paramSQLString, outputParamTypes, 
                            resultParameterProcessor);
                }

                BObject iteratorObject = resultParameterProcessor.getCustomProcedureCallObject();

                BObject procedureCallResult = ValueCreator.createObjectValue(ModuleUtils.getModule(),
                        PROCEDURE_CALL_RESULT, new Object[]{iteratorObject});
                Object[] recordDescriptions = recordTypes.getValues();
                resultSet = statement.getResultSet();
                int resultSetCount = 1;
                if (resultType && resultSet != null) {
                    List<ColumnDefinition> columnDefinitions;
                    StructureType streamConstraint;
                    if (recordTypes.size() == 0) {
                        columnDefinitions = getColumnDefinitions(resultSet, null);
                        streamConstraint = getDefaultRecordType(columnDefinitions);
                    } else {
                        streamConstraint = (StructureType) ((BTypedesc) recordDescriptions[0]).getDescribingType();
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
                procedureCallResult.addNativeData(STATEMENT_NATIVE_DATA_FIELD, statement);
                procedureCallResult.addNativeData(CONNECTION_NATIVE_DATA_FIELD, connection);
                procedureCallResult.addNativeData(TYPE_DESCRIPTIONS_NATIVE_DATA_FIELD, recordDescriptions);
                procedureCallResult.addNativeData(RESULT_SET_TOTAL_NATIVE_DATA_FIELD, recordTypes.size());
                procedureCallResult.addNativeData(RESULT_SET_COUNT_NATIVE_DATA_FIELD, resultSetCount);
                return procedureCallResult;
            } catch (SQLException e) {
                return ErrorGenerator.getSQLDatabaseError(e, "Error while executing SQL query: " + sqlQuery + ". ");
            } catch (ApplicationError | IOException e) {
                return ErrorGenerator.getSQLApplicationError("Error while executing SQL query: "
                        + sqlQuery + ". " + e.getMessage());
            }
        } else {
            return ErrorGenerator.getSQLApplicationError("Client is not properly initialized!");
        }
    }

    private static void setCallParameters(Connection connection, CallableStatement statement,
                                  BObject paramString, HashMap<Integer, Integer> outputParamTypes,
                                  DefaultStatementParameterProcessor statementParameterProcessor)
            throws SQLException, ApplicationError, IOException {
        BArray arrayValue = paramString.getArrayValue(Constants.ParameterizedQueryFields.INSERTIONS);
        for (int i = 0; i < arrayValue.size(); i++) {
            Object object = arrayValue.get(i);
            int index = i + 1;
            if (object instanceof BObject) {
                BObject objectValue = (BObject) object;
                if ((objectValue.getType().getTag() != TypeTags.OBJECT_TYPE_TAG)) {
                    throw new ApplicationError("Unsupported type:" +
                            objectValue.getType().getQualifiedName() + " in column index: " + index);
                }

                String parameterType;
                String objectType = objectValue.getType().getName();
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
                        statementParameterProcessor.setSQLValueParam(connection, statement, index, object, false);
                }
            } else {
                statementParameterProcessor.setSQLValueParam(connection, statement, index, object, false);
            }
        }
    }

    private static void populateOutParameters(CallableStatement statement, BObject paramSQLString,
                                      HashMap<Integer, Integer> outputParamTypes,
                                      DefaultResultParameterProcessor resultParameterProcessor)
            throws SQLException, ApplicationError {
        if (outputParamTypes.size() == 0) {
            return;
        }
        BArray arrayValue = paramSQLString.getArrayValue(Constants.ParameterizedQueryFields.INSERTIONS);

        for (Map.Entry<Integer, Integer> entry : outputParamTypes.entrySet()) {
            int paramIndex = entry.getKey();
            int sqlType = entry.getValue();

            BObject parameter = (BObject) arrayValue.get(paramIndex - 1);
            parameter.addNativeData(Constants.ParameterObject.SQL_TYPE_NATIVE_DATA, sqlType);

            switch (sqlType) {
                case Types.CHAR:
                    resultParameterProcessor.populateChar(statement, parameter, paramIndex);
                    break;
                case Types.VARCHAR:
                    resultParameterProcessor.populateVarchar(statement, parameter, paramIndex);
                    break;
                case Types.LONGVARCHAR:
                    resultParameterProcessor.populateLongVarchar(statement, parameter, paramIndex);
                    break;
                case Types.NCHAR:
                    resultParameterProcessor.populateNChar(statement, parameter, paramIndex);
                    break;
                case Types.NVARCHAR:
                    resultParameterProcessor.populateNVarchar(statement, parameter, paramIndex);
                    break;
                case Types.LONGNVARCHAR:
                    resultParameterProcessor.populateLongNVarchar(statement, parameter, paramIndex);
                    break;
                case Types.BINARY:
                    resultParameterProcessor.populateBinary(statement, parameter, paramIndex);
                    break;
                case Types.VARBINARY:
                    resultParameterProcessor.populateVarBinary(statement, parameter, paramIndex);
                    break;
                case Types.LONGVARBINARY:
                    resultParameterProcessor.populateLongVarBinary(statement, parameter, paramIndex);
                    break;
                case Types.BLOB:
                    resultParameterProcessor.populateBlob(statement, parameter, paramIndex);
                    break;
                case Types.CLOB:
                    resultParameterProcessor.populateClob(statement, parameter, paramIndex);
                    break;
                case Types.NCLOB:
                    resultParameterProcessor.populateNClob(statement, parameter, paramIndex);
                    break;
                case Types.DATE:
                    resultParameterProcessor.populateDate(statement, parameter, paramIndex);
                    break;
                case Types.TIME:
                    resultParameterProcessor.populateTime(statement, parameter, paramIndex);
                    break;
                case Types.TIME_WITH_TIMEZONE:
                    resultParameterProcessor.populateTimeWithTimeZone(statement, parameter, paramIndex);
                    break;
                case Types.TIMESTAMP:
                    resultParameterProcessor.populateTimestamp(statement, parameter, paramIndex);
                    break;
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    resultParameterProcessor.populateTimestampWithTimeZone(statement, parameter, paramIndex);
                    break;
                case Types.ARRAY:
                    resultParameterProcessor.populateArray(statement, parameter, paramIndex);
                    break;
                case Types.ROWID:
                    resultParameterProcessor.populateRowID(statement, parameter, paramIndex);
                    break;
                case Types.TINYINT:
                    resultParameterProcessor.populateTinyInt(statement, parameter, paramIndex);
                    break;
                case Types.SMALLINT:
                    resultParameterProcessor.populateSmallInt(statement, parameter, paramIndex);
                    break;
                case Types.INTEGER:
                    resultParameterProcessor.populateInteger(statement, parameter, paramIndex);
                    break;
                case Types.BIGINT:
                    resultParameterProcessor.populateBigInt(statement, parameter, paramIndex);
                    break;
                case Types.REAL:
                    resultParameterProcessor.populateReal(statement, parameter, paramIndex);
                    break;
                case Types.FLOAT:
                    resultParameterProcessor.populateFloat(statement, parameter, paramIndex);
                    break;
                case Types.DOUBLE:
                    resultParameterProcessor.populateDouble(statement, parameter, paramIndex);
                    break;
                case Types.NUMERIC:
                    resultParameterProcessor.populateNumeric(statement, parameter, paramIndex);
                    break;
                case Types.DECIMAL:
                    resultParameterProcessor.populateDecimal(statement, parameter, paramIndex);
                    break;
                case Types.BIT:
                    resultParameterProcessor.populateBit(statement, parameter, paramIndex);
                    break;
                case Types.BOOLEAN:
                    resultParameterProcessor.populateBoolean(statement, parameter, paramIndex);
                    break;
                case Types.REF:
                    resultParameterProcessor.populateRef(statement, parameter, paramIndex);
                    break;
                case Types.STRUCT:
                    resultParameterProcessor.populateStruct(statement, parameter, paramIndex);
                    break;
                case Types.SQLXML:
                    resultParameterProcessor.populateXML(statement, parameter, paramIndex);
                    break;
                default:
                    resultParameterProcessor.populateCustomOutParameters(statement, parameter, paramIndex, sqlType);
            }
        }
    }

    private static int getOutParameterType(
            BObject typedValue, DefaultStatementParameterProcessor statementParameterProcessor
            ) throws ApplicationError {
        String sqlType = typedValue.getType().getName();
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
        case Constants.OutParameterTypes.TIMEWITHTIMEZONE:
            sqlTypeValue = Types.TIME_WITH_TIMEZONE;
            break;
        case Constants.OutParameterTypes.TIMESTAMP:
        case Constants.OutParameterTypes.DATETIME:
            sqlTypeValue = Types.TIMESTAMP;
            break;
        case Constants.OutParameterTypes.TIMESTAMPWITHTIMEZONE:
            sqlTypeValue = Types.TIMESTAMP_WITH_TIMEZONE;
            break;
        case Constants.OutParameterTypes.ARRAY:
            sqlTypeValue = Types.ARRAY;
            break;
        case Constants.OutParameterTypes.REF:
            sqlTypeValue = Types.REF;
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
