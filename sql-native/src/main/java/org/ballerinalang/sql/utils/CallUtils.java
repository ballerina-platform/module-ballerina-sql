/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.ballerinalang.sql.utils;

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
import org.ballerinalang.sql.Constants;
import org.ballerinalang.sql.datasource.SQLDatasource;
import org.ballerinalang.sql.exception.ApplicationError;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.ballerinalang.sql.Constants.CONNECTION_NATIVE_DATA_FIELD;
import static org.ballerinalang.sql.Constants.DATABASE_CLIENT;
import static org.ballerinalang.sql.Constants.PROCEDURE_CALL_RESULT;
import static org.ballerinalang.sql.Constants.QUERY_RESULT_FIELD;
import static org.ballerinalang.sql.Constants.RESULT_SET_COUNT_NATIVE_DATA_FIELD;
import static org.ballerinalang.sql.Constants.RESULT_SET_TOTAL_NATIVE_DATA_FIELD;
import static org.ballerinalang.sql.Constants.STATEMENT_NATIVE_DATA_FIELD;
import static org.ballerinalang.sql.Constants.TYPE_DESCRIPTIONS_NATIVE_DATA_FIELD;
import static org.ballerinalang.sql.utils.Utils.getColumnDefinitions;
import static org.ballerinalang.sql.utils.Utils.getDefaultRecordType;
import static org.ballerinalang.sql.utils.Utils.setSQLValueParam;
import static org.ballerinalang.sql.utils.Utils.updateProcedureCallExecutionResult;

/**
 * This class holds the utility methods involved with executing the call statements.
 */
public class CallUtils {
    private static final Calendar calendar = Calendar
            .getInstance(TimeZone.getTimeZone(Constants.TIMEZONE_UTC.getValue()));

    public static Object nativeCall(BObject client, Object paramSQLString, BArray recordTypes) {
        Object dbClient = client.getNativeData(DATABASE_CLIENT);
        TransactionResourceManager trxResourceManager = TransactionResourceManager.getInstance();
        if (dbClient != null) {
            SQLDatasource sqlDatasource = (SQLDatasource) dbClient;
            Connection connection;
            CallableStatement statement;
            ResultSet resultSet;
            String sqlQuery = null;
            try {
                if (paramSQLString instanceof BString) {
                    sqlQuery = ((BString) paramSQLString).getValue();
                } else {
                    sqlQuery = Utils.getSqlQuery((BObject) paramSQLString);
                }
                connection = SQLDatasource.getConnection(trxResourceManager, client, sqlDatasource);
                statement = connection.prepareCall(sqlQuery);

                HashMap<Integer, Integer> outputParamTypes = new HashMap<>();
                if (paramSQLString instanceof BObject) {
                    setCallParameters(connection, statement, (BObject) paramSQLString, outputParamTypes);
                }

                boolean resultType = statement.execute();

                if (paramSQLString instanceof BObject) {
                    populateOutParameters(statement, (BObject) paramSQLString, outputParamTypes);
                }

                BObject procedureCallResult = ValueCreator.createObjectValue(ModuleUtils.getModule(),
                        PROCEDURE_CALL_RESULT);
                Object[] recordDescriptions = recordTypes.getValues();
                int resultSetCount = 0;
                if (resultType) {
                    List<ColumnDefinition> columnDefinitions;
                    StructureType streamConstraint;
                    resultSet = statement.getResultSet();
                    if (recordTypes.size() == 0) {
                        columnDefinitions = getColumnDefinitions(resultSet, null);
                        streamConstraint = getDefaultRecordType(columnDefinitions);
                    } else {
                        streamConstraint = (StructureType) ((BTypedesc) recordDescriptions[0]).getDescribingType();
                        columnDefinitions = getColumnDefinitions(resultSet, streamConstraint);
                        resultSetCount++;
                    }
                    BStream streamValue = ValueCreator.createStreamValue(TypeCreator.createStreamType(streamConstraint),
                            Utils.createRecordIterator(resultSet, null, null, columnDefinitions, streamConstraint));
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
                                  BObject paramString, HashMap<Integer, Integer> outputParamTypes)
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
                        sqlType = setSQLValueParam(connection, statement, innerObject, index, true);
                        outputParamTypes.put(index, sqlType);
                        statement.registerOutParameter(index, sqlType);
                        break;
                    case Constants.ParameterObject.OUT_PARAMETER:
                        sqlType = getOutParameterType(objectValue);
                        outputParamTypes.put(index, sqlType);
                        statement.registerOutParameter(index, sqlType);
                        break;
                    default:
                        setSQLValueParam(connection, statement, object, index, false);
                }
            } else {
                setSQLValueParam(connection, statement, object, index, false);
            }
        }
    }

    private static void populateOutParameters(CallableStatement statement, BObject paramSQLString,
                                      HashMap<Integer, Integer> outputParamTypes)
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
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                            statement.getString(paramIndex));
                    break;
                case Types.BLOB:
                    parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                            statement.getBlob(paramIndex));
                    break;
                case Types.CLOB:
                    parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                            statement.getClob(paramIndex));
                    break;
                case Types.NCLOB:
                    parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                            statement.getNClob(paramIndex));
                    break;
                case Types.DATE:
                    parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                            statement.getDate(paramIndex, calendar));
                    break;
                case Types.TIME:
                    parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                            statement.getTime(paramIndex, calendar));
                    break;
                case Types.TIME_WITH_TIMEZONE:
                    try {
                        Time time = statement.getTime(paramIndex, calendar);
                        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA, time);
                    } catch (SQLException ex) {
                        //Some database drivers do not support getTime operation,
                        // therefore falling back to getObject method.
                        OffsetTime offsetTime = statement.getObject(paramIndex, OffsetTime.class);
                        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                                Time.valueOf(offsetTime.toLocalTime()));
                    }
                    break;
                case Types.TIMESTAMP:
                    parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                            statement.getTimestamp(paramIndex, calendar));
                    break;
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    try {
                        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                                statement.getTimestamp(paramIndex, calendar));
                    } catch (SQLException ex) {
                        //Some database drivers do not support getTimestamp operation,
                        // therefore falling back to getObject method.
                        OffsetDateTime offsetDateTime = statement.getObject(paramIndex, OffsetDateTime.class);
                        parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                                Timestamp.valueOf(offsetDateTime.toLocalDateTime()));
                    }
                    break;
                case Types.ARRAY:
                    parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                            statement.getArray(paramIndex));
                    break;
                case Types.ROWID:
                    parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                            statement.getRowId(paramIndex));
                    break;
                case Types.TINYINT:
                    parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA, statement.getInt(paramIndex));
                    break;
                case Types.SMALLINT:
                    parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                        Integer.valueOf(statement.getShort(paramIndex)));
                    break;
                case Types.INTEGER:
                    parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                        Long.valueOf(statement.getInt(paramIndex)));
                    break;
                case Types.BIGINT:
                    parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA, statement.getLong(paramIndex));
                    break;
                case Types.REAL:
                case Types.FLOAT:
                    parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                            statement.getFloat(paramIndex));
                    break;
                case Types.DOUBLE:
                    parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                            statement.getDouble(paramIndex));
                    break;
                case Types.NUMERIC:
                case Types.DECIMAL:
                    parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                            statement.getBigDecimal(paramIndex));
                    break;
                case Types.BIT:
                case Types.BOOLEAN:
                    parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                            statement.getBoolean(paramIndex));
                    break;
                case Types.REF:
                case Types.STRUCT:
                    parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                            statement.getObject(paramIndex));
                    break;
                case Types.SQLXML:
                    parameter.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA,
                            statement.getSQLXML(paramIndex));
                    break;
                default:
                    throw new ApplicationError("Unsupported SQL type '" + sqlType + "' when reading Procedure call " +
                            "Out parameter of index '" + paramIndex + "'.");
            }
        }
    }

    private static int getOutParameterType(BObject typedValue) throws ApplicationError {
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
        case Constants.OutParameterTypes.TIMESTAMP:
        case Constants.OutParameterTypes.DATETIME:
            sqlTypeValue = Types.TIMESTAMP;
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
            throw new ApplicationError("Unsupported OutParameter type: " + sqlType);
        }
        return sqlTypeValue;
    }
}
