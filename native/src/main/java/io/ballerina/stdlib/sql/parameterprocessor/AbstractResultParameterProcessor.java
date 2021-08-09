/*
 *  Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.ballerina.stdlib.sql.parameterprocessor;

import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.StructureType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.JsonUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.sql.Constants;
import io.ballerina.stdlib.sql.exception.ApplicationError;
import io.ballerina.stdlib.sql.utils.ColumnDefinition;
import io.ballerina.stdlib.sql.utils.ModuleUtils;
import io.ballerina.stdlib.sql.utils.Utils;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.List;

import static io.ballerina.runtime.api.utils.StringUtils.fromString;
import static io.ballerina.stdlib.sql.utils.Utils.getString;

/**
 * This class has abstract implementation of methods to process JDBC Callable statement. Populate methods are used to
 * process callable statements and convert methods are used to transform the results to required ballerina types.
 *
 * @since 0.5.6
 */
public abstract class AbstractResultParameterProcessor {

    protected abstract BArray createAndPopulateCustomValueArray(Object firstNonNullElement, Type type,
            Array array) throws ApplicationError, SQLException;

    protected abstract BArray createAndPopulateCustomBBRefValueArray(Object firstNonNullElement, Type type,
            Array array) throws ApplicationError, SQLException;

    protected abstract void createUserDefinedTypeSubtype(Field internalField, StructureType structType)
            throws ApplicationError;

    public abstract BArray convertArray(Array array, int sqlType, Type type)
            throws SQLException, ApplicationError;

    public abstract BString convertChar(String value, int sqlType, Type type)
            throws ApplicationError;

    protected abstract Object convertChar(String value, int sqlType, Type type, String sqlTypeName)
            throws ApplicationError;

    public abstract Object convertByteArray(byte[] value, int sqlType, Type type, String sqlTypeName)
            throws ApplicationError;

    public abstract Object convertInteger(long value, int sqlType, Type type, boolean isNull)
            throws ApplicationError;

    public abstract Object convertDouble(double value, int sqlType, Type type, boolean isNull)
            throws ApplicationError;

    public abstract Object convertDecimal(BigDecimal value, int sqlType, Type type, boolean isNull)
            throws ApplicationError;

    public abstract Object convertBlob(Blob value, int sqlType, Type type) throws ApplicationError, SQLException;

    public abstract Object convertDate(java.util.Date date, int sqlType, Type type) throws ApplicationError;

    public abstract Object convertTime(java.util.Date time, int sqlType, Type type) throws ApplicationError;

    public abstract Object convertTimeWithTimezone(java.time.OffsetTime offsetTime, int sqlType, Type type)
            throws ApplicationError;

    public abstract Object convertTimeStamp(java.util.Date timeStamp, int sqlType, Type type)
            throws ApplicationError;

    public abstract Object convertTimestampWithTimezone(java.time.OffsetDateTime offsetDateTime, int sqlType,
                                                        Type type)throws ApplicationError;

    public abstract Object convertBoolean(boolean value, int sqlType, Type type, boolean isNull)
            throws ApplicationError;

    public abstract Object convertBinary(Object value, int sqlType, Type ballerinaType) throws ApplicationError; 

    public abstract Object convertStruct(Struct value, int sqlType, Type type) throws ApplicationError;

    public abstract Object convertXml(SQLXML value, int sqlType, Type type) throws ApplicationError, SQLException;

    public abstract Object convertCustomOutParameter(Object value, String outParamObjectName, int sqlType,
                                                     Type ballerinaType);

    public abstract Object convertCustomInOutParameter(Object value, Object inParamValue, int sqlType,
                                                       Type ballerinaType);

    public abstract Object processChar(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processVarchar(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processLongVarchar(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processNChar(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processNVarchar(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processLongNVarchar(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processBinary(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processVarBinary(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processLongVarBinary(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processBlob(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processClob(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processNClob(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processDate(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processTime(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processTimeWithTimeZone(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processTimestamp(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processTimestampWithTimeZone(
            CallableStatement statement, int paramIndex) throws SQLException;

    public abstract Object processArray(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processRowID(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processTinyInt(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processSmallInt(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processInteger(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processBigInt(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processReal(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processFloat(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processDouble(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processNumeric(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processDecimal(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processBit(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processBoolean(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processRef(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processStruct(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processXML(CallableStatement statement, int paramIndex)
            throws SQLException;

    public abstract Object processCustomOutParameters(
            CallableStatement statement, int paramIndex, int sqlType) throws ApplicationError;

    public abstract Object processCustomTypeFromResultSet(ResultSet resultSet, int columnIndex,
                                                           ColumnDefinition columnDefinition) throws ApplicationError,
            SQLException;

    public Object processArrayResult(ResultSet resultSet, int columnIndex, int sqlType, Type ballerinaType)
            throws ApplicationError, SQLException {
        Array array = resultSet.getArray(columnIndex);
        return convertArray(array, sqlType, ballerinaType);
    }

    public Object processCharResult(ResultSet resultSet, int columnIndex, int sqlType, Type ballerinaType)
            throws ApplicationError, SQLException {
        String string = resultSet.getString(columnIndex);
        return convertChar(string, sqlType, ballerinaType);
    }

    public Object processCharResult(ResultSet resultSet, int columnIndex, int sqlType, Type ballerinaType,
                                    String sqlTypeName) throws ApplicationError, SQLException {
        String string = resultSet.getString(columnIndex);
        return convertChar(string, sqlType, ballerinaType, sqlTypeName);
    }

    public Object processByteArrayResult(ResultSet resultSet, int columnIndex, int sqlType, Type ballerinaType,
                                         String sqlTypeName) throws ApplicationError, SQLException {
        byte[] bytes = resultSet.getBytes(columnIndex);
        return convertByteArray(bytes, sqlType, ballerinaType, sqlTypeName);
    }

    public Object processBlobResult(ResultSet resultSet, int columnIndex, int sqlType, Type ballerinaType)
            throws ApplicationError, SQLException {
        Blob blob = resultSet.getBlob(columnIndex);
        return convertBlob(blob, sqlType, ballerinaType);
    }

    public Object processClobResult(ResultSet resultSet, int columnIndex, int sqlType, Type ballerinaType)
            throws ApplicationError, SQLException, IOException {
        String clobValue = getString(resultSet.getClob(columnIndex));
        return convertChar(clobValue, sqlType, ballerinaType);
    }

    public Object processNClobResult(ResultSet resultSet, int columnIndex, int sqlType, Type ballerinaType)
            throws ApplicationError, SQLException, IOException {
        String nClobValue = getString(resultSet.getNClob(columnIndex));
        return convertChar(nClobValue, sqlType, ballerinaType);
    }

    public Object processDateResult(ResultSet resultSet, int columnIndex, int sqlType, Type ballerinaType)
            throws ApplicationError, SQLException {
        Date date = resultSet.getDate(columnIndex);
        return convertDate(date, sqlType, ballerinaType);
    }

    public Object processTimeResult(ResultSet resultSet, int columnIndex, int sqlType, Type ballerinaType)
            throws ApplicationError, SQLException {
        Time time = resultSet.getTime(columnIndex);
        return convertTime(time, sqlType, ballerinaType);
    }

    public Object processTimeWithTimezoneResult(ResultSet resultSet, int columnIndex, int sqlType, Type ballerinaType)
            throws ApplicationError, SQLException {
        OffsetTime offsetTime = resultSet.getObject(columnIndex, OffsetTime.class);
        return convertTimeWithTimezone(offsetTime, sqlType, ballerinaType);
    }

    public Object processTimestampResult(ResultSet resultSet, int columnIndex, int sqlType, Type ballerinaType)
            throws ApplicationError, SQLException {
        Timestamp timestamp = resultSet.getTimestamp(columnIndex);
        return convertTimeStamp(timestamp, sqlType, ballerinaType);
    }

    public Object processTimestampWithTimezoneResult(ResultSet resultSet, int columnIndex, int sqlType,
                                                     Type ballerinaType) throws ApplicationError, SQLException {
        OffsetDateTime offsetDateTime = resultSet.getObject(columnIndex, OffsetDateTime.class);
        return convertTimestampWithTimezone(offsetDateTime, sqlType, ballerinaType);
    }

    public Object processRowIdResult(ResultSet resultSet, int columnIndex, int sqlType, Type ballerinaType,
                                     String sqlTypeName) throws ApplicationError, SQLException {
        RowId rowId = resultSet.getRowId(columnIndex);
        return convertByteArray(rowId.getBytes(), sqlType,
                ballerinaType, sqlTypeName);
    }

    public Object processIntResult(ResultSet resultSet, int columnIndex, int sqlType, Type ballerinaType)
            throws ApplicationError, SQLException {
        long iValue = resultSet.getInt(columnIndex);
        return convertInteger(iValue, sqlType, ballerinaType, resultSet.wasNull());
    }

    public Object processLongResult(ResultSet resultSet, int columnIndex, int sqlType, Type ballerinaType)
            throws ApplicationError, SQLException {
        long iValue = resultSet.getLong(columnIndex);
        return convertInteger(iValue, sqlType, ballerinaType, resultSet.wasNull());
    }

    public Object processFloatResult(ResultSet resultSet, int columnIndex, int sqlType, Type ballerinaType)
            throws ApplicationError, SQLException {
        double fValue = resultSet.getFloat(columnIndex);
        return convertDouble(fValue, sqlType, ballerinaType, resultSet.wasNull());
    }

    public Object processDoubleResult(ResultSet resultSet, int columnIndex, int sqlType, Type ballerinaType)
            throws ApplicationError, SQLException {
        double dValue = resultSet.getDouble(columnIndex);
        return convertDouble(dValue, sqlType, ballerinaType, resultSet.wasNull());
    }

    public Object processDecimalResult(ResultSet resultSet, int columnIndex, int sqlType, Type ballerinaType)
            throws ApplicationError, SQLException {
        BigDecimal decimalValue = resultSet.getBigDecimal(columnIndex);
        return convertDecimal(decimalValue, sqlType, ballerinaType, resultSet.wasNull());
    }

    public Object processBooleanResult(ResultSet resultSet, int columnIndex, int sqlType, Type ballerinaType)
            throws ApplicationError, SQLException {
        boolean boolValue = resultSet.getBoolean(columnIndex);
        return convertBoolean(boolValue, sqlType, ballerinaType, resultSet.wasNull());
    }

    public Object processStructResult(ResultSet resultSet, int columnIndex, int sqlType, Type ballerinaType)
            throws ApplicationError, SQLException {
        Struct structData = (Struct) resultSet.getObject(columnIndex);
        return convertStruct(structData, sqlType, ballerinaType);
    }

    public Object processXmlResult(ResultSet resultSet, int columnIndex, int sqlType, Type ballerinaType)
            throws ApplicationError, SQLException {
        SQLXML sqlxml = resultSet.getSQLXML(columnIndex);
        return convertXml(sqlxml, sqlType, ballerinaType);
    }

    public Object processJsonResult(ResultSet resultSet, int columnIndex, int sqlType, Type ballerinaType)
            throws ApplicationError, SQLException {
        String jsonString = convertChar(
                resultSet.getString(columnIndex), sqlType, ballerinaType).getValue();
        Reader reader = new StringReader(jsonString);
        try {
            return JsonUtils.parse(reader, JsonUtils.NonStringValueProcessingMode.FROM_JSON_STRING);
        } catch (BError e) {
            throw new ApplicationError("Error while converting to JSON type. " + e.getDetails());
        }
    }

    public Object convertArrayOutParameter(String objectTypeName, Object[] dataArray, Type ballerinaType)
            throws ApplicationError {
        switch (objectTypeName) {
            case Constants.OutParameterTypes.CHAR_ARRAY:
            case Constants.OutParameterTypes.VARCHAR_ARRAY:
            case Constants.OutParameterTypes.NVARCHAR_ARRAY:
                return Utils.toStringArray(dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.BIT_ARRAY:
                return Utils.toBitArray(dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.BOOLEAN_ARRAY:
                return Utils.toBooleanArray(dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.INTEGER_ARRAY:
            case Constants.OutParameterTypes.SMALL_INT_ARRAY:
            case Constants.OutParameterTypes.BIGINT_ARRAY:
                return Utils.toIntArray(dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.REAL_ARRAY:
            case Constants.OutParameterTypes.DOUBLE_ARRAY:
                return Utils.toRealArray(dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.DATE_ARRAY:
                return Utils.toDateArray(dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.TIME_ARRAY:
                return Utils.toTimeArray(dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.TIME_WITH_TIMEZONE_ARRAY:
                return Utils.toTimeWithTimezoneArray(dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.DATE_TIME_ARRAY:
                return Utils.toDateTimeArray(dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.TIMESTAMP_WITH_TIMEZONE_ARRAY:
                return Utils.toTimestampWithTimezoneArray(dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.FLOAT_ARRAY:
                return Utils.toFloatArray(dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.DECIMAL_ARRAY:
            case Constants.OutParameterTypes.NUMERIC_ARRAY:
                return Utils.toNumericArray(dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.TIMESTAMP_ARRAY:
                return Utils.toTimestampArray(dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.VARBINARY_ARRAY:
            case Constants.OutParameterTypes.BINARY_ARRAY:
                return Utils.toBinaryArray(dataArray, objectTypeName, ballerinaType);
            default:
                return processCustomArrayOutParameter(dataArray, ballerinaType);
        }
    }

    public Object convertArrayInOutParameter(Object[] dataArray, Type ballerinaType) throws ApplicationError {
        String name = ballerinaType.toString();
        String className = dataArray[0].getClass().getCanonicalName();
        if (name.equalsIgnoreCase(Constants.ArrayTypes.STRING)) {
            return Utils.createStringArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.BYTE) &&
                className.equalsIgnoreCase(Constants.Classes.BYTE)) {
            return Utils.createByteArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.BOOLEAN)) {
            if (className.equalsIgnoreCase(Constants.Classes.INTEGER)) {
                return Utils.booleanToIntArray(dataArray);
            } else {
                return Utils.createBooleanArray(dataArray);
            }
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.INTEGER)) {
            if (className.equalsIgnoreCase(Constants.Classes.LONG)) {
                return Utils.createLongArray(dataArray);
            } else if (className.equalsIgnoreCase(Constants.Classes.BIG_DECIMAL)) {
                return Utils.decimalToIntArray(dataArray);
            } else {
                return Utils.createIntegerArray(dataArray);
            }
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.FLOAT)) {
            if (className.equalsIgnoreCase(Constants.Classes.BIG_DECIMAL)) {
                return Utils.floatToFloatArray(dataArray);
            } else if (className.equalsIgnoreCase(Constants.Classes.DOUBLE)) {
                return Utils.createDoubleArray(dataArray);
            } else {
                return Utils.createFloatArray(dataArray);
            }
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.DECIMAL)) {
            if (className.equalsIgnoreCase(Constants.Classes.BIG_DECIMAL)) {
                return Utils.createBigDecimalArray(dataArray);
            } else {
                return Utils.createDoubleArray(dataArray);
            }
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.CIVIL)) {
            return Utils.createTimestampArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.UTC)) {
            return Utils.createTimestampArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.TIME_OF_DAY)) {
            return Utils.createTimeArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.DATE)) {
            return Utils.createDateArray(dataArray);
        } else {
            return processCustomArrayInOutParameter(dataArray, ballerinaType);
        }
    }

    public abstract Object processCustomArrayInOutParameter(Object[] dataArray, Type ballerinaType)
            throws ApplicationError;

    public abstract Object processCustomArrayOutParameter(Object[] dataArray, Type ballerinaType)
            throws ApplicationError;

    public BObject createRecordIterator(
            ResultSet resultSet, Statement statement, Connection connection,
            List<ColumnDefinition> columnDefinitions, StructureType streamConstraint) {
        BObject resultIterator = ValueCreator.createObjectValue(ModuleUtils.getModule(),
                Constants.RESULT_ITERATOR_OBJECT, null, getBalStreamResultIterator());
        resultIterator.addNativeData(Constants.RESULT_SET_NATIVE_DATA_FIELD, resultSet);
        resultIterator.addNativeData(Constants.STATEMENT_NATIVE_DATA_FIELD, statement);
        resultIterator.addNativeData(Constants.CONNECTION_NATIVE_DATA_FIELD, connection);
        resultIterator.addNativeData(Constants.COLUMN_DEFINITIONS_DATA_FIELD, columnDefinitions);
        resultIterator.addNativeData(Constants.RECORD_TYPE_DATA_FIELD, streamConstraint);
        return resultIterator;
    }

    public BMap<BString, Object> createRecord(
            ResultSet resultSet,  List<ColumnDefinition> columnDefinitions, StructureType recordConstraint)
            throws SQLException, ApplicationError, IOException {
        BMap<BString, Object> record = ValueCreator.createMapValue(recordConstraint);
        DefaultResultParameterProcessor resultParameterProcessor = DefaultResultParameterProcessor.getInstance();
        for (int i = 0; i < columnDefinitions.size(); i++) {
            ColumnDefinition columnDefinition = columnDefinitions.get(i);
            record.put(fromString(columnDefinition.getBallerinaFieldName()),
                    Utils.getResult(resultSet, i + 1, columnDefinition, resultParameterProcessor));
        }
        return record;
    }

    public Object createValue(ResultSet resultSet, int columnIndex, ColumnDefinition columnDefinition)
            throws SQLException, ApplicationError, IOException {
        DefaultResultParameterProcessor resultParameterProcessor = DefaultResultParameterProcessor.getInstance();
        return Utils.getResult(resultSet, columnIndex, columnDefinition, resultParameterProcessor);
    }

    public abstract BObject getBalStreamResultIterator();
}
