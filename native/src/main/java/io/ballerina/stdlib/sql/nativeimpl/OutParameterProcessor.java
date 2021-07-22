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

package io.ballerina.stdlib.sql.nativeimpl;

import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BTypedesc;
import io.ballerina.stdlib.sql.Constants;
import io.ballerina.stdlib.sql.exception.ApplicationError;
import io.ballerina.stdlib.sql.parameterprocessor.DefaultResultParameterProcessor;
import io.ballerina.stdlib.sql.utils.ErrorGenerator;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.OffsetDateTime;
import java.time.OffsetTime;

import static io.ballerina.stdlib.sql.utils.Utils.getString;

/**
 * This class provides the implementation of processing InOut/Out parameters of procedure calls.
 *
 * @since 0.5.6
 */
public class OutParameterProcessor {

    private OutParameterProcessor() {
    }

    public static Object get(BObject result, BTypedesc typeDesc) {
        return get(result, typeDesc, DefaultResultParameterProcessor.getInstance());
    }

    public static Object get(BObject result, BTypedesc typeDesc,
                             DefaultResultParameterProcessor resultParameterProcessor) {
        int sqlType = (int) result.getNativeData(Constants.ParameterObject.SQL_TYPE_NATIVE_DATA);
        Object value = result.getNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA);
        Type ballerinaType = typeDesc.getDescribingType();
        try {
            switch (sqlType) {
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR:
                    return resultParameterProcessor.convertChar((String) value, sqlType, ballerinaType);
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    return resultParameterProcessor.convertBinary(value, sqlType, ballerinaType);
                case Types.ARRAY:
                    Array array = (Array) value;
                    Object[] dataArray = (Object[]) array.getArray();
                    if (dataArray == null || dataArray.length == 0) {
                        return null;
                    }  else {
                        return convertToArray(result.getType().getName(), dataArray, ballerinaType);
                    }
                case Types.BLOB:
                    return resultParameterProcessor.convertBlob((Blob) value, sqlType, ballerinaType);
                case Types.CLOB:
                    String clobValue = getString((Clob) value);
                    return resultParameterProcessor.convertChar(clobValue, sqlType, ballerinaType);
                case Types.NCLOB:
                    String nClobValue = getString((NClob) value);
                    return resultParameterProcessor.convertChar(nClobValue, sqlType, ballerinaType);
                case Types.DATE:
                    return resultParameterProcessor.convertDate((Date) value, sqlType, ballerinaType);
                case Types.TIME:
                    return resultParameterProcessor.convertTime((Time) value, sqlType, ballerinaType);
                case Types.TIME_WITH_TIMEZONE:
                    return resultParameterProcessor.convertTimeWithTimezone((OffsetTime) value, sqlType,
                            ballerinaType);
                case Types.TIMESTAMP:
                    return resultParameterProcessor.convertTimeStamp((Timestamp) value, sqlType, ballerinaType);
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    return resultParameterProcessor.convertTimestampWithTimezone((OffsetDateTime) value, sqlType,
                            ballerinaType);
                case Types.ROWID:
                    return resultParameterProcessor.convertByteArray(
                            ((RowId) value).getBytes(), sqlType, ballerinaType, "SQL RowID"
                    );
                case Types.TINYINT:
                case Types.SMALLINT:
                    if (value == null) {
                        return null;
                    }
                    return resultParameterProcessor.convertInteger((int) value, sqlType, ballerinaType, false);
                case Types.INTEGER:
                case Types.BIGINT:
                    if (value == null) {
                        return null;
                    }
                    return resultParameterProcessor.convertInteger((long) value, sqlType, ballerinaType, false);
                case Types.REAL:
                case Types.FLOAT:
                    if (value == null) {
                        return null;
                    }
                    return resultParameterProcessor.convertDouble((float) value, sqlType, ballerinaType, false);
                case Types.DOUBLE:
                    if (value == null) {
                        return null;
                    }
                    return resultParameterProcessor.convertDouble((double) value, sqlType, ballerinaType, false);
                case Types.NUMERIC:
                case Types.DECIMAL:
                    if (value == null) {
                        return null;
                    }
                    return resultParameterProcessor.convertDecimal((BigDecimal) value, sqlType, ballerinaType, false);
                case Types.BIT:
                case Types.BOOLEAN:
                    if (value == null) {
                        return null;
                    }
                    return resultParameterProcessor.convertBoolean((boolean) value, sqlType, ballerinaType, false);
                case Types.REF:
                case Types.STRUCT:
                    return resultParameterProcessor.convertStruct((Struct) value, sqlType, ballerinaType);
                case Types.SQLXML:
                    return resultParameterProcessor.convertXml((SQLXML) value, sqlType, ballerinaType);
                default:
                    return resultParameterProcessor.getCustomOutParameters(result, sqlType, ballerinaType);
            }
        } catch (ApplicationError | IOException applicationError) {
            return ErrorGenerator.getSQLApplicationError(applicationError.getMessage());
        } catch (SQLException sqlException) {
            return ErrorGenerator.getSQLDatabaseError(sqlException, "Error when parsing out parameter.");
        }
    }

    private static Object convertToArray(String objectTypeName, Object[] dataArray, Type ballerinaType)
            throws SQLException {
        String name = ballerinaType.toString();
        switch (objectTypeName) {
            case Constants.OutParameterTypes.CHAR_ARRAY:
            case Constants.OutParameterTypes.VARCHAR_ARRAY:
            case Constants.OutParameterTypes.NVARCHAR_ARRAY:
                return stringToArray(name, dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.BIT_ARRAY:
                return bitToArray(name, dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.BOOLEAN_ARRAY:
                return booleanToArray(name, dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.INTEGER_ARRAY:
            case Constants.OutParameterTypes.SMALL_INT_ARRAY:
            case Constants.OutParameterTypes.BIGINT_ARRAY:
                return intToArray(name, dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.REAL_ARRAY:
            case Constants.OutParameterTypes.DOUBLE_ARRAY:
                return realToArray(name, dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.DATE_ARRAY:
                return dateToArray(name, dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.TIME_ARRAY:
                return timeToArray(name, dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.TIME_WITH_TIMEZONE_ARRAY:
                return timeWithTimezoneToArray(name, dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.DATE_TIME_ARRAY:
                return dateTimeToArray(name, dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.TIMESTAMP_WITH_TIMEZONE_ARRAY:
                return timestampWithTimezoneToArray(name, dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.FLOAT_ARRAY:
                return floatToArray(name, dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.DECIMAL_ARRAY:
            case Constants.OutParameterTypes.NUMERIC_ARRAY:
                return numericToArray(name, dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.TIMESTAMP_ARRAY:
                return timestampToArray(name, dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.VARBINARY_ARRAY:
            case Constants.OutParameterTypes.BINARY_ARRAY:
                return binaryToArray(name, dataArray, objectTypeName, ballerinaType);
            case Constants.OutParameterTypes.ARRAY:
            case Constants.ParameterObject.INOUT_PARAMETER:
                return toArray(name, dataArray, ballerinaType);
            default:
                return ErrorGenerator.getSQLApplicationError("Unsupported SQL type " + objectTypeName);
        }
    }

    private static Object stringToArray(String name, Object[] dataArray, String objectTypeName, Type ballerinaType) {
        if (name.equalsIgnoreCase(Constants.ArrayTypes.STRING)) {
            return DefaultResultParameterProcessor.createStringArray(dataArray);
        } else {
            return getError(ballerinaType, objectTypeName);
        }
    }

    private static Object booleanToArray(String name, Object[] dataArray, String objectTypeName, Type ballerinaType) {
        if (name.equalsIgnoreCase(Constants.ArrayTypes.INTEGER)) {
            return booleanToIntArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.BOOLEAN)) {
            return DefaultResultParameterProcessor.createBooleanArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.STRING)) {
            return DefaultResultParameterProcessor.createStringArray(dataArray);
        } else {
            return getError(ballerinaType, objectTypeName);
        }
    }

    private static Object bitToArray(String name, Object[] dataArray, String objectTypeName, Type ballerinaType) {
        if (name.equalsIgnoreCase(Constants.ArrayTypes.INTEGER)) {
            return booleanToIntArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.BOOLEAN)) {
            return DefaultResultParameterProcessor.createBooleanArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.STRING)) {
            return DefaultResultParameterProcessor.createStringArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.BYTE)) {
            return DefaultResultParameterProcessor.createByteArray(dataArray);
        } else {
            return getError(ballerinaType, objectTypeName);
        }
    }

    private static Object booleanToIntArray(Object[] dataArray) {
        BArray intDataArray = ValueCreator.createArrayValue(Constants.ArrayTypes.INT_ARRAY);
        for (int i = 0; i < dataArray.length; i++) {
            int val = ((Boolean) dataArray[i]) ? 1 : 0;
            intDataArray.add(i, val);
        }
        return intDataArray;
    }

    private static Object intToArray(String name, Object[] dataArray, String objectTypeName, Type ballerinaType) {
        String className = dataArray[0].getClass().getCanonicalName();
        if (name.equalsIgnoreCase(Constants.ArrayTypes.INTEGER) &&
                className.equalsIgnoreCase(Constants.Classes.INTEGER)) {
            return DefaultResultParameterProcessor.createIntegerArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.INTEGER) &&
                className.equalsIgnoreCase(Constants.Classes.LONG)) {
            return DefaultResultParameterProcessor.createLongArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.STRING)) {
            return DefaultResultParameterProcessor.createStringArray(dataArray);
        } else {
            return getError(ballerinaType, objectTypeName);
        }
    }

    private static Object realToArray(String name, Object[] dataArray, String objectTypeName, Type ballerinaType) {
        if (name.equalsIgnoreCase(Constants.ArrayTypes.INTEGER)) {
            BArray intDataArray = ValueCreator.createArrayValue(Constants.ArrayTypes.INT_ARRAY);
            for (int i = 0; i < dataArray.length; i++) {
                intDataArray.add(i, ((Double) dataArray[i]).intValue());
            }
            return intDataArray;
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.STRING)) {
            return DefaultResultParameterProcessor.createStringArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.DECIMAL)) {
            return toDecimalArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.FLOAT)) {
            return toFloatArray(dataArray);
        } else {
            return getError(ballerinaType, objectTypeName);
        }
        
    }

    private static Object dateToArray(String name, Object[] dataArray, String objectTypeName, Type ballerinaType) {
        if (name.equalsIgnoreCase(Constants.ArrayTypes.STRING)) {
            return DefaultResultParameterProcessor.createStringArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.DATE)) {
            return DefaultResultParameterProcessor.createDateArray(dataArray);
        } else {
            return getError(ballerinaType, objectTypeName);
        }
        
    }

    private static Object timeToArray(String name, Object[] dataArray, String objectTypeName,
                                      Type ballerinaType) {
        if (name.equalsIgnoreCase(Constants.ArrayTypes.STRING)) {
            return DefaultResultParameterProcessor.createStringArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.TIME_OF_DAY)) {
            return DefaultResultParameterProcessor.createTimeArray(dataArray);
        } else {
            return getError(ballerinaType, objectTypeName);
        }
        
    }

    private static Object toDecimalArray(Object[] dataArray) {
        BArray decimalDataArray = ValueCreator.createArrayValue(Constants.ArrayTypes.DECIMAL_ARRAY);
        for (int i = 0; i < dataArray.length; i++) {
            Double doubleValue = (Double) dataArray[i];
            decimalDataArray.add(i, ValueCreator.createDecimalValue(BigDecimal.valueOf(doubleValue)));
        }
        return decimalDataArray;
    }

    private static Object toFloatArray(Object[] dataArray) {
        BArray floatDataArray = ValueCreator.createArrayValue(Constants.ArrayTypes.FLOAT_ARRAY);
        for (int i = 0; i < dataArray.length; i++) {
            floatDataArray.add(i, ((Double) dataArray[i]).floatValue());
        }
        return floatDataArray;
    }

    private static Object timeWithTimezoneToArray(String name, Object[] dataArray, String objectTypeName,
                                                  Type ballerinaType) {
        if (name.equalsIgnoreCase(Constants.ArrayTypes.STRING)) {
            return DefaultResultParameterProcessor.createStringArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.TIME_OF_DAY)) {
            return DefaultResultParameterProcessor.createOffsetArray(dataArray);
        } else {
            return getError(ballerinaType, objectTypeName);
        }
        
    }

    private static Object dateTimeToArray(String name, Object[] dataArray, String objectTypeName,
                                          Type ballerinaType) {
        if (name.equalsIgnoreCase(Constants.ArrayTypes.STRING)) {
            return DefaultResultParameterProcessor.createStringArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.CIVIL)) {
            return DefaultResultParameterProcessor.createTimestampArray(dataArray);
        } else {
            return getError(ballerinaType, objectTypeName);
        }
        
    }
    private static Object timestampWithTimezoneToArray(String name, Object[] dataArray, String objectTypeName,
                                                       Type ballerinaType) {
        if (name.equalsIgnoreCase(Constants.ArrayTypes.STRING)) {
            return DefaultResultParameterProcessor.createStringArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.CIVIL)) {
            return DefaultResultParameterProcessor.createOffsetTimeArray(dataArray);
        } else {
            return getError(ballerinaType, objectTypeName);
        }
        
    }

    private static Object floatToArray(String name, Object[] dataArray, String objectTypeName,
                                         Type ballerinaType) {
        if (name.equalsIgnoreCase(Constants.ArrayTypes.INTEGER)) {
            BArray intDataArray = ValueCreator.createArrayValue(Constants.ArrayTypes.INT_ARRAY);
            for (int i = 0; i < dataArray.length; i++) {
                Double doubleValue = (Double) dataArray[i];
                intDataArray.add(i, doubleValue.intValue());
            }
            return intDataArray;
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.FLOAT)) {
            return DefaultResultParameterProcessor.createFloatArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.STRING)) {
            return DefaultResultParameterProcessor.createStringArray(dataArray);
        } else {
            return getError(ballerinaType, objectTypeName);
        }
        
    }

    private static Object numericToArray(String name, Object[] dataArray, String objectTypeName,
                                         Type ballerinaType) {
        if (name.equalsIgnoreCase(Constants.ArrayTypes.STRING)) {
            return DefaultResultParameterProcessor.createStringArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.DECIMAL)) {
            return DefaultResultParameterProcessor.createBigDecimalArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.FLOAT)) {
            return floatToFloatArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.INTEGER)) {
            return decimalToIntArray(dataArray);
        } else {
            return getError(ballerinaType, objectTypeName);
        }
    }

    private static Object timestampToArray(String name, Object[] dataArray, String objectTypeName,
                                         Type ballerinaType) {
        if (name.equalsIgnoreCase(Constants.ArrayTypes.STRING)) {
            return DefaultResultParameterProcessor.createStringArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.UTC)) {
            return DefaultResultParameterProcessor.createTimestampArray(dataArray);
        } else {
            return getError(ballerinaType, objectTypeName);
        }
    }

    private static Object binaryToArray(String name, Object[] dataArray, String objectTypeName,
                                         Type ballerinaType) {
        if (name.equalsIgnoreCase(Constants.ArrayTypes.STRING)) {
            return DefaultResultParameterProcessor.createStringArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.BYTE)) {
            return DefaultResultParameterProcessor.createByteArray(dataArray);
        } else {
            return getError(ballerinaType, objectTypeName);
        }
    }

    private static Object toArray(String name, Object[] dataArray, Type ballerinaType) {
        String className = dataArray[0].getClass().getCanonicalName();
        if (name.equalsIgnoreCase(Constants.ArrayTypes.STRING)) {
            return DefaultResultParameterProcessor.createStringArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.BYTE) &&
                className.equalsIgnoreCase(Constants.Classes.BYTE)) {
            return DefaultResultParameterProcessor.createByteArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.BOOLEAN)) {
            if (className.equalsIgnoreCase(Constants.Classes.INTEGER)) {
                return booleanToIntArray(dataArray);
            } else {
                return DefaultResultParameterProcessor.createBooleanArray(dataArray);
            }
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.INTEGER)) {
            if (className.equalsIgnoreCase(Constants.Classes.LONG)) {
                return DefaultResultParameterProcessor.createLongArray(dataArray);
            } else if (className.equalsIgnoreCase(Constants.Classes.BIG_DECIMAL)) {
                return decimalToIntArray(dataArray);
            } else {
                return DefaultResultParameterProcessor.createIntegerArray(dataArray);
            }
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.FLOAT)) {
            if (className.equalsIgnoreCase(Constants.Classes.BIG_DECIMAL)) {
                return floatToFloatArray(dataArray);
            } else if (className.equalsIgnoreCase(Constants.Classes.DOUBLE)) {
                return DefaultResultParameterProcessor.createDoubleArray(dataArray);
            } else {
                return DefaultResultParameterProcessor.createFloatArray(dataArray);
            }
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.DECIMAL)) {
            if (className.equalsIgnoreCase(Constants.Classes.BIG_DECIMAL) ||
                    className.equalsIgnoreCase(Constants.Classes.OBJECT)) {
                return DefaultResultParameterProcessor.createBigDecimalArray(dataArray);
            } else {
                return DefaultResultParameterProcessor.createDoubleArray(dataArray);
            }
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.CIVIL)) {
            return DefaultResultParameterProcessor.createTimestampArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.UTC)) {
            return DefaultResultParameterProcessor.createTimestampArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.TIME_OF_DAY)) {
            return DefaultResultParameterProcessor.createTimeArray(dataArray);
        } else if (name.equalsIgnoreCase(Constants.ArrayTypes.DATE)) {
            return DefaultResultParameterProcessor.createDateArray(dataArray);
        } else {
            return ErrorGenerator.getSQLApplicationError("Unsupported Ballerina type:" +
                    ballerinaType + " for SQL Date data type.");
        }
    }

    private static Object floatToFloatArray(Object[] dataArray) {
        BArray floatDataArray = ValueCreator.createArrayValue(Constants.ArrayTypes.FLOAT_ARRAY);
        for (int i = 0; i < dataArray.length; i++) {
            floatDataArray.add(i, ((BigDecimal) dataArray[i]).floatValue());
        }
        return floatDataArray;
    }

    private static Object decimalToIntArray(Object[] dataArray) {
        BArray intDataArray = ValueCreator.createArrayValue(Constants.ArrayTypes.INT_ARRAY);
        for (int i = 0; i < dataArray.length; i++) {
            intDataArray.add(i, ((BigDecimal) dataArray[i]).intValue());
        }
        return intDataArray;
    }

    private static BError getError(Type ballerinaType, String objectTypeName) {
        return ErrorGenerator.getSQLApplicationError("Unsupported Ballerina type:" +
                ballerinaType + " for SQL Date data type:" + objectTypeName.replace("ArrayOutParameter",
                " array"));
    }
}
