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

package org.ballerinalang.sql.nativeimpl;

import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BTypedesc;
import org.ballerinalang.sql.Constants;
import org.ballerinalang.sql.exception.ApplicationError;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;



import static org.ballerinalang.sql.utils.Utils.getString;

/**
 * class to process InOut/Out parameters of procedure calls.
 */
public class OutParameterProcessor {

    public static Object get(BObject result, BTypedesc typeDesc) {
        get(result, typeDesc, ResultParameterProcessor.getInstance());
    }

    public static Object get(BObject result, BTypedesc typeDesc, ResultParameterProcessor resultParameterProcessor) {
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
                    return resultParameterProcessor.convert((String) value, sqlType, ballerinaType);
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    if (ballerinaType.getTag() == TypeTags.STRING_TAG) {
                        return resultParameterProcessor.convert((String) value, sqlType, ballerinaType);
                    } else {
                        return resultParameterProcessor.convert(((String) value).getBytes(Charset.defaultCharset()), sqlType, ballerinaType,
                                JDBCType.valueOf(sqlType).getName());
                    }
                case Types.ARRAY:
                    return resultParameterProcessor.convert((Array) value, sqlType, ballerinaType);
                case Types.BLOB:
                    return resultParameterProcessor.convert((Blob) value, sqlType, ballerinaType);
                case Types.CLOB:
                    String clobValue = getString((Clob) value);
                    return resultParameterProcessor.convert(clobValue, sqlType, ballerinaType);
                case Types.NCLOB:
                    String nClobValue = getString((NClob) value);
                    return resultParameterProcessor.convert(nClobValue, sqlType, ballerinaType);
                case Types.DATE:
                    return resultParameterProcessor.convert((Date) value, sqlType, ballerinaType);
                case Types.TIME:
                case Types.TIME_WITH_TIMEZONE:
                    return resultParameterProcessor.convert((Time) value, sqlType, ballerinaType);
                case Types.TIMESTAMP:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                    return resultParameterProcessor.convert((Timestamp) value, sqlType, ballerinaType);
                case Types.ROWID:
                    return resultParameterProcessor.convert(((RowId) value).getBytes(), sqlType, ballerinaType, "SQL RowID");
                case Types.TINYINT:
                case Types.SMALLINT:
                    if (value == null) {
                        return null;
                    }
                    return resultParameterProcessor.convert((int) value, sqlType, ballerinaType, false);
                case Types.INTEGER:
                case Types.BIGINT:
                    if (value == null) {
                        return null;
                    }
                    return resultParameterProcessor.convert((long) value, sqlType, ballerinaType, false);
                case Types.REAL:
                case Types.FLOAT:
                    if (value == null) {
                        return null;
                    }
                    return resultParameterProcessor.convert((float) value, sqlType, ballerinaType, false);
                case Types.DOUBLE:
                    if (value == null) {
                        return null;
                    }
                    return resultParameterProcessor.convert((double) value, sqlType, ballerinaType, false);
                case Types.NUMERIC:
                case Types.DECIMAL:
                    if (value == null) {
                        return null;
                    }
                    return resultParameterProcessor.convert((BigDecimal) value, sqlType, ballerinaType, false);
                case Types.BIT:
                case Types.BOOLEAN:
                    if (value == null) {
                        return null;
                    }
                    return resultParameterProcessor.convert((boolean) value, sqlType, ballerinaType, false);
                case Types.REF:
                case Types.STRUCT:
                    return resultParameterProcessor.convert((Struct) value, sqlType, ballerinaType);
                case Types.SQLXML:
                    return resultParameterProcessor.convert((SQLXML) value, sqlType, ballerinaType);
                default:
                    return resultParameterProcessor.customOutParameter();
                    // return ErrorGenerator.getSQLApplicationError("Unsupported SQL type " + sqlType);
            }
        } catch (ApplicationError | IOException applicationError) {
            return ErrorGenerator.getSQLApplicationError(applicationError.getMessage());
        } catch (SQLException sqlException) {
            return ErrorGenerator.getSQLDatabaseError(sqlException, "Error when parsing out parameter.");
        }
    }
}
