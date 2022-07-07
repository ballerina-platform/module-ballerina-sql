/*
 * Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.stdlib.sql.compiler;


import io.ballerina.compiler.api.symbols.ModuleSymbol;
import io.ballerina.compiler.api.symbols.TypeDescKind;
import io.ballerina.compiler.api.symbols.TypeReferenceTypeSymbol;
import io.ballerina.compiler.api.symbols.TypeSymbol;
import io.ballerina.compiler.syntax.tree.ExpressionNode;
import io.ballerina.projects.plugins.SyntaxNodeAnalysisContext;
import io.ballerina.tools.diagnostics.DiagnosticInfo;

import java.sql.Types;
import java.util.Optional;

import static io.ballerina.stdlib.sql.compiler.Constants.TimeRecordTypes.CIVIL;
import static io.ballerina.stdlib.sql.compiler.Constants.TimeRecordTypes.DATE;
import static io.ballerina.stdlib.sql.compiler.Constants.TimeRecordTypes.PACKAGE_NAME;
import static io.ballerina.stdlib.sql.compiler.Constants.TimeRecordTypes.TIME_OF_DAY;
import static io.ballerina.stdlib.sql.compiler.Constants.TimeRecordTypes.UTC;
import static io.ballerina.stdlib.sql.compiler.SQLDiagnosticsCodes.SQL_201;
import static io.ballerina.stdlib.sql.compiler.SQLDiagnosticsCodes.SQL_202;
import static io.ballerina.stdlib.sql.compiler.SQLDiagnosticsCodes.SQL_203;
import static io.ballerina.stdlib.sql.compiler.SQLDiagnosticsCodes.SQL_204;
import static io.ballerina.stdlib.sql.compiler.SQLDiagnosticsCodes.SQL_211;
import static io.ballerina.stdlib.sql.compiler.SQLDiagnosticsCodes.SQL_212;
import static io.ballerina.stdlib.sql.compiler.SQLDiagnosticsCodes.SQL_213;
import static io.ballerina.stdlib.sql.compiler.SQLDiagnosticsCodes.SQL_214;
import static io.ballerina.stdlib.sql.compiler.SQLDiagnosticsCodes.SQL_215;
import static io.ballerina.stdlib.sql.compiler.SQLDiagnosticsCodes.SQL_221;
import static io.ballerina.stdlib.sql.compiler.SQLDiagnosticsCodes.SQL_222;
import static io.ballerina.stdlib.sql.compiler.SQLDiagnosticsCodes.SQL_223;
import static io.ballerina.stdlib.sql.compiler.SQLDiagnosticsCodes.SQL_231;

/**
 * Utils method for Compiler plugin.
 */
public class Utils {

    private Utils() {
    }

    public static boolean isSQLOutParameter(SyntaxNodeAnalysisContext ctx, ExpressionNode node) {
        Optional<TypeSymbol> objectType = ctx.semanticModel().typeOf(node);
        if (objectType.isEmpty()) {
            return false;
        }
        if (objectType.get() instanceof TypeReferenceTypeSymbol) {
            return isSQLOutParameter(((TypeReferenceTypeSymbol) objectType.get()));
        }
        return false;
    }

    public static boolean isSQLOutParameter(TypeReferenceTypeSymbol typeReference) {
        Optional<ModuleSymbol> optionalModuleSymbol = typeReference.getModule();
        if (optionalModuleSymbol.isEmpty()) {
            return false;
        }
        ModuleSymbol module = optionalModuleSymbol.get();
        if (!(module.id().orgName().equals(Constants.BALLERINA) && module.id().moduleName().equals(Constants.SQL))) {
            return false;
        }
        String objectName = typeReference.definition().getName().get();
        return objectName.equals(Constants.INOUT_PARAMETER) || objectName.endsWith(Constants.OUT_PARAMETER_POSTFIX);
    }

    public static DiagnosticInfo addDiagnosticsForInvalidTypes(String outParameterName,
                                                               TypeSymbol argumentTypeSymbol) {
        TypeDescKind requestedReturnType = argumentTypeSymbol.typeKind();
        String requestedReturnTypeArgValue = "";
        if (requestedReturnType == TypeDescKind.TYPE_REFERENCE) {
            TypeSymbol typeDescriptor = ((TypeReferenceTypeSymbol) argumentTypeSymbol).typeDescriptor();
            requestedReturnType = typeDescriptor.typeKind();
            requestedReturnTypeArgValue = argumentTypeSymbol.signature();
        }

        int sqlTypeValue;
        switch (outParameterName) {
            case Constants.OutParameter.VARCHAR:
            case Constants.OutParameter.TEXT:
                sqlTypeValue = Types.VARCHAR;
                break;
            case Constants.OutParameter.CHAR:
                sqlTypeValue = Types.CHAR;
                break;
            case Constants.OutParameter.NCHAR:
                sqlTypeValue = Types.NCHAR;
                break;
            case Constants.OutParameter.NVARCHAR:
                sqlTypeValue = Types.NVARCHAR;
                break;
            case Constants.OutParameter.BIT:
                sqlTypeValue = Types.BIT;
                break;
            case Constants.OutParameter.BOOLEAN:
                sqlTypeValue = Types.BOOLEAN;
                break;
            case Constants.OutParameter.INTEGER:
                sqlTypeValue = Types.INTEGER;
                break;
            case Constants.OutParameter.BIGINT:
                sqlTypeValue = Types.BIGINT;
                break;
            case Constants.OutParameter.SMALLINT:
                sqlTypeValue = Types.SMALLINT;
                break;
            case Constants.OutParameter.FLOAT:
                sqlTypeValue = Types.FLOAT;
                break;
            case Constants.OutParameter.REAL:
                sqlTypeValue = Types.REAL;
                break;
            case Constants.OutParameter.DOUBLE:
                sqlTypeValue = Types.DOUBLE;
                break;
            case Constants.OutParameter.NUMERIC:
                sqlTypeValue = Types.NUMERIC;
                break;
            case Constants.OutParameter.DECIMAL:
                sqlTypeValue = Types.DECIMAL;
                break;
            case Constants.OutParameter.BINARY:
                sqlTypeValue = Types.BINARY;
                break;
            case Constants.OutParameter.VARBINARY:
                sqlTypeValue = Types.VARBINARY;
                break;
            case Constants.OutParameter.BLOB:
                if (requestedReturnType == TypeDescKind.ARRAY) {
                    sqlTypeValue = Types.VARBINARY;
                } else {
                    sqlTypeValue = Types.LONGVARBINARY;
                }
                break;
            case Constants.OutParameter.CLOB:
            case Constants.OutParameter.NCLOB:
                if (requestedReturnType == TypeDescKind.STRING) {
                    sqlTypeValue = Types.CLOB;
                } else {
                    sqlTypeValue = Types.LONGVARCHAR;
                }
                break;
            case Constants.OutParameter.DATE:
                sqlTypeValue = Types.DATE;
                break;
            case Constants.OutParameter.TIME:
                sqlTypeValue = Types.TIME;
                break;
            case Constants.OutParameter.TIME_WITH_TIMEZONE:
                sqlTypeValue = Types.TIME_WITH_TIMEZONE;
                break;
            case Constants.OutParameter.TIMESTAMP:
            case Constants.OutParameter.DATE_TIME:
                sqlTypeValue = Types.TIMESTAMP;
                break;
            case Constants.OutParameter.TIMESTAMP_WITH_TIMEZONE:
                sqlTypeValue = Types.TIMESTAMP_WITH_TIMEZONE;
                break;
            case Constants.OutParameter.ARRAY:
            case Constants.OutParameter.SMALL_INT_ARRAY:
            case Constants.OutParameter.BIGINT_ARRAY:
            case Constants.OutParameter.BINARY_ARRAY:
            case Constants.OutParameter.BIT_ARRAY:
            case Constants.OutParameter.BOOLEAN_ARRAY:
            case Constants.OutParameter.CHAR_ARRAY:
            case Constants.OutParameter.DATE_ARRAY:
            case Constants.OutParameter.DATE_TIME_ARRAY:
            case Constants.OutParameter.DECIMAL_ARRAY:
            case Constants.OutParameter.DOUBLE_ARRAY:
            case Constants.OutParameter.FLOAT_ARRAY:
            case Constants.OutParameter.INTEGER_ARRAY:
            case Constants.OutParameter.NUMERIC_ARRAY:
            case Constants.OutParameter.NVARCHAR_ARRAY:
            case Constants.OutParameter.TIME_WITH_TIMEZONE_ARRAY:
            case Constants.OutParameter.TIMESTAMP_WITH_TIMEZONE_ARRAY:
            case Constants.OutParameter.TIMESTAMP_ARRAY:
            case Constants.OutParameter.REAL_ARRAY:
            case Constants.OutParameter.VARBINARY_ARRAY:
            case Constants.OutParameter.VARCHAR_ARRAY:
            case Constants.OutParameter.TIME_ARRAY:
                sqlTypeValue = Types.ARRAY;
                break;
            case Constants.OutParameter.REF:
                sqlTypeValue = Types.REF;
                break;
            case Constants.OutParameter.STRUCT:
                sqlTypeValue = Types.STRUCT;
                break;
            case Constants.OutParameter.ROW:
                sqlTypeValue = Types.ROWID;
                break;
            case Constants.OutParameter.XML:
                sqlTypeValue = Types.SQLXML;
                break;
            default:
                return null;
        }

        switch (sqlTypeValue) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.CLOB:
                if (requestedReturnType == TypeDescKind.STRING ||
                    requestedReturnType == TypeDescKind.JSON) {
                    return null;
                }
                return new DiagnosticInfo(SQL_211.getCode(), SQL_211.getMessage(), SQL_211.getSeverity());
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                // todo check array element type byte
                if (requestedReturnType == TypeDescKind.ARRAY ||
                        requestedReturnType == TypeDescKind.STRING) {
                    return null;
                }
                return new DiagnosticInfo(SQL_212.getCode(), SQL_212.getMessage(), SQL_212.getSeverity());
            case Types.ARRAY:
                // todo check array element type
                if (requestedReturnType == TypeDescKind.ARRAY) {
                    return null;
                }
                return new DiagnosticInfo(SQL_201.getCode(), SQL_201.getMessage(), SQL_201.getSeverity());
            case Types.DATE:
                if ((requestedReturnType == TypeDescKind.RECORD && requestedReturnTypeArgValue.startsWith(PACKAGE_NAME)
                        && requestedReturnTypeArgValue.endsWith(DATE)) ||
                        requestedReturnType == TypeDescKind.INT ||
                        requestedReturnType == TypeDescKind.STRING) {
                    return null;
                }
                return new DiagnosticInfo(SQL_222.getCode(), SQL_222.getMessage(), SQL_222.getSeverity());
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                if ((requestedReturnType == TypeDescKind.RECORD && requestedReturnTypeArgValue.startsWith(PACKAGE_NAME)
                        && requestedReturnTypeArgValue.endsWith(TIME_OF_DAY)) ||
                        requestedReturnType == TypeDescKind.INT ||
                        requestedReturnType == TypeDescKind.STRING) {
                    return null;
                }
                return new DiagnosticInfo(SQL_223.getCode(), SQL_223.getMessage(), SQL_223.getSeverity());
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                if ((requestedReturnType == TypeDescKind.RECORD && requestedReturnTypeArgValue.startsWith(PACKAGE_NAME)
                        && requestedReturnTypeArgValue.endsWith(CIVIL)) ||
                    (requestedReturnType == TypeDescKind.INTERSECTION
                            && requestedReturnTypeArgValue.startsWith(PACKAGE_NAME)
                            && requestedReturnTypeArgValue.endsWith(UTC)) ||
                        requestedReturnType == TypeDescKind.INT ||
                        requestedReturnType == TypeDescKind.STRING) {
                    return null;
                }
                return new DiagnosticInfo(SQL_231.getCode(), SQL_231.getMessage(), SQL_231.getSeverity());
            case Types.ROWID:
                // todo check array element type byte
                if (requestedReturnType == TypeDescKind.ARRAY) {
                    return null;
                }
                return new DiagnosticInfo(SQL_204.getCode(), SQL_204.getMessage(), SQL_204.getSeverity());
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
                if (requestedReturnType == TypeDescKind.INT ||
                        requestedReturnType == TypeDescKind.STRING) {
                    return null;
                }
                return new DiagnosticInfo(SQL_213.getCode(), SQL_213.getMessage(), SQL_213.getSeverity());
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
                if (requestedReturnType == TypeDescKind.FLOAT ||
                        requestedReturnType == TypeDescKind.STRING) {
                    return null;
                }
                return new DiagnosticInfo(SQL_214.getCode(), SQL_214.getMessage(), SQL_214.getSeverity());
            case Types.NUMERIC:
            case Types.DECIMAL:
                if (requestedReturnType == TypeDescKind.DECIMAL ||
                        requestedReturnType == TypeDescKind.STRING) {
                    return null;
                }
                return new DiagnosticInfo(SQL_215.getCode(), SQL_215.getMessage(), SQL_215.getSeverity());
            case Types.BIT:
            case Types.BOOLEAN:
                if (requestedReturnType == TypeDescKind.BOOLEAN ||
                        requestedReturnType == TypeDescKind.INT ||
                        requestedReturnType == TypeDescKind.STRING) {
                    return null;
                }
                return new DiagnosticInfo(SQL_221.getCode(), SQL_221.getMessage(), SQL_221.getSeverity());
            case Types.REF:
            case Types.STRUCT:
                if (requestedReturnType == TypeDescKind.RECORD) {
                    return null;
                }
                return new DiagnosticInfo(SQL_202.getCode(), SQL_202.getMessage(), SQL_202.getSeverity());
            case Types.SQLXML:
                if (requestedReturnType == TypeDescKind.XML) {
                    return null;
                }
                return new DiagnosticInfo(SQL_203.getCode(), SQL_203.getMessage(), SQL_203.getSeverity());
            default:
                return null;
        }
    }
}
