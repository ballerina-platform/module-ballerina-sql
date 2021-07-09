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
package io.ballerina.stdlib.sql.utils;

import io.ballerina.runtime.api.types.Type;

/**
 * This class provides the mapping of the sql columns, its names and types.
 *
 * @since 1.2.0
 */
public class ColumnDefinition {
    private final String columnName;
    private final String ballerinaFieldName;
    private final int sqlType;
    private final String sqlName;
    private final Type ballerinaType;
    private final boolean isNullable;

    protected ColumnDefinition(String columnName, String ballerinaFieldName, int sqlType, String sqlName,
                     Type ballerinaType, boolean isNullable) {
        this.columnName = columnName;
        if (ballerinaFieldName != null && !ballerinaFieldName.isEmpty()) {
            this.ballerinaFieldName = ballerinaFieldName;
        } else {
            this.ballerinaFieldName = this.columnName;
        }
        this.sqlType = sqlType;
        this.ballerinaType = ballerinaType;
        this.isNullable = isNullable;
        this.sqlName = sqlName;
    }

    public String getColumnName() {
        return columnName;
    }

    public int getSqlType() {
        return sqlType;
    }

    public Type getBallerinaType() {
        return ballerinaType;
    }

    public boolean isNullable() {
        return isNullable;
    }

    public String getBallerinaFieldName() {
        return ballerinaFieldName;
    }

    public String getSqlName() {
        return sqlName;
    }
}
