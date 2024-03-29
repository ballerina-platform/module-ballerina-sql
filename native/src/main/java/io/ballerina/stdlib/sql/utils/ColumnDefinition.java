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
    private final String ballerinaFieldName;
    private final Type ballerinaType;

    protected ColumnDefinition(String ballerinaFieldName, Type ballerinaType) {
        this.ballerinaFieldName = ballerinaFieldName;
        this.ballerinaType = ballerinaType;
    }

    public Type getBallerinaType() {
        return ballerinaType;
    }

    public String getBallerinaFieldName() {
        return ballerinaFieldName;
    }

}
