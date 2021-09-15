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
package io.ballerina.stdlib.sql.exception;

import java.util.Arrays;

/**
 * This exception represents the occurs when a query retrieves a result that differs from the expected result type.
 */
public class TypeMismatchError extends DataError {

    public TypeMismatchError(String sqlType, String typeFound, String typeExpected) {
        super(String.format("The ballerina type expected for '%s' type is '%s' but found type '%s'.",
                sqlType, typeExpected, typeFound));
    }

    public TypeMismatchError(String sqlType, String typeFound, String[] typeExpected) {
        super(String.format("The ballerina type expected for '%s' type are '%s' but found type '%s'.",
                sqlType, String.join("', and '",
                String.join("', '", Arrays.copyOf(typeExpected, typeExpected.length - 1)),
                typeExpected[typeExpected.length  - 1]), typeFound));
    }

    public TypeMismatchError(String sqlType, String typeFound) {
        super(String.format("SQL Type '%s' cannot be converted to ballerina type '%s'.", sqlType, typeFound));
    }

    public TypeMismatchError(String message) {
        super(message);
    }

    public TypeMismatchError(String message, Exception error) {
        super(message, error);
    }
}
