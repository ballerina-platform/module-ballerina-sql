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

package io.ballerina.stdlib.sql.tests.utils;

import io.ballerina.runtime.api.PredefinedTypes;
import io.ballerina.runtime.api.types.StructureType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BLink;
import io.ballerina.runtime.api.values.BValue;
import io.ballerina.stdlib.sql.exception.ApplicationError;
import io.ballerina.stdlib.sql.tests.TestUtils;
import io.ballerina.stdlib.sql.utils.ColumnDefinition;
import io.ballerina.stdlib.sql.utils.Utils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Utils class test.
 *
 * @since 0.6.0-beta.2
 */
public class UtilsTest {
    @Test
    void throwInvalidParameterErrorTest1() {
       Integer intVal = 1;
       ApplicationError error = Utils.throwInvalidParameterError(intVal, "Integer");
       assertEquals(error.getMessage(),
               "Invalid parameter :java.lang.Integer is passed as value for SQL type : Integer");
    }

    @Test
    void createTimeStructTest() {
        try {
            BArray bArray = Utils.createTimeStruct(1400020000);
        } catch (UnsupportedOperationException e) {
            assertEquals(e.getMessage(), "java.lang.UnsupportedOperationException");
        }
    }

    @Test
    void throwInvalidParameterErrorTest() {
        BValue bValue = new BValue() {
            @Override
            public Object copy(Map<Object, Object> map) {
                return null;
            }

            @Override
            public Object frozenCopy(Map<Object, Object> map) {
                return null;
            }

            @Override
            public String stringValue(BLink bLink) {
                return null;
            }

            @Override
            public String expressionStringValue(BLink bLink) {
                return null;
            }

            @Override
            public Type getType() {
                return TypeUtils.getType(1);
            }
        };

        ApplicationError applicationError = Utils.throwInvalidParameterError(bValue, "INTEGER");
        assertEquals(applicationError.getMessage(), "Invalid parameter :byte is passed as value for " +
                "SQL type : INTEGER");
    }

    @Test
    void getDefaultRecordTypeTest() {
        ColumnDefinition columnDefinition1 = new TestUtils.ExtendedColumnDefinition("int_type", null,
                2, "INT", TypeUtils.getType(4), false);
        ColumnDefinition columnDefinition2 = new TestUtils.ExtendedColumnDefinition("string_type", null,
                2, "STRING", TypeUtils.getType(12), true);
        List<ColumnDefinition> list = new ArrayList<>();
        list.add(columnDefinition1);
        list.add(columnDefinition2);
        StructureType structureType = Utils.getDefaultRecordType(list);
        assertEquals(structureType.getFlags(), 0);
    }

    @Test
    void validatedInvalidFieldAssignmentTest() {
        try {
            Utils.validatedInvalidFieldAssignment(1, PredefinedTypes.TYPE_INT, "New Type");
        } catch (ApplicationError e) {
            assertEquals(e.getMessage(), "SQL Type 'New Type' cannot be converted to ballerina type 'int'.");
        }
    }

    @Test
    void validatedInvalidFieldAssignmentTest1() {
        try {
            Utils.validatedInvalidFieldAssignment(1, PredefinedTypes.TYPE_CLONEABLE, "New Type");
        } catch (ApplicationError e) {
            assertEquals(e.getMessage(), "SQL Type 'New Type' cannot be converted to ballerina type 'Cloneable'.");
        }
    }

    @Test
    void validatedInvalidFieldAssignmentTest2() {
        try {
            Utils.validatedInvalidFieldAssignment(100, TestUtils.getBooleanStructRecord(), "New Type");
        } catch (Exception ignored) {
            fail("Exception received");
        }
    }
}
