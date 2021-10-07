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

import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.stdlib.sql.tests.TestUtils;
import io.ballerina.stdlib.sql.utils.PrimitiveTypeColumnDefinition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * ColumnDefinition class test.
 *
 * @since 0.6.0-beta.2
 */
public class ColumnDefinitionTest {


    @Test
    void isNullableTest() {
        PrimitiveTypeColumnDefinition columnDefinition = new TestUtils.ExtendedColumnDefinition("int_type",
                2, "INT", false, 1, null, TypeUtils.getType(1));
        assertFalse(columnDefinition.isNullable(), "nullable flag is wrong");
    }

    @Test
    void columnNameTest() {
        PrimitiveTypeColumnDefinition columnDefinition = new TestUtils.ExtendedColumnDefinition("int_type",
                2, "INT", false, 1, null, TypeUtils.getType(1));
        assertEquals(columnDefinition.getColumnName(), "int_type", "column name is wrong");
    }
}
