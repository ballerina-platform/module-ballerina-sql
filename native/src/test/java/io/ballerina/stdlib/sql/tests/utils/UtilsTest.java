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

import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.StructureType;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.sql.Constants;
import io.ballerina.stdlib.sql.ParameterizedQuery;
import io.ballerina.stdlib.sql.tests.TestUtils;
import io.ballerina.stdlib.sql.utils.PrimitiveTypeColumnDefinition;
import io.ballerina.stdlib.sql.utils.Utils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.ballerina.runtime.api.utils.StringUtils.fromString;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Utils class test.
 *
 * @since 0.6.0-beta.2
 */
public class UtilsTest {

    @Test
    void createTimeStructTest() {
        try {
            Utils.createTimeStruct(1400020000);
        } catch (UnsupportedOperationException e) {
            assertEquals(e.getMessage(), "java.lang.UnsupportedOperationException");
        }
    }

    @Test
    void getDefaultRecordTypeTest() {
        PrimitiveTypeColumnDefinition columnDefinition1 = new TestUtils.ExtendedColumnDefinition("int_type",
                2, "INT", false, 1, null, TypeUtils.getType(4));
        PrimitiveTypeColumnDefinition columnDefinition2 = new TestUtils.ExtendedColumnDefinition("string_type",
                2, "STRING", false, 1, null, TypeUtils.getType(12));
        List<PrimitiveTypeColumnDefinition> list = new ArrayList<>();
        list.add(columnDefinition1);
        list.add(columnDefinition2);
        StructureType structureType = Utils.getDefaultRecordType(list);
        assertEquals(structureType.getFlags(), 0);
    }

    @Test
    void backTickEscapeTest() {
        // HSQLDB does not support backtick quotes
        BObject bParameterizedQuery = TestUtils.getMockObject("parameterizedQuery");

        BString[] bStrings = {fromString("x"), fromString("y"), fromString("z"), fromString("")};

        BString[] insertions = {fromString("`"), fromString("abc"), fromString("`") };


        bParameterizedQuery.addNativeData(Constants.ParameterizedQueryFields.STRINGS.getValue(),
                ValueCreator.createArrayValue(bStrings));
        bParameterizedQuery.addNativeData(Constants.ParameterizedQueryFields.INSERTIONS.getValue(),
                ValueCreator.createArrayValue(insertions));

        ParameterizedQuery parameterizedSQLQuery = Utils.getParameterizedSQLQuery(bParameterizedQuery);
        assertEquals(parameterizedSQLQuery.getSqlQuery(), "x`y ? z`");
    }
}
