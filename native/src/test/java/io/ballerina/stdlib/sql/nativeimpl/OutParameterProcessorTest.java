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

package io.ballerina.stdlib.sql.nativeimpl;

import io.ballerina.runtime.api.PredefinedTypes;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.sql.Constants;
import io.ballerina.stdlib.sql.TestUtils;
import io.ballerina.stdlib.sql.parameterprocessor.DefaultResultParameterProcessor;
import org.testng.annotations.Test;

import static io.ballerina.runtime.api.utils.StringUtils.fromString;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * QueryProcessor class test.
 *
 * @since 0.6.0-beta.2
 */
public class OutParameterProcessorTest {

    @Test
    void getSmallIntNullTest() {
        BObject object = TestUtils.getMockObject("SMALLINT");
        object.addNativeData(Constants.ParameterObject.SQL_TYPE_NATIVE_DATA, 5);
        assertNull(OutParameterProcessor.get(object, TestUtils.getBTypedesc(PredefinedTypes.TYPE_INT),
                DefaultResultParameterProcessor.getInstance(), true));
    }

    @Test
    void getIntegerNullTest() {
        BObject object = TestUtils.getMockObject("INTEGER");
        object.addNativeData(Constants.ParameterObject.SQL_TYPE_NATIVE_DATA, 4);
        assertNull(OutParameterProcessor.get(object, TestUtils.getBTypedesc(PredefinedTypes.TYPE_INT),
                DefaultResultParameterProcessor.getInstance(), true));
    }

    @Test
    void getFloatNullTest() {
        BObject object = TestUtils.getMockObject("FLOAT");
        object.addNativeData(Constants.ParameterObject.SQL_TYPE_NATIVE_DATA, 6);
        assertNull(OutParameterProcessor.get(object, TestUtils.getBTypedesc(PredefinedTypes.TYPE_FLOAT),
                DefaultResultParameterProcessor.getInstance(), true));
    }

    @Test
    void getDoubleNullTest() {
        BObject object = TestUtils.getMockObject("DOUBLE");
        object.addNativeData(Constants.ParameterObject.SQL_TYPE_NATIVE_DATA, 8);
        assertNull(OutParameterProcessor.get(object, TestUtils.getBTypedesc(PredefinedTypes.TYPE_FLOAT),
                DefaultResultParameterProcessor.getInstance(), true));
    }

    @Test
    void getBooleanNullTest() {
        BObject object = TestUtils.getMockObject("BOOLEAN");
        object.addNativeData(Constants.ParameterObject.SQL_TYPE_NATIVE_DATA, 16);
        assertNull(OutParameterProcessor.get(object, TestUtils.getBTypedesc(PredefinedTypes.TYPE_BOOLEAN),
                DefaultResultParameterProcessor.getInstance(), true));
    }

    @Test
    void getStructTest() {
        BObject object = TestUtils.getMockObject("STRUCT");
        object.addNativeData(Constants.ParameterObject.SQL_TYPE_NATIVE_DATA, 2002);
        object.addNativeData(Constants.ParameterObject.VALUE_NATIVE_DATA, TestUtils.getBooleanStruct());
        Object obj = OutParameterProcessor.get(object,
                TestUtils.getBTypedesc(TestUtils.getBooleanStructRecord()),
                DefaultResultParameterProcessor.getInstance(), true);
        BMap<BString, Object> map = (BMap<BString, Object>) obj;
        assertEquals(map.get(fromString("value1")), false);
    }
}
