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

package io.ballerina.stdlib.sql.tests.datasource;

import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BValue;
import io.ballerina.runtime.internal.values.MapValueImpl;
import io.ballerina.stdlib.sql.datasource.PoolKey;
import io.ballerina.stdlib.sql.tests.TestUtils;
import org.testng.annotations.Test;

import static io.ballerina.runtime.api.utils.StringUtils.fromString;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * PoolKey class test.
 *
 * @since 0.6.0-beta.2
 */
public class PoolKeyTest {
    @Test
    void poolKeyConstructorTest() {
        BMap<BString, Boolean> options = new MapValueImpl<>();
        options.put(fromString("booleanValue"), true);
        PoolKey poolKey = new PoolKey("JDBC_URL", options);
        assertTrue(poolKey.equals(poolKey));
    }

    @Test
    void poolKeyConstructorTest1() {
        BMap<BString, Boolean> options = new MapValueImpl<>();
        options.put(fromString("booleanValue"), true);
        PoolKey poolKey = new PoolKey("JDBC_URL", options);
        assertFalse(poolKey.equals(1));
    }

    @Test
    void hashCodeTest() {
        BMap<BString, Boolean> options = new MapValueImpl<>();
        options.put(fromString("booleanValue"), true);
        PoolKey poolKey = new PoolKey("JDBC_URL", options);
        assertEquals(poolKey.hashCode(), 1346265441);
    }

    @Test
    void hashCodeTest1() {
        BMap<BString, Double> options = new MapValueImpl<>();
        options.put(fromString("floatValue"), 1.2);
        PoolKey poolKey = new PoolKey("JDBC_URL", options);
        assertEquals(poolKey.hashCode(), 1581738924);
    }

    @Test
    void hashCodeTest2() {
        BMap<BString, BValue> options = new MapValueImpl<>();
        options.put(fromString("charValue"), TestUtils.getMockBValueJson());
        PoolKey poolKey = new PoolKey("JDBC_URL", options);
        try {
            poolKey.hashCode();
        } catch (AssertionError e) {
            assertEquals(e.getMessage(), "type json shouldn't have occurred");
        }
    }

    @Test
    void hashCodeTest3() {
        BMap<BString, Character> options = new MapValueImpl<>();
        options.put(fromString("floatValue"), 'a');
        PoolKey poolKey = new PoolKey("JDBC_URL", options);
        assertEquals(poolKey.hashCode(), 1367829517);
    }

    @Test
    void optionsEqualTest() {
        BMap<BString, Double> options = new MapValueImpl<>();
        options.put(fromString("floatValue"), 1.2);
        PoolKey poolKey = new PoolKey("JDBC_URL", options);
        BMap<BString, Double> options1 = new MapValueImpl<>();
        options1.put(fromString("floatValue"), 1.3);
        PoolKey poolKey1 = new PoolKey("JDBC_URL", options1);
        assertFalse(poolKey.equals(poolKey1));

        PoolKey poolKey2 = new PoolKey("JDBC_URL", null);
        assertFalse(poolKey.equals(poolKey2));

        BMap<BString, Double> options2 = new MapValueImpl<>();
        PoolKey poolKey3 = new PoolKey("JDBC_URL", options2);
        assertFalse(poolKey.equals(poolKey3));

    }
}
