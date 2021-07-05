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

package org.ballerinalang.sql.tests.datasource;

import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.internal.values.MapValueImpl;
import org.ballerinalang.sql.datasource.PoolKey;
import org.junit.jupiter.api.Test;

import static io.ballerina.runtime.api.utils.StringUtils.fromString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
}
