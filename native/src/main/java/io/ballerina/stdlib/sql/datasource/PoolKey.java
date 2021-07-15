/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package io.ballerina.stdlib.sql.datasource;

import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BValue;

import java.util.Map;
import java.util.Objects;

/**
 * The key that uniquely identifies a connection pool encapsulated by {@link SQLDatasource}.
 *
 * @since 1.2.0
 */
public class PoolKey {
    private final String jdbcUrl;
    private final BMap<BString, ?> options;

    public PoolKey(String jdbcUrl, BMap<BString, ?> options) {
        this.jdbcUrl = jdbcUrl;
        this.options = options;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof PoolKey)) {
            return false;
        }
        boolean jdbcUrlEqual = ((PoolKey) obj).jdbcUrl.equals(this.jdbcUrl);
        return jdbcUrlEqual && optionsEqual((PoolKey) obj);
    }

    @Override
    public int hashCode() {
        int hashCode = 17;
        hashCode = hashCode * 31 + jdbcUrl.hashCode();
        if (options != null) {
            hashCode = 31 * hashCode + calculateDbOptionsHashCode();
        }
        return hashCode;
    }

    private int calculateDbOptionsHashCode() {
        int hashCode = 17;
        for (Map.Entry<BString, ?> entry : options.entrySet()) {
            int keyHashCode = entry.getKey().hashCode();
            Object value = entry.getValue();
            int valueHashCode;
            Objects.requireNonNull(value, "type null shouldn't have occurred");
            if (value instanceof BValue || value instanceof Number
                     || value instanceof String || value instanceof Boolean) {
                Type type = TypeUtils.getType(value);
                int typeTag = type.getTag();
                switch (typeTag) {
                    case TypeTags.STRING_TAG:
                    case TypeTags.DECIMAL_TAG:
                        valueHashCode = value.hashCode();
                        break;
                    case TypeTags.BYTE_TAG:
                    case TypeTags.INT_TAG:
                        // Assert to convince spotbugs that value is a number
                        assert value instanceof Number;
                        long longValue = (Long) value;
                        valueHashCode = (int) (longValue ^ (longValue >>> 32));
                        break;
                    case TypeTags.FLOAT_TAG:
                        assert value instanceof Double;
                        long longValueConvertedFromDouble = Double.doubleToLongBits((Double) value);
                        valueHashCode = (int) (longValueConvertedFromDouble ^ (longValueConvertedFromDouble >>> 32));
                        break;
                    case TypeTags.BOOLEAN_TAG:
                        assert value instanceof Boolean;
                        valueHashCode = ((Boolean) value ? 1 : 0);
                        break;
                    default:
                        throw new AssertionError("type " + type.getName() + " shouldn't have occurred");
                }
            } else {
                valueHashCode = value.hashCode();
            }

            hashCode = hashCode + keyHashCode + valueHashCode;
        }
        return hashCode;
    }

    private boolean optionsEqual(PoolKey anotherPoolKey) {
        BMap<BString, ?> anotherDbOptions = anotherPoolKey.options;
        if (options == null && anotherDbOptions == null) {
            return true;
        }
        if (options == null || anotherDbOptions == null) {
            return false;
        }
        if (this.options.size() != anotherDbOptions.size()) {
            return false;
        }
        for (Map.Entry<BString, ?> entry : options.entrySet()) {
            if (!entry.getValue().equals(anotherDbOptions.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }
}
