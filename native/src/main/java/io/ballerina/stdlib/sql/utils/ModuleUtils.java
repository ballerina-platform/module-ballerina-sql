/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.stdlib.sql.utils;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.Module;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BString;

/**
 * Utility functions relevant to module operations.
 *
 * @since 2.0.0
 */
public class ModuleUtils {

    private static Module sqlModule;
    private static BString pkgIdentifier;

    private ModuleUtils() {
    }

    public static void setModule(Environment env) {
        sqlModule = env.getCurrentModule();
        pkgIdentifier = StringUtils.fromString(sqlModule.toString());
    }

    public static Module getModule() {
        return sqlModule;
    }

    public static BString getPkgIdentifier() {
        return pkgIdentifier;
    }
}
