/*
 * Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.stdlib.sql.testutils.nativeimpl;

import io.ballerina.runtime.api.values.BObject;
import io.ballerina.stdlib.sql.testutils.utils.OSConstants;
import io.ballerina.stdlib.sql.testutils.utils.OSUtils;

/**
 * External function for exitCode.
 *
 * @since 0.6.0
 */
public class ExitCode {

    public static Object exitCode(BObject objVal) {
        Process process = OSUtils.processFromObject(objVal);
        try {
            return process.exitValue();
        } catch (java.lang.IllegalThreadStateException e) {
            return OSUtils.getBallerinaError(OSConstants.PROCESS_EXEC_ERROR, e);
        }
    }
}
