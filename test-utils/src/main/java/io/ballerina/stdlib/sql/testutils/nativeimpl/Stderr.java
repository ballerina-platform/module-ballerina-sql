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
import io.ballerina.stdlib.io.channels.AbstractNativeChannel;
import io.ballerina.stdlib.io.channels.BlobChannel;
import io.ballerina.stdlib.io.channels.BlobIOChannel;
import io.ballerina.stdlib.sql.testutils.utils.OSUtils;

import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * Extern function exec.
 *
 * @since 1.0.0
 */
public class Stderr extends AbstractNativeChannel {

    public static BObject stderr(BObject objVal) {
        Process process = OSUtils.processFromObject(objVal);
        InputStream in = process.getErrorStream();
        ReadableByteChannel readableByteChannel = Channels.newChannel(in);
        return createChannel(new BlobIOChannel(new BlobChannel(readableByteChannel)));
    }
}
