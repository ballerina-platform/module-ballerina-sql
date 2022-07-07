// Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 Inc. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/sql;
import ballerina/time;
import ballerina/http;

public function main() returns error? {
    sql:CharOutParameter charOut = new();
    string _ = check charOut.get(string);
    int _ = check charOut.get(int);

    sql:TimeOutParameter timeOut = new();
    int _ = check timeOut.get(int);
    time:TimeOfDay _ = check timeOut.get(time:TimeOfDay);
    string _ = check timeOut.get(string);
    json _ = check timeOut.get(json);

    sql:DateOutParameter dateOut = new();
    int _ = check dateOut.get(int);
    time:Date _ = check dateOut.get(time:Date);
    string _ = check dateOut.get(string);
    json _ = check dateOut.get(json);

    sql:TimestampWithTimezoneOutParameter timeWithTimezoneOut = new();
    int _ = check timeWithTimezoneOut.get(int);
    time:Civil _ = check timeWithTimezoneOut.get(time:Civil);
    time:Utc _ = check timeWithTimezoneOut.get(time:Utc);
    string _ = check timeWithTimezoneOut.get(string);
    json _ = check timeWithTimezoneOut.get(json);

    http:Client httpclient = check new ("adasdsasd");
    _ = check httpclient->get("sdfsdf");
}
