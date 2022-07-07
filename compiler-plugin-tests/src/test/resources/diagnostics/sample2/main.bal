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

type User record {
    int id;
};

public function main() returns error? {

    sql:ParameterizedQuery query = ``;
    sql:Value value = 5;

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

    sql:TimestampWithTimezoneOutParameter timestampWithTimezoneOut = new();
    int _ = check timestampWithTimezoneOut.get(int);
    time:Civil _ = check timestampWithTimezoneOut.get(time:Civil);
    time:Utc _ = check timestampWithTimezoneOut.get(time:Utc);
    string _ = check timestampWithTimezoneOut.get(string);
    json _ = check timestampWithTimezoneOut.get(json);

    sql:VarcharOutParameter varcharOut = new();
    json _ = check varcharOut.get(json);
    int _ = check varcharOut.get(int);

    sql:NCharOutParameter ncharOut = new();
    json _ = check ncharOut.get(json);
    int _ = check ncharOut.get(int);

    sql:NVarcharOutParameter nvarcharOut = new();
    json _ = check nvarcharOut.get(json);
    int _ = check nvarcharOut.get(int);

    sql:BitOutParameter bitOut = new();
    json _ = check bitOut.get(json);
    int _ = check bitOut.get(int);
    boolean _ = check bitOut.get(boolean);
    string _ = check bitOut.get(string);

    sql:BooleanOutParameter booleanOut = new();
    json _ = check booleanOut.get(json);
    int _ = check booleanOut.get(int);
    boolean _ = check booleanOut.get(boolean);
    string _ = check booleanOut.get(string);

    sql:IntegerOutParameter integerOut = new();
    float _ = check integerOut.get(float);
    string _ = check integerOut.get(string);
    int _ = check integerOut.get(int);

    sql:BigIntOutParameter bigIntOut = new();
    float _ = check bigIntOut.get(float);
    string _ = check bigIntOut.get(string);
    int _ = check bigIntOut.get(int);

    sql:SmallIntOutParameter smallIntOut = new();
    float _ = check smallIntOut.get(float);
    string _ = check smallIntOut.get(string);
    int _ = check smallIntOut.get(int);

    sql:FloatOutParameter floatOut = new();
    int _ = check floatOut.get(int);
    float _ = check floatOut.get(float);
    string _ = check floatOut.get(string);

    sql:RealOutParameter realOut = new();
    int _ = check realOut.get(int);
    float _ = check realOut.get(float);
    string _ = check realOut.get(string);

    sql:DoubleOutParameter doubleOut = new();
    int _ = check doubleOut.get(int);
    float _ = check doubleOut.get(float);
    string _ = check doubleOut.get(string);

    sql:NumericOutParameter numericOut = new();
    int _ = check numericOut.get(int);
    decimal _ = check numericOut.get(decimal);
    string _ = check numericOut.get(string);

    sql:DecimalOutParameter decimalOut = new();
    int _ = check decimalOut.get(int);
    decimal _ = check decimalOut.get(decimal);
    string _ = check decimalOut.get(string);

    sql:BinaryOutParameter binaryOut = new();
    int _ = check binaryOut.get(int);
    string _ = check binaryOut.get(string);

    sql:VarBinaryOutParameter varBinaryOut = new();
    int _ = check varBinaryOut.get(int);
    string _ = check varBinaryOut.get(string);

    sql:BlobOutParameter blobOut = new();
    int _ = check blobOut.get(int);
    string _ = check blobOut.get(string);

    sql:ClobOutParameter clobOut = new();
    int _ = check clobOut.get(int);
    json _ = check clobOut.get(json);
    string _ = check clobOut.get(string);

    sql:NClobOutParameter nclobOut = new();
    int _ = check nclobOut.get(int);
    json _ = check nclobOut.get(json);
    string _ = check nclobOut.get(string);

    sql:TimeWithTimezoneOutParameter timeWithTimezoneOut = new();
    time:TimeOfDay _ = check timeWithTimezoneOut.get(time:TimeOfDay);

    sql:TimestampOutParameter timestampOut = new();
    time:Civil _ = check timestampOut.get(time:Civil);
    time:Utc _ = check timestampOut.get(time:Utc);

    sql:DateTimeOutParameter dateTimeOut = new();
    time:Date _ = check dateTimeOut.get(time:Date);

    sql:VarcharArrayOutParameter varcharArrayOut = new();
    int _ = check varcharArrayOut.get(int);

    sql:RefOutParameter refOut = new();
    User _ = check refOut.get(User);
    int _ = check refOut.get(int);

    sql:StructOutParameter structOut = new();
    User _ = check structOut.get(User);
    int _ = check structOut.get(int);

    sql:RowOutParameter rowOut = new();
    int _ = check rowOut.get(int);

    sql:XMLOutParameter xmlOut = new();
    int _ = check xmlOut.get(int);

    http:Client httpclient = check new ("adasdsasd");
    _ = check httpclient->get("sdfsdf");
    _ = httpclient.getCookieStore();
}
