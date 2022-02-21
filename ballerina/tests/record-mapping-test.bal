// Copyright (c) 2021 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import ballerina/test;

public type StudentsWithoutTeachersField record {
    int id;
    string name;
    int age;
    int supervisorId;
};

@test:Config {
    groups: ["query", "query-row"]
}
function queryWithoutRecordField() returns error? {

    MockClient dbClient = check getMockClient(queryRowDb);
    StudentsWithoutTeachersField student = check dbClient->queryRow(`SELECT * FROM students JOIN teachers ON students.supervisorId = teachers.id`);
    check dbClient.close();

    StudentsWithoutTeachersField expectedStudent = {
        id: 1,
        name: "Alice",
        age: 25,
        supervisorId: 1,
        "TEACHERS.ID": 1,
        "TEACHERS.NAME": "James"
    };

    test:assertEquals(student, expectedStudent, "Expected student record did not match");
}

public type StudentsWithoutTeachersFieldClosed record {|
    int id;
    string name;
    int age;
    int supervisorId;
|};

@test:Config {
    groups: ["query", "query-row"]
}
function queryWithoutRecordFieldSealed() returns error? {

    MockClient dbClient = check getMockClient(queryRowDb);
    StudentsWithoutTeachersFieldClosed|error failure = 
                dbClient->queryRow(`SELECT * FROM students JOIN teachers ON students.supervisorId = teachers.id`);
    check dbClient.close();

    if failure is error {
        test:assertEquals(failure.message(), 
            "No mapping field found for SQL table column 'TEACHERS.ID' in the record type 'StudentsWithoutTeachersFieldClosed'", 
            "Expected error message record did not match");
    } else {
        test:assertFail("Error expected");
    }
}

public type Student record {|
    int id;
    string name;
    int age;
    int supervisorId;
    record {} teachers;
|};

@test:Config {
    groups: ["query", "query-row"]
}
function queryAnnonRecord() returns error? {

    MockClient dbClient = check getMockClient(queryRowDb);
    Student student = check 
                dbClient->queryRow(`SELECT * FROM students JOIN teachers ON students.supervisorId = teachers.id`);
    check dbClient.close();

    Student expectedStudent = {
        id: 1,
        name: "Alice",
        age: 25,
        supervisorId: 1,
        teachers: {
            "ID": 1,
            "NAME": "James"
        }
    };

    test:assertEquals(student, expectedStudent, "Expected student record did not match");
}

public type Student1 record {|
    int id;
    string name;
    int age;
    int supervisorId;
    Teachers1 teachers;
|};

public type Teachers1 record {
    int ID;
    string NAME;
};

@test:Config {
    groups: ["query", "query-row"]
}
function queryTypedRecordWithFields() returns error? {

    MockClient dbClient = check getMockClient(queryRowDb);
    Student1 student = check 
                dbClient->queryRow(`SELECT * FROM students JOIN teachers ON students.supervisorId = teachers.id`);
    check dbClient.close();

    Student1 expectedStudent = {
        id: 1,
        name: "Alice",
        age: 25,
        supervisorId: 1,
        teachers: {
            "ID": 1,
            "NAME": "James"
        }
    };

    test:assertEquals(student, expectedStudent, "Expected student record did not match");
}

@test:Config {
    groups: ["query", "query-row"]
}
function queryTypedRecordWithFieldsStream() returns error? {

    MockClient dbClient = check getMockClient(queryRowDb);
    stream<Student1, Error?> studentStream = 
                dbClient->query(`SELECT * FROM students JOIN teachers ON students.supervisorId = teachers.id`);
    Student1? returnData = ();
    check from Student1 data in studentStream
        do {
            returnData = data;
        };

    check dbClient.close();

    Student1 expectedStudent = {
        id: 1,
        name: "Alice",
        age: 25,
        supervisorId: 1,
        teachers: {
            "ID": 1,
            "NAME": "James"
        }
    };

    test:assertEquals(returnData, expectedStudent, "Expected student record did not match");
}

public type Student2 record {|
    int id;
    string name;
    int age;
    int supervisorId;
    Teachers2 teachers;
|};

public type Teachers2 record {
    int ID;
};

@test:Config {
    groups: ["query", "query-row"]
}
function queryTypedRecordWithoutFields() returns error? {

    MockClient dbClient = check getMockClient(queryRowDb);
    Student2 student = check 
                dbClient->queryRow(`SELECT * FROM students JOIN teachers ON students.supervisorId = teachers.id`);
    check dbClient.close();

    Student2 expectedStudent = {
        id: 1,
        name: "Alice",
        age: 25,
        supervisorId: 1,
        teachers: {
            "ID": 1,
            "NAME": "James"
        }
    };

    test:assertEquals(student, expectedStudent, "Expected student record did not match");
}

public type Student3 record {|
    int id;
    string name;
    int age;
    int supervisorId;
    Teachers3 teachers;
|};

public type Teachers3 record {|
    int ID;
|};

@test:Config {
    groups: ["query", "query-row"]
}
function queryTypedRecordWithoutFieldsClosed() returns error? {

    MockClient dbClient = check getMockClient(queryRowDb);
    Student3|Error failure = 
                dbClient->queryRow(`SELECT * FROM students JOIN teachers ON students.supervisorId = teachers.id`);
    check dbClient.close();

    if failure is error {
        test:assertEquals(failure.message(), 
            "No mapping field found for SQL table column 'TEACHERS.NAME' in the record type 'Teachers3'", 
            "Expected error message record did not match");
    } else {
        test:assertFail("Error expected");
    }
}

annotation ColumnConfig TestColumn on record field;

public type Album record {|
    @Column{
        name: "id_test"
    }
    string id;
    string name;
    @Column{
        name: "artist_test"
    }
    string artist;
    @TestColumn{
        name: "price"
    }
    decimal price;
|};

@test:Config {
    groups: ["query", "query-row"]
}
function queryRowWithColumnAnnotation() returns error? {
    MockClient dbClient = check getMockClient(queryRowDb);
    Album album = check dbClient->queryRow(`SELECT * FROM Album`);
    check dbClient.close();

    Album expectedAlbum = {
        id: "1",
        name: "Lemonade",
        artist: "Beyonce",
        price: 20.0
    };

    test:assertEquals(album, expectedAlbum, "Expected Album record did not match");
}
