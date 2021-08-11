// Copyright (c) 2020 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import ballerina/lang.'transaction as transactions;
import ballerina/io;
import ballerina/test;

string localTransactionDB = urlPrefix + "9004/transaction";

type TransactionResultCount record {
    int COUNTVAL;
};

public class SQLDefaultRetryManager {
    private int count;
    public isolated function init(int count = 2) {
        self.count = count;
    }
    public isolated function shouldRetry(error? e) returns boolean {
        if e is error && self.count >  0 {
            self.count -= 1;
            return true;
        } else {
            return false;
        }
    }
}

@test:BeforeGroups {
	value: ["transaction"]
}
function initTransactionContainer() returns error? {
	check initializeDockerContainer("sql-transaction", "transaction", "9004", "transaction", "local-transaction-test-data.sql");
}

@test:AfterGroups {
	value: ["transaction"]
}
function cleanTransactionContainer() returns error? {
    check cleanDockerContainer("sql-transaction");
}

@test:Config {
    groups: ["transaction"]
}
function testLocalTransaction() returns error? {
    MockClient dbClient = check new (url = localTransactionDB, user = user, password = password);
    int retryVal = -1;
    boolean committedBlockExecuted = false;
    transactions:Info transInfo;
    retry<SQLDefaultRetryManager>(1) transaction {
        var res = check dbClient->execute("Insert into Customers (firstName,lastName,registrationID,creditLimit,country) " +
                                "values ('James', 'Clerk', 200, 5000.75, 'USA')");
        res = check dbClient->execute("Insert into Customers (firstName,lastName,registrationID,creditLimit,country) " +
                                "values ('James', 'Clerk', 200, 5000.75, 'USA')");
        transInfo = transactions:info();
        var commitResult = commit;
        if(commitResult is ()){
            committedBlockExecuted = true;
        }
    }
    retryVal = transInfo.retryNumber;
    //check whether update action is performed
    int count = check getCount(dbClient, "200");
    check dbClient.close();

    test:assertEquals(retryVal, 0);
    test:assertEquals(count, 2);
    test:assertEquals(committedBlockExecuted, true);
}

boolean stmtAfterFailureExecutedRWC = false;
int retryValRWC = -1;
@test:Config {
    groups: ["transaction"],
    dependsOn: [testLocalTransaction]
}
function testTransactionRollbackWithCheck() returns error? {
    MockClient dbClient = check new (url = localTransactionDB, user = user, password = password);
    error? res = testTransactionRollbackWithCheckHelper(dbClient);
    int count = check getCount(dbClient, "210");
    check dbClient.close();

    test:assertEquals(retryValRWC, 1);
    test:assertEquals(count, 0);
    test:assertEquals(stmtAfterFailureExecutedRWC, false);
}

function testTransactionRollbackWithCheckHelper(MockClient dbClient) returns error? {
    transactions:Info transInfo;
    retry<SQLDefaultRetryManager>(1) transaction {
        transInfo = transactions:info();
        retryValRWC = transInfo.retryNumber;
        var e1 = check dbClient->execute("Insert into Customers (firstName,lastName,registrationID," +
                "creditLimit,country) values ('James', 'Clerk', 210, 5000.75, 'USA')");
        var e2 = check dbClient->execute("Insert into Customers2 (firstName,lastName,registrationID," +
                    "creditLimit,country) values ('James', 'Clerk', 210, 5000.75, 'USA')");
        stmtAfterFailureExecutedRWC  = true;
        check commit;
    }
}

@test:Config {
    groups: ["transaction"],
    dependsOn: [testTransactionRollbackWithCheck]
}
function testTransactionRollbackWithRollback() returns error? {
   MockClient dbClient = check new (url = localTransactionDB, user = user, password = password);
    int retryVal = -1;
    boolean stmtAfterFailureExecuted = false;
    transactions:Info transInfo;
    retry<SQLDefaultRetryManager>(1) transaction {
        transInfo = transactions:info();
        var e1 = dbClient->execute("Insert into Customers (firstName,lastName,registrationID," +
                "creditLimit,country) values ('James', 'Clerk', 211, 5000.75, 'USA')");
        if e1 is error {
            rollback;
        } else {
            var e2 = dbClient->execute("Insert into Customers2 (firstName,lastName,registrationID," +
                        "creditLimit,country) values ('James', 'Clerk', 211, 5000.75, 'USA')");
            if e2 is error {
                rollback;
                stmtAfterFailureExecuted  = true;
            } else {
                check commit;
            }
        }
    }
    retryVal = transInfo.retryNumber;
    int count = check getCount(dbClient, "211");
    check dbClient.close();

    test:assertEquals(retryVal, 0);
    test:assertEquals(count, 0);
    test:assertEquals(stmtAfterFailureExecuted, true);

}

@test:Config {
    groups: ["transaction"],
    dependsOn: [testTransactionRollbackWithRollback]
}
function testLocalTransactionUpdateWithGeneratedKeys() returns error? {
   MockClient dbClient = check new (url = localTransactionDB, user = user, password = password);
    int returnVal = 0;
    transactions:Info transInfo;
    retry<SQLDefaultRetryManager>(1) transaction {
        transInfo = transactions:info();
        var e1 = check dbClient->execute("Insert into Customers " +
         "(firstName,lastName,registrationID,creditLimit,country) values ('James', 'Clerk', 615, 5000.75, 'USA')");
        var e2 =  check dbClient->execute("Insert into Customers " +
        "(firstName,lastName,registrationID,creditLimit,country) values ('James', 'Clerk', 615, 5000.75, 'USA')");
        check commit;
    }
    returnVal = transInfo.retryNumber;
    //Check whether the update action is performed.
    int count = check getCount(dbClient, "615");
    check dbClient.close();

    test:assertEquals(returnVal, 0);
    test:assertEquals(count, 2);
}

int returnValRGK = 0;
@test:Config {
    groups: ["transaction"],
    dependsOn: [testLocalTransactionUpdateWithGeneratedKeys]
}
function testLocalTransactionRollbackWithGeneratedKeys() returns error? {
    MockClient dbClient = check new (url = localTransactionDB, user = user, password = password);
    error? res = testLocalTransactionRollbackWithGeneratedKeysHelper(dbClient);
    //check whether update action is performed
    int count = check getCount(dbClient, "615");
    check dbClient.close();
    test:assertEquals(returnValRGK, 1);
    test:assertEquals(count, 2);
}

function testLocalTransactionRollbackWithGeneratedKeysHelper(MockClient dbClient) returns error? {
    transactions:Info transInfo;
    retry<SQLDefaultRetryManager>(1) transaction {
        transInfo = transactions:info();
        returnValRGK = transInfo.retryNumber;
        var e1 = check dbClient->execute("Insert into Customers " +
         "(firstName,lastName,registrationID,creditLimit,country) values ('James', 'Clerk', 615, 5000.75, 'USA')");
        var e2 = check dbClient->execute("Insert into Customers2 " +
        "(firstName,lastName,registrationID,creditLimit,country) values ('James', 'Clerk', 615, 5000.75, 'USA')");
        check commit;
    }
}

isolated int abortVal = 0;

@test:Config {
    groups: ["transaction"],
    dependsOn: [testLocalTransactionRollbackWithGeneratedKeys]
}
function testTransactionAbort() returns error?  {
   MockClient dbClient = check new (url = localTransactionDB, user = user, password = password);
    transactions:Info transInfo;

    var abortFunc = isolated function(transactions:Info? info, error? cause, boolean willTry) {
        lock {
            abortVal = -1;
        }
    };

    retry<SQLDefaultRetryManager>(1) transaction {
        transInfo = transactions:info();
        transactions:onRollback(abortFunc);
        var res = check dbClient->execute("Insert into Customers " +
         "(firstName,lastName,registrationID,creditLimit,country) values ('James', 'Clerk', 220, 5000.75, 'USA')");
        res = check dbClient->execute("Insert into Customers " +
        "(firstName,lastName,registrationID,creditLimit,country) values ('James', 'Clerk', 220, 5000.75, 'USA')");
        int i = 0;
        if i == 0 {
            rollback;
        } else {
            check commit;
        }
    }
    int returnVal = transInfo.retryNumber;
    //Check whether the update action is performed.
    int count = check getCount(dbClient, "220");
    check dbClient.close();

    test:assertEquals(returnVal, 0);
    lock {
        test:assertEquals(abortVal, -1);
    }
    test:assertEquals(count, 0);
}

int testTransactionErrorPanicRetVal = 0;
@test:Config {
    enable: false,
    groups: ["transaction"]
}
function testTransactionErrorPanic() returns error? {
    MockClient dbClient = check new (url = localTransactionDB, user = user, password = password);
    int returnVal = 0;
    int catchValue = 0;
    var ret = trap testTransactionErrorPanicHelper(dbClient);
    io:println(ret);
    if ret is error {
        catchValue = -1;
    }
    //Check whether the update action is performed.
    int count = check getCount(dbClient, "260");
    check dbClient.close();
    test:assertEquals(testTransactionErrorPanicRetVal, 1);
    test:assertEquals(catchValue, -1);
    test:assertEquals(count, 0);
}

function testTransactionErrorPanicHelper(MockClient dbClient) returns error? {
    int returnVal = 0;
    transactions:Info transInfo;
    retry<SQLDefaultRetryManager>(1) transaction {
        transInfo = transactions:info();
        var e1 = check dbClient->execute("Insert into Customers (firstName,lastName," +
                              "registrationID,creditLimit,country) values ('James', 'Clerk', 260, 5000.75, 'USA')");
        int i = 0;
        if i == 0 {
            error e = error("error");
            panic e;
        } else {
            var r = check commit;
        }
    }
    io:println("exec");
    testTransactionErrorPanicRetVal = transInfo.retryNumber;
}

@test:Config {
    groups: ["transaction"],
    dependsOn: [testTransactionAbort]
}
function testTransactionErrorPanicAndTrap() returns error? {
   MockClient dbClient = check new (url = localTransactionDB, user = user, password = password);

    int catchValue = 0;
    transactions:Info transInfo;
    retry<SQLDefaultRetryManager>(1) transaction {
        transInfo = transactions:info();
        var e1 = check dbClient->execute("Insert into Customers (firstName,lastName,registrationID," +
                 "creditLimit,country) values ('James', 'Clerk', 250, 5000.75, 'USA')");
        var ret = trap testTransactionErrorPanicAndTrapHelper(0);
        if ret is error {
            catchValue = -1;
        }
        check commit;
    }
    int returnVal = transInfo.retryNumber;
    //Check whether the update action is performed.
    int count = check getCount(dbClient, "250");
    check dbClient.close();
    test:assertEquals(returnVal, 0);
    test:assertEquals(catchValue, -1);
    test:assertEquals(count, 1);
}

isolated function testTransactionErrorPanicAndTrapHelper(int i) {
    if i == 0 {
        error err = error("error");
        panic err;
    }
}

@test:Config {
    groups: ["transaction"],
    dependsOn: [testTransactionErrorPanicAndTrap]
}
function testTwoTransactions() returns error? {
    MockClient dbClient = check new (url = localTransactionDB, user = user, password = password);

     transactions:Info transInfo1;
     transactions:Info transInfo2;
     retry<SQLDefaultRetryManager>(1) transaction {
         transInfo1 = transactions:info();
         var e1 = check dbClient->execute("Insert into Customers (firstName,lastName,registrationID,creditLimit,country) " +
                             "values ('James', 'Clerk', 400, 5000.75, 'USA')");
         var e2 = check dbClient->execute("Insert into Customers (firstName,lastName,registrationID,creditLimit,country) " +
                             "values ('James', 'Clerk', 400, 5000.75, 'USA')");
         check commit;
     }
     int returnVal1 = transInfo1.retryNumber;

     retry<SQLDefaultRetryManager>(1) transaction {
         transInfo2 = transactions:info();
         var e1 = check dbClient->execute("Insert into Customers (firstName,lastName,registrationID,creditLimit,country) " +
                             "values ('James', 'Clerk', 400, 5000.75, 'USA')");
         var e2 = check dbClient->execute("Insert into Customers (firstName,lastName,registrationID,creditLimit,country) " +
                             "values ('James', 'Clerk', 400, 5000.75, 'USA')");
         check commit;
     }
     int returnVal2 = transInfo2.retryNumber;

     //Check whether the update action is performed.
     int count = check getCount(dbClient, "400");
     check dbClient.close();
    test:assertEquals(returnVal1, 0);
    test:assertEquals(returnVal2, 0);
    test:assertEquals(count, 4);
 }

@test:Config {
    groups: ["transaction"],
    dependsOn: [testTwoTransactions]
}
function testTransactionWithoutHandlers() returns error? {
   MockClient dbClient = check new (url = localTransactionDB, user = user, password = password);
    transaction {
        var res = check dbClient->execute("Insert into Customers (firstName,lastName,registrationID,creditLimit,country) " +
                            "values ('James', 'Clerk', 350, 5000.75, 'USA')");
        res = check dbClient->execute("Insert into Customers (firstName,lastName,registrationID,creditLimit,country) " +
                            "values ('James', 'Clerk', 350, 5000.75, 'USA')");
        check commit;
    }
    //Check whether the update action is performed.
    int count = check getCount(dbClient, "350");
    check dbClient.close();
    test:assertEquals(count, 2);
}

isolated string rollbackOut = "";

@test:Config {
    enable: false,
    groups: ["transaction"],
    dependsOn: [testTransactionWithoutHandlers]
}
function testLocalTransactionFailed() returns error? {
   MockClient dbClient = check new (url = localTransactionDB, user = user, password = password);

    string a = "beforetx";

    var ret = trap testLocalTransactionFailedHelper(dbClient);
    if ret is string {
        a += ret;
    } else {
        a += ret.message() + " trapped";
    }
    a = a + " afterTrx";
    int count = check getCount(dbClient, "111");
    check dbClient.close();
    test:assertEquals(a, "beforetx inTrx trxAborted inTrx trxAborted inTrx trapped afterTrx");
    test:assertEquals(count, 0);
}

isolated function testLocalTransactionFailedHelper(MockClient dbClient) returns string|error {
    transactions:Info transInfo;
    int i = 0;

    var onRollbackFunc = isolated function(transactions:Info? info, error? cause, boolean willTry) {
        lock {
           rollbackOut += " trxAborted";
        }
    };

    retry<SQLDefaultRetryManager>(2) transaction {
        lock {
           rollbackOut += " inTrx";
        }
        transInfo = transactions:info();
        transactions:onRollback(onRollbackFunc);
        var e1 = check dbClient->execute("Insert into Customers (firstName,lastName,registrationID,creditLimit,country) " +
                        "values ('James', 'Clerk', 111, 5000.75, 'USA')");
        var e2 = dbClient->execute("Insert into Customers2 (firstName,lastName,registrationID,creditLimit,country) " +
                        "values ('Anne', 'Clerk', 111, 5000.75, 'USA')");
        if(e2 is error){
           check getError();
        }
        check commit;
    }
    lock {
       return rollbackOut;
    }
}

isolated function getError() returns error? {
    lock {
       return error(rollbackOut);
    }
}

@test:Config {
    groups: ["transaction"],
    dependsOn: [testTransactionWithoutHandlers]
}
function testLocalTransactionSuccessWithFailed() returns error? {
   MockClient dbClient = check new (url = localTransactionDB, user = user, password = password);

    string a = "beforetx";
    string | error ret = trap testLocalTransactionSuccessWithFailedHelper(a, dbClient);
    if ret is string {
        a = ret;
    } else {
        a = a + "trapped";
    }
    a = a + " afterTrx";
    int count = check getCount(dbClient, "222");
    check dbClient.close();
    test:assertEquals(a, "beforetx inTrx inTrx inTrx committed afterTrx");
    test:assertEquals(count, 2);
}

isolated function testLocalTransactionSuccessWithFailedHelper(string status,MockClient dbClient) returns string|error {
    int i = 0;
    string a = status;
    retry<SQLDefaultRetryManager>(3) transaction {
        i = i + 1;
        a = a + " inTrx";
        var e1 = check dbClient->execute("Insert into Customers (firstName,lastName,registrationID,creditLimit,country)" +
                                    " values ('James', 'Clerk', 222, 5000.75, 'USA')");
        if i == 3 {
            var e2 = check dbClient->execute("Insert into Customers (firstName,lastName,registrationID,creditLimit,country) " +
                                        "values ('Anne', 'Clerk', 222, 5000.75, 'USA')");
        } else {
            var e3 = check dbClient->execute("Insert into Customers2 (firstName,lastName,registrationID,creditLimit,country) " +
                                        "values ('Anne', 'Clerk', 222, 5000.75, 'USA')");
        }
        check commit;
        a = a + " committed";
    }
    return a;
}

@test:Config {
    groups: ["transaction"],
    dependsOn: [testLocalTransactionSuccessWithFailed]
}
function testLocalTransactionWithBatchExecute() returns error? {
    MockClient dbClient = check new (url = localTransactionDB, user = user, password = password);
    int retryVal = -1;
    boolean committedBlockExecuted = false;
    transactions:Info transInfo;
    var data = [
            {firstName:"James", lastName:"Clerk", registrationID:301, creditLimit:5000.75 , country:"USA" },
            {firstName:"James", lastName:"Clerk", registrationID:301, creditLimit:5000.75 , country:"USA" }
    ];
    ParameterizedQuery[] sqlQueries =
        from var row in data
        select `INSERT INTO Customers (firstName,lastName,registrationID,creditLimit,country)
                                VALUES (${row.firstName}, ${row.lastName}, ${row.registrationID}, ${row.creditLimit},
                                ${row.country})`;
    retry<SQLDefaultRetryManager>(1) transaction {
        var res = check dbClient->batchExecute(sqlQueries);
        transInfo = transactions:info();
        var commitResult = commit;
        if(commitResult is ()){
            committedBlockExecuted = true;
        }
    }
    retryVal = transInfo.retryNumber;
    //check whether update action is performed
    int count = check getCount(dbClient, "301");
    check dbClient.close();

    test:assertEquals(retryVal, 0);
    test:assertEquals(count, 2);
    test:assertEquals(committedBlockExecuted, true);
}

@test:Config {
    groups: ["transaction"],
    dependsOn: [testLocalTransactionWithBatchExecute]
}
function testLocalTransactionWithQuery() returns error? {
    MockClient dbClient = check new (url = localTransactionDB, user = user, password = password);
        int retryVal = -1;
        boolean committedBlockExecuted = false;
        transactions:Info transInfo;
        var data = [
                {firstName:"James", lastName:"Clerk", registrationID:302, creditLimit:5000.75 , country:"USA" },
                {firstName:"James", lastName:"Clerk", registrationID:302, creditLimit:5000.75 , country:"USA" }
        ];
        ParameterizedQuery[] sqlQueries =
            from var row in data
            select `INSERT INTO Customers (firstName,lastName,registrationID,creditLimit,country)
                                    VALUES (${row.firstName}, ${row.lastName}, ${row.registrationID}, ${row.creditLimit},
                                    ${row.country})`;
        int count = 0;
        retry<SQLDefaultRetryManager>(1) transaction {
            var res = check dbClient->batchExecute(sqlQueries);
            transInfo = transactions:info();
            var commitResult = commit;
            if(commitResult is ()){
                committedBlockExecuted = true;
            }
            count = check getCount(dbClient, "302");
        }
        retryVal = transInfo.retryNumber;
        //check whether update action is performed
        check dbClient.close();

        test:assertEquals(retryVal, 0);
        test:assertEquals(count, 2);
        test:assertEquals(committedBlockExecuted, true);
}

@test:Config {
    groups: ["transaction"],
    dependsOn: [testLocalTransactionWithQuery]
}
function testLocalTransactionWithQueryRow() returns error? {
    MockClient dbClient = check new (url = localTransactionDB, user = user, password = password);
        int retryVal = -1;
        boolean committedBlockExecuted = false;
        transactions:Info transInfo;
        var data = [
                {firstName:"James", lastName:"Clerk", registrationID:303, creditLimit:5000.75 , country:"USA" },
                {firstName:"James", lastName:"Clerk", registrationID:303, creditLimit:5000.75 , country:"USA" }
        ];
        ParameterizedQuery[] sqlQueries =
            from var row in data
            select `INSERT INTO Customers (firstName,lastName,registrationID,creditLimit,country)
                                    VALUES (${row.firstName}, ${row.lastName}, ${row.registrationID}, ${row.creditLimit},
                                    ${row.country})`;
        int count = 0;
        retry<SQLDefaultRetryManager>(1) transaction {
            var res = check dbClient->batchExecute(sqlQueries);
            transInfo = transactions:info();
            var commitResult = commit;
            if(commitResult is ()){
                committedBlockExecuted = true;
            }
            count = check dbClient->queryRow(
                                      `Select COUNT(*) as countval from Customers where registrationID = 303`);
        }
        retryVal = transInfo.retryNumber;
        //check whether update action is performed
        check dbClient.close();

        test:assertEquals(retryVal, 0);
        test:assertEquals(count, 2);
        test:assertEquals(committedBlockExecuted, true);
}

isolated function getCount(MockClient dbClient, string id) returns int | error {
    stream<TransactionResultCount, Error?> streamData = dbClient->query(
        `Select COUNT(*) as countval from Customers where registrationID = ${id}`);
    record {|TransactionResultCount value;|}? data = check streamData.next();
    check streamData.close();
    TransactionResultCount? value = data?.value;
    if(value is TransactionResultCount){
       return value["COUNTVAL"];
    }
    return 0;
}
