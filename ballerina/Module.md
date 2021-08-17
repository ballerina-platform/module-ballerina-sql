## Overview

This module provides the common interface and functionality to interact with a database. The corresponding database
clients can be created by using specific database modules such as `mysql` or using the Java Database Connectivity 
module `jdbc`.

### List of Database Modules
Ballerina now has the [`jdbc` module](https://docs.central.ballerina.io/ballerinax/java.jdbc/latest) as the generic DB connector module to connect to any relational database by simply providing the JDBC URL and the other related properties.
Ballerina also provides specially designed various database-specific DB connectors so that you can work with different databases and you can access their DB-specific functionalities.

### Client

The database client should be created using any of the above-listed database modules and once it is created, the operations and functionality explained below can be used. 

#### Connection Pool Handling

All database modules share the same connection pooling concept and there are three possible scenarios for 
connection pool handling.  For its properties and possible values, see the [`sql:ConnectionPool`](https://docs.central.ballerina.io/ballerina/sql/latest/records/ConnectionPool).  

1. Global, shareable, default connection pool

    If you do not provide the `poolOptions` field when creating the database client, a globally-shareable pool will be 
    created for your database unless a connection pool matching with the properties you provided already exists. 
    The JDBC module sample below shows how the global connection pool is used. 

    ```ballerina
    jdbc:Client|sql:Error dbClient = 
                               new ("jdbc:mysql://localhost:3306/testdb", 
                                "root", "root");
    ```

2. Client-owned, unsharable connection pool

    If you define the `connectionPool` field inline when creating the database client with the `sql:ConnectionPool` type, 
    an unsharable connection pool will be created. The JDBC module sample below shows how the global 
    connection pool is used.

    ```ballerina
    jdbc:Client|sql:Error dbClient = 
                               new (url = "jdbc:mysql://localhost:3306/testdb", 
                               connectionPool = { maxOpenConnections: 5 });
    ```

3. Local, shareable connection pool

    If you create a record of the `sql:ConnectionPool` type and reuse that in the configuration of multiple clients, 
    for each set of clients that connects to the same database instance with the same set of properties, a shared 
    connection pool will be created. The JDBC module sample below shows how the global connection pool is used.

    ```ballerina
    sql:ConnectionPool connPool = {maxOpenConnections: 5};
    
    jdbc:Client|sql:Error dbClient1 =       
                               new (url = "jdbc:mysql://localhost:3306/testdb",
                               connectionPool = connPool);
    jdbc:Client|sql:Error dbClient2 = 
                               new (url = "jdbc:mysql://localhost:3306/testdb",
                               connectionPool = connPool);
    jdbc:Client|sql:Error dbClient3 = 
                               new (url = "jdbc:mysql://localhost:3306/testdb",
                               connectionPool = connPool);
    ```
    
#### Closing the Client

Once all the database operations are performed, you can close the database client you have created by invoking the `close()`
operation. This will close the corresponding connection pool if it is not shared by any other database clients. 

```ballerina
error? e = dbClient.close();
```
Or
```ballerina
check dbClient.close();
```

### Database Operations

Once the client is created, database operations can be executed through that client. This module defines the interface 
and common properties that are shared among multiple database clients.  It also supports querying, inserting, deleting, 
updating, and batch updating data.  

#### Creating Tables

This sample creates a table with two columns. One column is of type `int` and the other is of type `varchar`.
The `CREATE` statement is executed via the `execute` remote function of the client.

```ballerina
// Create the ‘Students’ table with the  ‘id’, 'name', and ‘age’ fields.
sql:ExecutionResult result = check dbClient->execute("CREATE TABLE student(id INT AUTO_INCREMENT, " +
                         "age INT, name VARCHAR(255), PRIMARY KEY (id))");
//A value of the sql:ExecutionResult type is returned for 'result'. 
```

#### Inserting Data

These samples show the data insertion by executing an `INSERT` statement using the `execute` remote function 
of the client.

In this sample, the query parameter values are passed directly into the query statement of the `execute` 
remote function.

```ballerina
sql:ExecutionResult result = check dbClient->execute("INSERT INTO student(age, name) " +
                         "values (23, 'john')");
```

In this sample, the parameter values, which are in local variables are used to parameterize the SQL query in 
the `execute` remote function. This type of a parameterized SQL query can be used with any primitive Ballerina type 
like `string`, `int`, `float`, or `boolean` and in that case, the corresponding SQL type of the parameter is derived 
from the type of the Ballerina variable that is passed in. 

```ballerina
string name = "Anne";
int age = 8;

sql:ParameterizedQuery query = `INSERT INTO student(age, name)
                                values (${age}, ${name})`;
sql:ExecutionResult result = check dbClient->execute(query);
```

In this sample, the parameter values are passed as a `sql:TypedValue` to the `execute` remote function. Use the 
corresponding subtype of the `sql:TypedValue` such as `sql:Varchar`, `sql:Char`, `sql:Integer`, etc., when you need to 
provide more details such as the exact SQL type of the parameter.

```ballerina
sql:VarcharValue name = new ("James");
sql:IntegerValue age = new (10);

sql:ParameterizedQuery query = `INSERT INTO student(age, name)
                                values (${age}, ${name})`;
sql:ExecutionResult result = check dbClient->execute(query);
```

#### Inserting Data With Auto-generated Keys

This sample demonstrates inserting data while returning the auto-generated keys. It achieves this by using the 
`execute` remote function to execute the `INSERT` statement.

```ballerina
int age = 31;
string name = "Kate";

sql:ParameterizedQuery query = `INSERT INTO student(age, name)
                                values (${age}, ${name})`;
sql:ExecutionResult result = check dbClient->execute(query);
//Number of rows affected by the execution of the query.
int? count = result.affectedRowCount;
//The integer or string generated by the database in response to a query execution.
string|int? generatedKey = result.lastInsertId;
}
```

#### Querying Data

These samples show how to demonstrate the different usages of the `query` operation to query the
database table and obtain the results. 

This sample demonstrates querying data from a table in a database.
First, a type is created to represent the returned result set. This record can be defined as an open or a closed record
according to the requirement. If an open record is defined, the returned stream type will include both defined fields
in the record and additional database columns fetched by the SQL query which are not defined in the record.
Note the mapping of the database column to the returned record's property is case-insensitive if it is defined in the
record(i.e., the `ID` column in the result can be mapped to the `id` property in the record). Additional Column names
added to the returned record as in the SQL query. If the record is defined as a close record, only defined fields in the
record are returned or gives an error when additional columns present in the SQL query. Next, the `SELECT` query is executed
via the `query` remote function of the client. Once the query is executed, each data record can be retrieved by looping 
the result set. The `stream` returned by the select operation holds a pointer to the actual data in the database and it 
loads data from the table only when it is accessed. This stream can be iterated only once.

```ballerina
// Define an open record type to represent the results.
type Student record {
    int id;
    int age;
    string name;
};

// Select the data from the database table. The query parameters are passed 
// directly. Similar to the `execute` samples, parameters can be passed as
// sub types of `sql:TypedValue` as well.
int id = 10;
int age = 12;
sql:ParameterizedQuery query = `SELECT * FROM students
                                WHERE id < ${id} AND age > ${age}`;
stream<Student, sql:Error?> resultStream = dbClient->query(query);

// Iterating the returned table.
error? e = resultStream.forEach(function(Student student) {
   //Can perform any operations using 'student' and can access any fields in the returned record of type Student.
});
```

Defining the return type is optional and you can query the database without providing the result type. Hence, 
the above sample can be modified as follows with an open record type as the return type. The property name in the open record 
type will be the same as how the column is defined in the database. 

```ballerina
// Select the data from the database table. The query parameters are passed 
// directly. Similar to the `execute` samples, parameters can be passed as 
// sub types of `sql:TypedValue` as well.
int id = 10;
int age = 12;
sql:ParameterizedQuery query = `SELECT * FROM students
                                WHERE id < ${id} AND age > ${age}`;
stream<record{}, sql:Error?> resultStream = dbClient->query(query);

// Iterating the returned table.
error? e = resultStream.forEach(function(record{} student) {
    //Can perform any operations using the 'student' and can access any fields in the returned record.
    io:println("Student name: ", student.value["name"]);
});
```

There are situations in which you may not want to iterate through the database and in that case, you may decide
to only use the `next()` operation in the result `stream` and retrieve the first record. In such cases, the returned
result stream will not be closed, and you have to explicitly invoke the `close` operation on the 
`sql:Client` to release the connection resources and avoid a connection leak as shown below.

```ballerina
stream<record{}, sql:Error?> resultStream = 
            dbClient->query("SELECT count(*) as total FROM students");

record {|record {} value;|}? result = check resultStream.next();

if result is record {|record {} value;|} {
    // A valid result is returned.
    io:println("total students: ", result.value["total"]);
} else {
    // Student table must be empty.
}

error? e = resultStream.close();
```

#### Updating Data

This sample demonstrates modifying data by executing an `UPDATE` statement via the `execute` remote function of 
the client.

```ballerina
int age = 23;
sql:ParameterizedQuery query = `UPDATE students SET name = 'John' 
                                WHERE age = ${age}`;
sql:ExecutionResult result = check dbClient->execute(query);
```

#### Deleting Data

This sample demonstrates deleting data by executing a `DELETE` statement via the `execute` remote function of 
the client.

```ballerina
string name = "John";
sql:ParameterizedQuery query = `DELETE from students WHERE name = ${name}`;
sql:ExecutionResult result = check dbClient->execute(query);
```

#### Batch Updating Data

This sample demonstrates how to insert multiple records with a single `INSERT` statement that is executed via the 
`batchExecute` remote function of the client. This is done by creating a `table` with multiple records and 
parameterized SQL query as same as the above `execute` operations.

```ballerina
// Create the table with the records that need to be inserted.
var data = [
  { name: "John", age: 25  },
  { name: "Peter", age: 24 },
  { name: "jane", age: 22 }
];

// Do the batch update by passing the batches.
sql:ParameterizedQuery[] batch = from var row in data
                                 select `INSERT INTO students ('name', 'age')
                                 VALUES (${row.name}, ${row.age})`;
sql:ExecutionResult[] result = check dbClient->batchExecute(batch);
```

#### Execute SQL Stored Procedures

This sample demonstrates how to execute a stored procedure with a single `INSERT` statement that is executed via the 
`call` remote function of the client.

```ballerina
int uid = 10;
sql:IntegerOutParameter insertId = new;

sql:ProcedureCallResult|sql:Error result = dbClient->call(`call InsertPerson(${uid}, ${insertId})`);
if result is error {
    //An error returned.
} else {
    stream<record{}, sql:Error?>? resultStr = result.queryResult;
    if resultStr is stream<record{}, sql:Error?> {
        sql:Error? e = resultStr.forEach(function(record{} result) {
        //can perform operations using 'result'.
      });
    }
    check result.close();
}
```
Note that you have to invoke the close operation explicitly on the `sql:ProcedureCallResult` to release the connection resources and avoid a connection leak as shown above.

>**Note:** The default thread pool size used in Ballerina is: `the number of processors available * 2`. You can configure the thread pool size by using the `BALLERINA_MAX_POOL_SIZE` environment variable.
