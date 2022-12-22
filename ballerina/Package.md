## Package overview

This package provides the generic interface and functionality to interact with an SQL database. The corresponding database
clients can be created by using specific database packages such as `mysql` or using the Java Database Connectivity
package `jdbc`.

### List of database packages
Ballerina now has the [`jdbc` package](https://docs.central.ballerina.io/ballerinax/java.jdbc/latest) as the generic DB connector package to connect to any relational database by simply providing the JDBC URL and the other related properties.

Ballerina also provides specially designed various database-specific DB connectors so that you can work with different databases, and you can access their DB-specific functionalities.
* [`MySQL` package](https://central.ballerina.io/ballerinax/mysql)
* [`PostgreSQL` package](https://central.ballerina.io/ballerinax/postgresql)
* [`MSSQL` package](https://central.ballerina.io/ballerinax/mssql)
* [`OracleDB` package](https://central.ballerina.io/ballerinax/oracledb)

### Client

The database client should be created using any of the above-listed database libraries and kept open throughout
the entirety of the application to perform the operations and functionality which are explained below.

#### Handle connection pools

All database packages share the same connection pooling concept and there are three possible scenarios for
connection pool handling.  For its properties and possible values, see the [`sql:ConnectionPool`](https://docs.central.ballerina.io/ballerina/sql/latest/records/ConnectionPool).

1. Global, shareable, default connection pool

   If you do not provide the `poolOptions` field when creating the database client, a globally-shareable pool will be
   created for your database unless a connection pool matching with the properties you provided already exists.
   The `jdbc` package sample below shows how the global connection pool is used.

    ```ballerina
    jdbc:Client|sql:Error dbClient = 
                               new ("jdbc:mysql://localhost:3306/testdb", 
                                "root", "root");
    ```

2. Client-owned, unsharable connection pool

   If you define the `connectionPool` field inline when creating the database client with the `sql:ConnectionPool` type,
   an unsharable connection pool will be created. The `jdbc` package sample below shows how the global
   connection pool is used.

    ```ballerina
    jdbc:Client|sql:Error dbClient = 
                            new (url = "jdbc:mysql://localhost:3306/testdb",
                            connectionPool = { maxOpenConnections: 5 });
    ```

3. Local, shareable connection pool

   If you create a record of the `sql:ConnectionPool` type and reuse that in the configuration of multiple clients,
   a shared connection pool will be created for each set of clients that connects to the same database instance with the same set of properties. The `jdbc` package sample below shows how the global connection pool is used.

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

#### Close the client

Once all the database operations are performed, you can close the database client you have created by invoking the `close()`
operation. This will close the corresponding connection pool if it is not shared by any other database clients.

```ballerina
error? e = dbClient.close();
```
Or
```ballerina
check dbClient.close();
```

### Database operations

Once the client is created, database operations can be executed through that client. This package defines the interface
and generic properties that are shared among multiple database clients. It also supports querying, inserting, deleting,
updating, and batch updating data.

#### Parameterized query

The `sql:ParameterizedQuery` is used to construct the SQL query to be executed by the client.
You can create a query with constant or dynamic input data as follows.

*Query with constant values*

```ballerina
sql:ParameterizedQuery query = `SELECT * FROM students 
                                WHERE id < 10 AND age > 12`;
```

*Query with dynamic values*

```ballerina
int[] ids = [10, 50];
int age = 12;
sql:ParameterizedQuery query = `SELECT * FROM students 
                                WHERE id < ${ids[0]} AND age > ${age}`;
```

Moreover, the SQL package has `sql:queryConcat()` and `sql:arrayFlattenQuery()` util functions which make it easier
to create a dynamic/constant complex query.

The `sql:queryConcat()` is used to create a parameterized query by concatenating a set of parameterized queries.
The sample below shows how to concatenate queries.

```ballerina
int id = 10;
int age = 12;
sql:ParameterizedQuery query = `SELECT * FROM students`;
sql:ParameterizedQuery query1 = ` WHERE id < ${id} AND age > ${age}`;
sql:ParameterizedQuery sqlQuery = sql:queryConcat(query, query1);
```

The query with the `IN` operator can be created using the `sql:ParameterizedQuery` like below. Here you need to flatten the array and pass each element separated by a comma.

```ballerina
int[] ids = [1, 2, 3];
sql:ParameterizedQuery query = `SELECT count(*) as total FROM DataTable 
                                WHERE row_id in (${ids[0]}, ${ids[1]}, ${ids[2]})`;
```

The util function `sql:arrayFlattenQuery()` is introduced to make the array flatten easier. It makes the inclusion of varying array elements into the query easier by flattening the array to return a parameterized query. You can construct the complex dynamic query with the `IN` operator by using both functions like below.

```ballerina
int[] ids = [1, 2];
sql:ParameterizedQuery sqlQuery = 
                         sql:queryConcat(`SELECT * FROM DataTable WHERE id IN (`, 
                                          sql:arrayFlattenQuery(ids), `)`);
```

#### Create tables

This sample creates a table with two columns. One column is of type `int` and the other is of type `varchar`.
The `CREATE` statement is executed via the `execute` remote function of the client.

```ballerina
// Create the ‘Students’ table with the ‘id’, 'name', and ‘age’ fields.
sql:ExecutionResult result = 
                check dbClient->execute(`CREATE TABLE student (
                                           id INT AUTO_INCREMENT,
                                           age INT, 
                                           name VARCHAR(255), 
                                           PRIMARY KEY (id)
                                         )`);
// A value of the sql:ExecutionResult type is returned for 'result'. 
```

#### Insert data

These samples show the data insertion by executing an `INSERT` statement using the `execute` remote function
of the client.

In this sample, the query parameter values are passed directly into the query statement of the `execute`
remote function.

```ballerina
sql:ExecutionResult result = check dbClient->execute(`INSERT INTO student(age, name)
                                                        VALUES (23, 'john')`);
```

In this sample, the parameter values, which are assigned to local variables are used to parameterize the SQL query in
the `execute` remote function. This parameterization can be performed with any primitive Ballerina type
like `string`, `int`, `float`, or `boolean` and in that case, the corresponding SQL type of the parameter is derived
from the type of the Ballerina variable that is passed in.

```ballerina
string name = "Anne";
int age = 8;

sql:ParameterizedQuery query = `INSERT INTO student(age, name)
                                  VALUES (${age}, ${name})`;
sql:ExecutionResult result = check dbClient->execute(query);
```

In this sample, the parameter values are passed as a `sql:TypedValue` to the `execute` remote function. Use the
corresponding subtype of the `sql:TypedValue` such as `sql:VarcharValue`, `sql:CharValue`, `sql:IntegerValue`, etc., when you need to
provide more details such as the exact SQL type of the parameter.

```ballerina
sql:VarcharValue name = new ("James");
sql:IntegerValue age = new (10);

sql:ParameterizedQuery query = `INSERT INTO student(age, name)
                                  VALUES (${age}, ${name})`;
sql:ExecutionResult result = check dbClient->execute(query);
```

#### Insert data with auto-generated keys

This sample demonstrates inserting data while returning the auto-generated keys. It achieves this by using the
`execute` remote function to execute the `INSERT` statement.

```ballerina
int age = 31;
string name = "Kate";

sql:ParameterizedQuery query = `INSERT INTO student(age, name)
                                  VALUES (${age}, ${name})`;
sql:ExecutionResult result = check dbClient->execute(query);'

// Number of rows affected by the execution of the query.
int? count = result.affectedRowCount;

// The integer or string generated by the database in response to a query execution.
string|int? generatedKey = result.lastInsertId;
```

#### Query data

These samples show how to demonstrate the different usages of the `query` operation to query the
database table and obtain the result as a stream. Once the operation is done, make sure to consume
all the fetched data or close the stream by invoking the `close()` operation.

This sample demonstrates querying data from a table in a database.
First, a type is created to represent the returned result set. This record can be defined as an open or a closed record
according to the requirement. If an open record is defined, the returned stream type will include both defined fields
in the record and additional database columns fetched by the SQL query which are not defined in the record.
Note the mapping of the database column to the returned record's property is case-insensitive if it is defined in the
record(i.e., the `ID` column in the result can be mapped to the `id` property in the record). Additional column names are
added to the returned record as in the SQL query. If the record is defined as a closed record, only defined fields in the
record are returned or gives an error when additional columns present in the SQL query. Next, the `SELECT` query is executed
via the `query` remote function of the client. Once the query is executed, each data record can be retrieved by iterating through
the result set. The `stream` returned by the `SELECT` operation holds a pointer to the actual data in the database and it
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
check from Student student in resultStream
    do {
       // Can perform operations using the record 'student' of type `Student`.
    };
```

Defining the return type is optional, and you can query the database without providing the result type. Hence,
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
check from record{} student in resultStream 
    do {
        // Can perform operations using the record 'student'.
        io:println("Student name: ", student.value["name"]);
    };
```

There are situations in which you may not want to iterate through the database and in that case, you may decide
to use the `sql:queryRow()` operation. If the provided return type is a record, this method returns only the first row
retrieved by the query as a record.

```ballerina
int id = 10;
sql:ParameterizedQuery query = `SELECT * FROM students WHERE id = ${id}`;
Student retrievedStudent = check dbClient->queryRow(query);
```

`sql:Column` annotation can be used to map database columns to Typed record fields of different name. This annotation should be attached to record fields.
```ballerina
type Student record {
    int id;
    @sql:Column { name: "first_name" }
    string firstName;
    @sql:Column { name: "last_name" }
    string lastName
};
```
The above annotation will map the database column `first_name` to the Ballerina record field `firstName`. If the `query()` function does not return `first_name` column, the field will not be populated.

Multiple table columns can be matched to a single Ballerina record within a returned record. For instance if the query returns data from multiple tables such as Students and Teachers.
All columns of the Teachers table can be grouped to another Typed record such as `Teacher` type within the `Student` record.

```ballerina
public type Students record {|
    int id;
    string name;
    string? age;
    float? gpa;
    Teachers teachers;
|}
type Teachers record {|
    int id;
    string name;
|}
```
In the above scenario also, `sql:Column` annotation can be used to rename field name such as,
```ballerina
public type Students record {|
    int id;
    string name;
    string? age;
    float? gpa;
    @sql:Column { name: "teachers" }
    Teacher teacher;
|}
```

The `sql:queryRow()` operation can also be used to retrieve a single value from the database (e.g., when querying using
`COUNT()` and other SQL aggregation functions). If the provided return type is not a record (i.e., a primitive data type)
, this operation will return the value of the first column of the first row retrieved by the query.

```ballerina
int age = 12;
sql:ParameterizedQuery query = `SELECT COUNT(*) FROM students WHERE age < ${age}`;
int youngStudents = check dbClient->queryRow(query);
```

#### Update data

This sample demonstrates modifying data by executing an `UPDATE` statement via the `execute` remote function of
the client.

```ballerina
int age = 23;
sql:ParameterizedQuery query = `UPDATE students SET name = 'John' WHERE age = ${age}`;
sql:ExecutionResult result = check dbClient->execute(query);
```

#### Delete data

This sample demonstrates deleting data by executing a `DELETE` statement via the `execute` remote function of
the client.

```ballerina
string name = "John";
sql:ParameterizedQuery query = `DELETE from students WHERE name = ${name}`;
sql:ExecutionResult result = check dbClient->execute(query);
```

#### Batch update data

This sample demonstrates how to insert multiple records with a single `INSERT` statement that is executed via the
`batchExecute` remote function of the client. This is done by creating a `table` with multiple records and a
parameterized SQL query as same as the above `execute` operations.

```ballerina
// Create the table with the records that need to be inserted.
var data = [
  { name: "John", age: 25 },
  { name: "Peter", age: 24 },
  { name: "jane", age: 22 }
];

// Do the batch update by passing the batches.
sql:ParameterizedQuery[] batch = from var row in data
                                 select `INSERT INTO students ('name', 'age')
                                           VALUES (${row.name}, ${row.age})`;
sql:ExecutionResult[] result = check dbClient->batchExecute(batch);
```

#### Execute SQL stored procedures

This sample demonstrates how to execute a stored procedure with a single `INSERT` statement that is executed via the
`call` remote function of the client.

```ballerina
int uid = 10;
sql:IntegerOutParameter insertId = new;

sql:ProcedureCallResult result = 
                         check dbClient->call(`call InsertPerson(${uid}, ${insertId})`);
stream<record{}, sql:Error?>? resultStr = result.queryResult;
if resultStr is stream<record{}, sql:Error?> {
    check from record{} value in resultStr
        do {
          // Can perform operations using the record 'result'.
        };
}
check result.close();
```

Note that you have to invoke the `close` operation explicitly on the `sql:ProcedureCallResult` to release the connection resources and avoid a connection leak as shown above.

>**Note:** The default thread pool size used in Ballerina is: `the number of processors available * 2`. You can configure the thread pool size by using the `BALLERINA_MAX_POOL_SIZE` environment variable.

## Report issues

To report bugs, request new features, start new discussions, view project boards, etc., go to the [Ballerina standard library parent repository](https://github.com/ballerina-platform/ballerina-standard-library)

## Useful links
- Chat live with us via our [Discord server](https://discord.gg/ballerinalang).
- Post all technical questions on Stack Overflow with the [#ballerina](https://stackoverflow.com/questions/tagged/ballerina) tag.
