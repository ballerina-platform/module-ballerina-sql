# Support Annotations for DB column to Ballerina Record Mapping

_Owners_: @daneshk @niveathika  
_Reviewers_: @daneshk  
_Created_: 2022/02/11  
_Updated_: 2021/02/11  
_Edition_: Swan Lake  
_Issues_: [#2652](https://github.com/ballerina-platform/ballerina-standard-library/issues/2652)

## Summary

Enhance `query()` method with the use of Annotations in the return record types to map database columns.
```ballerina
type Person record {
    int id;
    string first_name;
    string last_name
};
Stream<Person, sql:Error?> persons = database->query(`SELECT is, first_name, last_name FROM ATS_PERSON`);
```
Annotations will be bound to `Person` record fields to map value from a column of a different name.

## History

The 1.2.x version of SQL packages included mapping support. However, the mapping was limited to only the order of returned columns to the record fields. The package will map The first column of the result will be mapped to the first field in the records.

This feature held a few setbacks. Since the user now has to keep the order even if the mapping is not needed.

## Goals
- Support annotations in the `sql` package
- Support field mapping of DB columns to record fields.

## Motivation

The ability to map table columns to Ballerina record fields in the remote function is efficient. In typical enterprise use cases, it is typical that the user would not have control over the database schema and its naming convention.

With this support, the user does not need to process the return results to another record or change the query itself to change the metadata.

## Description

Introduce `sql:Column` annotation of `string` type. The user can provide the database column name in the annotation.

```ballerina
type Person record {
    int id;
    @sql:Column { name: "first_name" }
    string firstName;
    @sql:Column { name: "last_name" }
    string lastName
};
```
The above annotation will map the database column `first_name` to the Ballerina record field `firstName`. If the `query()` function does not return `first_name` column, the field will not be populated.

The annotation will be defined as,
```
# Defines the database column name which matches the record field. The default value is the record field name.
#
# + name - The database column name 
public type ColumnConfig record {|
    string name;
|};

# The Annotation used to specify which database column matches the record field.
public annotation ColumnConfig Column on record field;
```

The `Column` annotation's keys' will be defined by the ColumnConfig closed record, which allows only `name` field.
