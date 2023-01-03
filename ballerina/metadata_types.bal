// Copyright (c) 2022 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

# Represents the type of the table/view retrieved through the `getTableInfo` function.
#
# + BASE_TABLE - The retrieved object is a table
# + VIEW - The retrieved object is a view
public enum TableType {
    BASE_TABLE = "BASE TABLE",
    VIEW = "VIEW"
}

# Represents the type of actions that would be taken on delete or on update for foreign key constraints.
#
# + NO_ACTION - Behaviour is dependent on the database
# + RESTRICT - Rejects the operations if the foreign key constraint is violated
# + CASCADE - The changes are propogated to the child table
# + NET_NULL - Sets the values in the child table's corresponding rows to `null`
# + SET_DEFAULT - Replaces the values in the child table's corresponding rows with the default value
public enum ReferentialRule {
    NO_ACTION = "NO ACTION",
    RESTRICT = "RESTRICT",
    CASCADE = "CASCADE",
    SET_NULL = "SET NULL",
    SET_DEFAULT = "SET DEFAULT"
}

# Represents the type of the routine retrieved through the `getRoutineInfo` function.
#
# + PROCEDURE - The retrieved object is a procedure
# + FUNCTION - The retrieved object is a function
public enum RoutineType {
    PROCEDURE = "PROCEDURE",
    FUNCTION = "FUNCTION"
}

# Represents the mode of the parameter for a routine.
#
# + IN - Only used to pass an input into the routine
# + OUT - Only used to retrieve an output from the routine
# + INOUT - Used to both pass and input and retrieve an output from the routine
public enum ParameterMode {
    IN = "IN",
    OUT = "OUT",
    INOUT = "INOUT"
}

# Represents a table in the database.
#
# + name - The name of the table
# + 'type - Whether the table is a base table or a view
# + columns - The columns included in the table
# + checkConstraints - Check constraints associated with the column
public type TableDefinition record {
    string name;
    TableType 'type;
    ColumnDefinition[] columns?;
    CheckConstraint[] checkConstraints?;
};

# Represents a column in a table.
#
# + name - The name of the column
# + 'type - The SQL data-type associated with the column
# + defaultValue - The default value of the column
# + nullable - Whether the column is nullable
# + referentialConstraints - Referential constraints (foreign key relationships) associated with the column
public type ColumnDefinition record {
    string name;
    string 'type;
    anydata? defaultValue;
    boolean nullable;
    ReferentialConstraint[] referentialConstraints?;
};

# Represents a referential constraint (foriegn key constraint).
# 
# + name - The name of the constraint
# + tableName - The name of the table which contains the referenced column
# + columnName - The name of the referenced column
# + updateRule - The action taken when an update statement violates the constraint
# + deleteRule - The action taken when a delete statement violates the constraint
public type ReferentialConstraint record {
    string name;
    string tableName;
    string columnName;
    ReferentialRule updateRule;
    ReferentialRule deleteRule; 
};

# Represents a check constraint.
# 
# + name - The name of the constraint
# + clause - The actual text of the SQL definition statement
public type CheckConstraint record {
    string name;
    string clause;
};

# Represents a routine.
# 
# + name - The name of the routine
# + 'type - The type of the routine (procedure or function)
# + returnType - If the routine returns a value, the return data-type. Else ()
# + parameters - The parameters associated with the routine
public type RoutineDefinition record {
    string name;
    RoutineType 'type;
    string? returnType;
    ParameterDefinition[] parameters;
};

# Represents a routine parameter.
# 
# + mode - The mode of the parameter (IN, OUT, INOUT)
# + name - The name of the parameter
# + type - The data-type of the parameter
public type ParameterDefinition record {
    ParameterMode mode;
    string name;
    string 'type;
};
