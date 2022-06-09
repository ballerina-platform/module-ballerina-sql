import ballerina/time;

public enum TableType {
    BASE_TABLE,
    VIEW
}

public enum ReferentialRule {
    NO_ACTION,
    CASCADE,
    SET_NULL,
    SET_DEFAULT
}

public enum RoutineType {
    PROCEDURE,
    FUNCTION
}

public enum ParameterMode {
    IN,
    OUT,
    INOUT
}

# Represents a table in the database.
#
# + name - The name of the table
# + 'type - Whether the table is a base table or a view
# + columns - The columns included in the table
public type TableDefinition record {
    string name;
    TableType 'type;
    ColumnDefinition[] columns?;
};

# Represents a column in a table.
#
# + name - The name of the column
# + 'type - The SQL data-type associated with the column
# + defaultValue - The default value of the column
# + nullable - Whether the column is nullable
# + referentialConstraints - Referential constraints (foreign key relationships) associated with the column
# + checkConstraints - Check constraints associated with the column
public type ColumnDefinition record {
    string name;
    string 'type;
    anydata? defaultValue;
    boolean nullable;
    ReferentialConstraint[] referentialConstraints?;
    CheckConstraint[] checkConstraints?;
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
# + created - The timestamp at which the routine was created
# + lastAltered- The timestamp at which the routine was last altered
public type RoutineDefinition record {
    string name;
    RoutineType 'type;
    string? returnType;
    ParameterDefinition[] parameters;
    time:Civil created;
    time:Civil lastAltered;
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
