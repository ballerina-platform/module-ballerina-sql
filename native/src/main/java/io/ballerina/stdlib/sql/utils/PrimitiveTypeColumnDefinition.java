package io.ballerina.stdlib.sql.utils;

import io.ballerina.runtime.api.types.Type;

/**
 * This class provides the mapping of the sql columns, its names and types of grouped columns for inner record type.
 **/
public class PrimitiveTypeColumnDefinition extends ColumnDefinition {
    private final String columnName;
    private final int sqlType;
    private final String sqlTypeName;
    private final boolean isNullable;
    private final int resultSetColumnIndex;

    public PrimitiveTypeColumnDefinition(String columnName, int sqlType, String sqlTypeName, boolean isNullable,
                                         int resultSetColumnIndex, String ballerinaFieldName, Type ballerinaType) {
        super(ballerinaFieldName, ballerinaType);
        this.columnName = columnName;
        this.sqlType = sqlType;
        this.sqlTypeName = sqlTypeName;
        this.isNullable = isNullable;
        this.resultSetColumnIndex = resultSetColumnIndex;
    }

    public String getColumnName() {
        return columnName;
    }

    public int getSqlType() {
        return sqlType;
    }

    public String getSqlTypeName() {
        return sqlTypeName;
    }

    public boolean isNullable() {
        return isNullable;
    }

    public int getResultSetColumnIndex() {
        return resultSetColumnIndex;
    }

}
