package org.ballerinalang.sql.tests;

import org.ballerinalang.sql.Constants;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Constants class test.
 */
public class ConstantsTest {

    @Test
    void constantsTest() {
        Constants con = new Constants();
        assertEquals(con.DATABASE_CLIENT, Constants.DATABASE_CLIENT);
    }

    @Test
    void connectionPoolTest() {
        Constants.ConnectionPool con = new Constants.ConnectionPool();
        assertEquals(con.MAX_OPEN_CONNECTIONS, Constants.ConnectionPool.MAX_OPEN_CONNECTIONS);
    }

    @Test
    void optionsTest() {
        Constants.Options con = new Constants.Options();
        assertEquals(con.URL, Constants.Options.URL);
    }

    @Test
    void errorRecordFieldsTest() {
        Constants.ErrorRecordFields con = new Constants.ErrorRecordFields();
        assertEquals(con.ERROR_CODE, Constants.ErrorRecordFields.ERROR_CODE);
    }

    @Test
    void parameterizedQueryFieldsTest() {
        Constants.ParameterizedQueryFields con = new Constants.ParameterizedQueryFields();
        assertEquals(con.STRINGS, Constants.ParameterizedQueryFields.STRINGS);
    }

    @Test
    void typedValueFieldsTest() {
        Constants.TypedValueFields con = new Constants.TypedValueFields();
        assertEquals(con.VALUE, Constants.TypedValueFields.VALUE);
    }

    @Test
    void sqlTypesTest() {
        Constants.SqlTypes con = new Constants.SqlTypes();
        assertEquals(con.TEXT, Constants.SqlTypes.TEXT);
    }

    @Test
    void sqlArraysTest() {
        Constants.SqlArrays con = new Constants.SqlArrays();
        assertEquals(con.INTEGER, Constants.SqlArrays.INTEGER);
    }

    @Test
    void outParameterTypesTest() {
        Constants.OutParameterTypes con = new Constants.OutParameterTypes();
        assertEquals(con.TEXT, Constants.OutParameterTypes.TEXT);
    }

    @Test
    void sqlParamsFieldsTest() {
        Constants.SQLParamsFields con = new Constants.SQLParamsFields();
        assertEquals(con.URL, Constants.SQLParamsFields.URL);
    }

    @Test
    void parameterObjectTest() {
        Constants.ParameterObject con = new Constants.ParameterObject();
        assertEquals(con.INOUT_PARAMETER, Constants.ParameterObject.INOUT_PARAMETER);
    }

    @Test
    void classesTest() {
        Constants.Classes con = new Constants.Classes();
        assertEquals(con.STRING, Constants.Classes.STRING);
    }

}
