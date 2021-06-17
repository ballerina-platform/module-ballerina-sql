package org.ballerinalang.sql.tests.exception;

import org.ballerinalang.sql.exception.ApplicationError;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Application Error tests.
 *
 * @since 0.6.0-beta.2
 */
public class ApplicationErrorTest {
    @Test
    void applicationErrorTest1() {
        ApplicationError error = new ApplicationError("Application Error", new SQLException("new SQL Exception"));
        assertEquals(error.getMessage(), "Application Error");
    }
}
