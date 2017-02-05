package com.tyler.sqlplus.base;

import org.junit.After;
import org.junit.Before;
import org.junit.runners.Parameterized;

public class DatabaseTest {

    @Parameterized.Parameter
    public AbstractDatabase db;

    @Parameterized.Parameters(name = "{0}")
    public static Object[][] getDatabasesToTestAgainst() {
        return new Object[][]{
            { new H2Database() },
            { new MySQLDatabase() },
//            { new PostgreSQLDatabase() } Needs more work for schema setup
        };
    }

    @Before
    public void setupSchema() {
        destroySchema();
        db.setupSchema();
    }

    @After
    public void destroySchema() {
        db.destroySchema();
    }

}
