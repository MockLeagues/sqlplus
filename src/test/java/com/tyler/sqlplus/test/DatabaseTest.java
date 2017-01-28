package com.tyler.sqlplus.test;

import com.tyler.sqlplus.rule.AbstractDBRule;
import com.tyler.sqlplus.rule.H2Rule;
import com.tyler.sqlplus.rule.MySQLRule;
import org.junit.After;
import org.junit.Before;
import org.junit.runners.Parameterized;

public class DatabaseTest {

    @Parameterized.Parameter
    public AbstractDBRule dbRule;

    @Parameterized.Parameters
    public static Object[][] data() {
        return new Object[][]{
                { new H2Rule() },
                { new MySQLRule() }
        };
    }

    @Before
    public void setupSchema() {
        dbRule.setupSchema();
    }

    @After
    public void destroyChema() {
        dbRule.destroySchema();
    }

}
