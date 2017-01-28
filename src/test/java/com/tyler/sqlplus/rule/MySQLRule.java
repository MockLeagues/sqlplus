package com.tyler.sqlplus.rule;

import com.tyler.sqlplus.function.Functions;

public class MySQLRule extends AbstractDBRule {

    public MySQLRule() {
        Functions.run(() -> Class.forName(com.mysql.cj.jdbc.Driver.class.getName()));
    }

    @Override
    public String getUrl() {
        return "jdbc:mysql://localhost/sqlplus-test";
    }

    @Override
    public String getUsername() {
        return "sqlplus-test";
    }

    @Override
    public String getPassword() {
        return "sqlplus";
    }

}
