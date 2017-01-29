package com.tyler.sqlplus.base.databases;

import com.tyler.sqlplus.function.Functions;

public class MySQLDatabase extends AbstractDatabase {

    public MySQLDatabase() {
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

    @Override
    public String toString() {
        return "MySQL";
    }

}
