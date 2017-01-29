package com.tyler.sqlplus.base.databases;

import com.tyler.sqlplus.function.Functions;

public class PostgreSQLDatabase extends AbstractDatabase {

    public PostgreSQLDatabase() {
        Functions.run(() -> Class.forName(org.postgresql.Driver.class.getName()));
    }

    @Override
    public String getUrl() {
        return "jdbc:postgresql://localhost:5432/sqlplus";
    }

    @Override
    public String getUsername() {
        return "sqlplus";
    }

    @Override
    public String getPassword() {
        return "sqlplus";
    }

    @Override
    public String toString() {
        return "postgres";
    }

}
