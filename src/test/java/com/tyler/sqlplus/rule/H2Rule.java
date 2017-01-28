package com.tyler.sqlplus.rule;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.List;

import com.tyler.sqlplus.annotation.LoadQuery;
import com.tyler.sqlplus.exception.SQLRuntimeException;
import com.tyler.sqlplus.function.Functions;

public class H2Rule extends AbstractDBRule {

	@Override
	public Connection getConnection() {
		return Functions.runSQL(() ->DriverManager.getConnection("jdbc:h2:mem:db1;DB_CLOSE_DELAY=-1", "sa", "sa"));
	}

}