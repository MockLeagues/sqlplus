package com.tyler.sqlplus.base.databases;

public class H2Database extends AbstractDatabase {

	@Override
	public String getUrl() {
		return "jdbc:h2:mem:db1;DB_CLOSE_DELAY=-1";
	}

	@Override
	public String getUsername() {
		return "sa";
	}

	@Override
	public String getPassword() {
		return "sa";
	}

	@Override
	public String toString() {
		return "h2";
	}

}