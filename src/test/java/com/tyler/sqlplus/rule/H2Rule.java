package com.tyler.sqlplus.rule;

public class H2Rule extends AbstractDBRule {

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

}