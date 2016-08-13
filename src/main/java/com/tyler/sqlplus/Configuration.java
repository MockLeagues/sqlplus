package com.tyler.sqlplus;

public class Configuration {

	private String url;
	private String username;
	private String password;
	private String driverClass;
	private boolean convertCamelCaseToUnderscore = false;
	
	public String getUrl() {
		return url;
	}
	
	public Configuration setUrl(String url) {
		this.url = url;
		return this;
	}
	
	public String getUsername() {
		return username;
	}
	
	public Configuration setUsername(String username) {
		this.username = username;
		return this;
	}
	
	public String getPassword() {
		return password;
	}
	
	public Configuration setPassword(String password) {
		this.password = password;
		return this;
	}
	
	public String getDriverClass() {
		return driverClass;
	}
	
	public void setDriverClass(String driverClass) {
		this.driverClass = driverClass;
	}

	public boolean isConvertUnderscoreToCamelCase() {
		return convertCamelCaseToUnderscore;
	}

	public Configuration setConvertUnderscoreToCamelCase(boolean convertCamelCaseToUnderscore) {
		this.convertCamelCaseToUnderscore = convertCamelCaseToUnderscore;
		return this;
	}
	
}
