package com.tyler.sqlplus.spring;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.tyler.sqlplus.SqlPlus;

@Component("sb")
public class SpringBean {

	@Autowired
	private SqlPlus sqlPlus;
	
	public SqlPlus getSqlPlus() {
		return sqlPlus;
	}
	
}
