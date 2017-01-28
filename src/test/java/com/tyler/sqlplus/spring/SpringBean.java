package com.tyler.sqlplus.spring;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.tyler.sqlplus.SQLPlus;

@Component("sb")
public class SpringBean {

	@Autowired
	private SQLPlus sqlPlus;
	
	public SQLPlus getSqlPlus() {
		return sqlPlus;
	}
	
}
