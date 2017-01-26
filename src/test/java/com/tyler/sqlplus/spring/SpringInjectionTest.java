package com.tyler.sqlplus.spring;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class SpringInjectionTest {

	@Test
	public void testSqlPlusGetsInjectedWhenSettingConfigurationObjectInBeansXML() {
		@SuppressWarnings("resource")
		ApplicationContext context = new ClassPathXmlApplicationContext("beans.xml");
		SpringBean bean = context.getBean(SpringBean.class);
		assertNotNull(bean);
		assertNotNull(bean.getSqlPlus());
	}
	
}
