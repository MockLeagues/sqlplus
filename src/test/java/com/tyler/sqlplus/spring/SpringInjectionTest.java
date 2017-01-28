package com.tyler.sqlplus.spring;

import org.junit.Test;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertNotNull;

public class SpringInjectionTest {

	@Test
	public void testSQLPlusGetsInjectedWhenSettingConfigurationObjectInBeansXML() {
		Logger.getLogger(ClassPathXmlApplicationContext.class.getName()).setLevel(Level.OFF);
		Logger.getLogger(XmlBeanDefinitionReader.class.getName()).setLevel(Level.OFF);
		@SuppressWarnings("resource")
		ApplicationContext context = new ClassPathXmlApplicationContext("beans.xml");
		SpringBean bean = context.getBean(SpringBean.class);
		assertNotNull(bean);
		assertNotNull(bean.getSQLPlus());
	}
	
}
