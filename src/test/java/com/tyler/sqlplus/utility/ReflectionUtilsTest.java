package com.tyler.sqlplus.utility;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ReflectionUtilsTest {

	@Test
	public void testExtractFieldName() throws Exception {
		
		// Verify 'get' prefix
		assertEquals("myField", ReflectionUtils.extractFieldName("getMyField"));
		
		// Verify 'is' prefix
		assertEquals("allowed", ReflectionUtils.extractFieldName("isAllowed"));
		
		// Verify 'set' prefix
		assertEquals("age", ReflectionUtils.extractFieldName("setAge"));
		
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testThrowsExceptionIfNotJavaBeansStyle() throws Exception {
		ReflectionUtils.extractFieldName("incrementAge");
	}

	@Test
	public void testUnderscoreToCamelCaseWithAllLowercaseDBFieldName() throws Exception {
		assertEquals("myAwesomeField", ReflectionUtils.underscoreToCamelCase("my_awesome_field"));
	}
	
	@Test
	public void testUnderscoreToCamelCaseWithAllUppercaseDBFieldName() throws Exception {
		assertEquals("myAwesomeField", ReflectionUtils.underscoreToCamelCase("MY_AWESOME_FIELD"));
	}
	
	@Test
	public void testUnderscoreToCamelCaseWithOnlyOneWord() throws Exception {
		assertEquals("name", ReflectionUtils.underscoreToCamelCase("name"));
		assertEquals("name", ReflectionUtils.underscoreToCamelCase("NAME"));
	}
	
	@Test
	public void testCamelCaseToUnderscoreWithMultipleWords() throws Exception {
		assertEquals("my_awesome_field", ReflectionUtils.camelCaseToUnderscore("myAwesomeField"));
	}

	@Test
	public void testCamelCaseToUnderscoreWithOneWord() throws Exception {
		assertEquals("name", ReflectionUtils.camelCaseToUnderscore("name"));
	}
	
}


