package com.tyler.sqlplus.utility;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ReflectionUtilsTest {

	@Test
	public void testExtractFieldName() throws Exception {
		
		// Verify 'get' prefix
		assertEquals("myField", Reflections.extractFieldName("getMyField"));
		
		// Verify 'is' prefix
		assertEquals("allowed", Reflections.extractFieldName("isAllowed"));
		
		// Verify 'set' prefix
		assertEquals("age", Reflections.extractFieldName("setAge"));
		
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testThrowsExceptionIfNotJavaBeansStyle() throws Exception {
		Reflections.extractFieldName("incrementAge");
	}

	@Test
	public void testUnderscoreToCamelCaseWithAllLowercaseDBFieldName() throws Exception {
		assertEquals("myAwesomeField", Reflections.underscoreToCamelCase("my_awesome_field"));
	}
	
	@Test
	public void testUnderscoreToCamelCaseWithAllUppercaseDBFieldName() throws Exception {
		assertEquals("myAwesomeField", Reflections.underscoreToCamelCase("MY_AWESOME_FIELD"));
	}
	
	@Test
	public void testUnderscoreToCamelCaseWithOnlyOneWord() throws Exception {
		assertEquals("name", Reflections.underscoreToCamelCase("name"));
		assertEquals("name", Reflections.underscoreToCamelCase("NAME"));
	}
	
	@Test
	public void testCamelCaseToUnderscoreWithMultipleWords() throws Exception {
		assertEquals("my_awesome_field", Reflections.camelCaseToUnderscore("myAwesomeField"));
	}

	@Test
	public void testCamelCaseToUnderscoreWithOneWord() throws Exception {
		assertEquals("name", Reflections.camelCaseToUnderscore("name"));
	}
	
}


