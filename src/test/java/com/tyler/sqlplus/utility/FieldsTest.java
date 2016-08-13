package com.tyler.sqlplus.utility;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class FieldsTest {

	@Test
	public void testExtractFieldName() throws Exception {
		
		// Verify 'get' prefix
		assertEquals("myField", Fields.extractFieldName("getMyField"));
		
		// Verify 'is' prefix
		assertEquals("allowed", Fields.extractFieldName("isAllowed"));
		
		// Verify 'set' prefix
		assertEquals("age", Fields.extractFieldName("setAge"));
		
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void testThrowsExceptionIfNotJavaBeansStyle() throws Exception {
		Fields.extractFieldName("incrementAge");
	}

	@Test
	public void testUnderscoreToCamelCaseWithAllLowercaseDBFieldName() throws Exception {
		assertEquals("myAwesomeField", Fields.underscoreToCamelCase("my_awesome_field"));
	}
	
	@Test
	public void testUnderscoreToCamelCaseWithAllUppercaseDBFieldName() throws Exception {
		assertEquals("myAwesomeField", Fields.underscoreToCamelCase("MY_AWESOME_FIELD"));
	}
	
	@Test
	public void testUnderscoreToCamelCaseWithOnlyOneWord() throws Exception {
		assertEquals("name", Fields.underscoreToCamelCase("name"));
		assertEquals("name", Fields.underscoreToCamelCase("NAME"));
	}
	
	@Test
	public void testCamelCaseToUnderscoreWithMultipleWords() throws Exception {
		assertEquals("my_awesome_field", Fields.camelCaseToUnderscore("myAwesomeField"));
	}

	@Test
	public void testCamelCaseToUnderscoreWithOneWord() throws Exception {
		assertEquals("name", Fields.camelCaseToUnderscore("name"));
	}
	
}


