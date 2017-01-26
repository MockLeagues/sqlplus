package com.tyler.sqlplus.utility;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
	public void underscoreToCamelCaseWithNoUnderscores() {
		assertEquals("test", Fields.underscoreToCamelCase("TEST"));
	}

	@Test
	public void underscoreToCamelCaseWithUnderscores() {
		assertEquals("testField", Fields.underscoreToCamelCase("TEST_FIELD"));
	}

}


