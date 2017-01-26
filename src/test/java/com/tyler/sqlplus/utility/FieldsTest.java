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

}


