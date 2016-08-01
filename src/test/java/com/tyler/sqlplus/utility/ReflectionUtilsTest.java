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
	
}
