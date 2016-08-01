package com.tyler.sqlplus;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import org.junit.Test;

public class ResultMapperTest {

	@Test
	public void testMapStringArray() throws Exception {
		
		ResultSetMetaData rsMeta = mock(ResultSetMetaData.class);
		when(rsMeta.getColumnCount()).thenReturn(2);
		
		ResultSet rsToMap = mock(ResultSet.class);
		when(rsToMap.getMetaData()).thenReturn(rsMeta);
		when(rsToMap.getString(1)).thenReturn("valA");
		when(rsToMap.getString(2)).thenReturn("valB");
		
		String[] row = ResultMapper.forStringArray().map(rsToMap);
		assertArrayEquals(new String[]{"valA", "valB"}, row);
	}
	
}
