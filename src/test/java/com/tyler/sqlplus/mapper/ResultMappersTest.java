package com.tyler.sqlplus.mapper;

import com.tyler.sqlplus.Session;
import com.tyler.sqlplus.annotation.LoadQuery;
import com.tyler.sqlplus.conversion.ConversionRegistry;
import javassist.util.proxy.Proxy;
import org.junit.Test;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResultMappersTest {

	@Test
	public void testMapStringArray() throws Exception {
		
		ResultSetMetaData rsMeta = mock(ResultSetMetaData.class);
		when(rsMeta.getColumnCount()).thenReturn(2);
		
		ResultSet rsToMap = mock(ResultSet.class);
		when(rsToMap.getMetaData()).thenReturn(rsMeta);
		when(rsToMap.getString(1)).thenReturn("valA");
		when(rsToMap.getString(2)).thenReturn("valB");
		
		String[] row = ResultMappers.forStringArray().map(rsToMap);
		assertArrayEquals(new String[]{"valA", "valB"}, row);
	}
	
	public static class MyPOJO {
		
		enum Size { SMALL, MEDIUM, LARGE; }
		
		private int intField;         private Integer bigIntField;
		private float floatField;     private Float bigFloatField;
		private short shortField;     private Short bigShortField;
		private long longField;       private Long bigLongField;
		private double doubleField;   private Double bigDoubleField;
		private boolean boolField;    private Boolean bigBoolField;
		private char charField;       private Character bigCharField;
		
		private String stringField;
		private Size enumField;
		private LocalDate localDateField;
		
	}
	
	@Test
	public void testMapRowToPOJO() throws Exception {
		
		ResultSetMetaData rsMeta = mock(ResultSetMetaData.class);
		when(rsMeta.getColumnCount()).thenReturn(17);
		when(rsMeta.getColumnLabel(1)).thenReturn("intField");
		when(rsMeta.getColumnLabel(2)).thenReturn("bigIntField");
		when(rsMeta.getColumnLabel(3)).thenReturn("floatField");
		when(rsMeta.getColumnLabel(4)).thenReturn("bigFloatField");
		when(rsMeta.getColumnLabel(5)).thenReturn("shortField");
		when(rsMeta.getColumnLabel(6)).thenReturn("bigShortField");
		when(rsMeta.getColumnLabel(7)).thenReturn("longField");
		when(rsMeta.getColumnLabel(8)).thenReturn("bigLongField");
		when(rsMeta.getColumnLabel(9)).thenReturn("doubleField");
		when(rsMeta.getColumnLabel(10)).thenReturn("bigDoubleField");
		when(rsMeta.getColumnLabel(11)).thenReturn("boolField");
		when(rsMeta.getColumnLabel(12)).thenReturn("bigBoolField");
		when(rsMeta.getColumnLabel(13)).thenReturn("charField");
		when(rsMeta.getColumnLabel(14)).thenReturn("bigCharField");
		when(rsMeta.getColumnLabel(15)).thenReturn("stringField");
		when(rsMeta.getColumnLabel(16)).thenReturn("enumField");
		when(rsMeta.getColumnLabel(17)).thenReturn("localDateField");
		
		ResultSet rsToMap = mock(ResultSet.class);
		
		when(rsToMap.getMetaData()).thenReturn(rsMeta);
		when(rsToMap.getInt("intField")).thenReturn(1);
		when(rsToMap.getFloat("floatField")).thenReturn(1.5f);
		when(rsToMap.getShort("shortField")).thenReturn((short) 2);
		when(rsToMap.getLong("longField")).thenReturn((long) 3);
		when(rsToMap.getDouble("doubleField")).thenReturn((double) 4);
		when(rsToMap.getBoolean("boolField")).thenReturn(true);
		when(rsToMap.getString("charField")).thenReturn("c");
		
		when(rsToMap.getInt("bigIntField")).thenReturn(1);
		when(rsToMap.getFloat("bigFloatField")).thenReturn(1.5f);
		when(rsToMap.getShort("bigShortField")).thenReturn((short) 2);
		when(rsToMap.getLong("bigLongField")).thenReturn((long) 3);
		when(rsToMap.getDouble("bigDoubleField")).thenReturn((double) 4);
		when(rsToMap.getBoolean("bigBoolField")).thenReturn(true);
		when(rsToMap.getString("bigCharField")).thenReturn("c");
		
		when(rsToMap.getString("stringField")).thenReturn("string");
		when(rsToMap.getString("enumField")).thenReturn("SMALL");
		when(rsToMap.getString("localDateField")).thenReturn("2015-01-01");
		
		MyPOJO pojo = ResultMappers.forClass(MyPOJO.class, new ConversionRegistry(), mock(Session.class)).map(rsToMap);
		
		assertEquals(1, pojo.intField);
		assertEquals(new Float(1.5), new Float(pojo.floatField));
		assertEquals(new Short((short) 2), new Short(pojo.shortField));
		assertEquals(new Long(3), new Long(pojo.longField));
		assertEquals(new Double(4.0), new Double(pojo.doubleField));
		assertEquals(true, pojo.boolField);
		assertEquals('c', pojo.charField);
		
		assertEquals(new Integer(1), pojo.bigIntField);
		assertEquals(new Float(1.5), new Float(pojo.bigFloatField));
		assertEquals(new Short((short) 2), new Short(pojo.bigShortField));
		assertEquals(new Long(3), new Long(pojo.bigLongField));
		assertEquals(new Double(4.0), new Double(pojo.bigDoubleField));
		assertEquals(true, pojo.bigBoolField);
		assertEquals(new Character('c'), pojo.bigCharField);
		
		assertEquals("string", pojo.stringField);
		assertEquals(MyPOJO.Size.SMALL, pojo.enumField);
		assertEquals(LocalDate.of(2015, 1, 1), pojo.localDateField);
	}
	
	public static class POJOWithNullFields {
		private Integer presentField;
		private String nonPresentField;
	}
	
	@Test
	public void testFieldsNotPresentInResultSetRemainNull() throws Exception {
		
		ResultSetMetaData rsMeta = mock(ResultSetMetaData.class);
		when(rsMeta.getColumnCount()).thenReturn(1);
		when(rsMeta.getColumnLabel(1)).thenReturn("presentField");
		
		ResultSet rsToMap = mock(ResultSet.class);
		when(rsToMap.getMetaData()).thenReturn(rsMeta);
		when(rsToMap.getInt("presentField")).thenReturn(1);
		
		POJOWithNullFields pojo = ResultMappers.forClass(POJOWithNullFields.class, new ConversionRegistry(), mock(Session.class)).map(rsToMap);
		
		assertEquals(new Integer(1), pojo.presentField);
		assertNull(pojo.nonPresentField);
	}
	
	public static class POJOMappableFields {
		public String mappableA, mappableB;
	}
	
	@Test
	public void testDetermineMappableFieldsWithNoCustomMappingsOrRelationClasses() throws Exception {
		
		ResultSetMetaData rsMeta = mock(ResultSetMetaData.class);
		when(rsMeta.getColumnCount()).thenReturn(2);
		when(rsMeta.getColumnLabel(1)).thenReturn("mappableA");
		when(rsMeta.getColumnLabel(2)).thenReturn("notMappable");
		
		ResultSet rsToMap = mock(ResultSet.class);
		when(rsToMap.getMetaData()).thenReturn(rsMeta);
		
		Map<Field, String> mappableFields = ResultMappers.determineLoadableFields(rsToMap, POJOMappableFields.class);
		
		assertTrue(mappableFields.containsKey(POJOMappableFields.class.getDeclaredField("mappableA")));
		assertFalse(mappableFields.containsKey(POJOMappableFields.class.getDeclaredField("mappableB")));
	}
	
	static class ProxiablePOJOByField {
		
		String id, name;
		
		@LoadQuery("select * from table")
		List<String> relations;
		
	}
	
	@Test
	public void testProxyIsReturnedWhenLazyLoadFieldsPresent() throws Exception {

		ResultSetMetaData rsMeta = mock(ResultSetMetaData.class);
		when(rsMeta.getColumnCount()).thenReturn(2);
		when(rsMeta.getColumnLabel(1)).thenReturn("id");
		when(rsMeta.getColumnLabel(2)).thenReturn("name");
		
		ResultSet rsToMap = mock(ResultSet.class);
		
		when(rsToMap.getMetaData()).thenReturn(rsMeta);
		when(rsToMap.getString("id")).thenReturn("12345");
		when(rsToMap.getString("name")).thenReturn("fakeyMcMadeup");
		
		ProxiablePOJOByField proxy = ResultMappers.forClass(ProxiablePOJOByField.class, new ConversionRegistry(), mock(Session.class)).map(rsToMap);
		assertTrue(proxy instanceof Proxy);
	}
	
	static class ProxiablePOJOByMethod {
		
		String id, name;
		
		List<String> relations;
		
		@LoadQuery("select * from table")
		public List<String> getRelations() {
			return relations;
		}
		
	}
	
	@Test
	public void testProxyIsReturnedWhenLazyLoadMethodsPresent() throws Exception {

		ResultSetMetaData rsMeta = mock(ResultSetMetaData.class);
		when(rsMeta.getColumnCount()).thenReturn(2);
		when(rsMeta.getColumnLabel(1)).thenReturn("id");
		when(rsMeta.getColumnLabel(2)).thenReturn("name");
		
		ResultSet rsToMap = mock(ResultSet.class);
		
		when(rsToMap.getMetaData()).thenReturn(rsMeta);
		when(rsToMap.getString("id")).thenReturn("12345");
		when(rsToMap.getString("name")).thenReturn("fakeyMcMadeup");
		
		ProxiablePOJOByMethod proxy = ResultMappers.forClass(ProxiablePOJOByMethod.class, new ConversionRegistry(), mock(Session.class)).map(rsToMap);
		assertTrue(proxy instanceof Proxy);
	}
	
	static class NormalPOJO {
		String id, name;
	}
	
	@Test
	public void testRegularPOJOIsReturnedWhenNoLazyLoadFieldPresent() throws Exception {
		
		ResultSetMetaData rsMeta = mock(ResultSetMetaData.class);
		when(rsMeta.getColumnCount()).thenReturn(2);
		when(rsMeta.getColumnLabel(1)).thenReturn("id");
		when(rsMeta.getColumnLabel(2)).thenReturn("name");
		
		ResultSet rsToMap = mock(ResultSet.class);
		
		when(rsToMap.getMetaData()).thenReturn(rsMeta);
		when(rsToMap.getString("id")).thenReturn("12345");
		when(rsToMap.getString("name")).thenReturn("fakeyMcMadeup");
		
		NormalPOJO pojo = ResultMappers.forClass(NormalPOJO.class, new ConversionRegistry(), mock(Session.class)).map(rsToMap);
		assertFalse(pojo instanceof Proxy);
		assertTrue(pojo.getClass() == NormalPOJO.class);
	}
	
}
