package com.tyler.sqlplus.mapping;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

import com.tyler.sqlplus.ResultMapper;
import com.tyler.sqlplus.mapping.ResultMapperTest.MyPOJO.Size;

public class ResultMapperTest {

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
		
		MyPOJO pojo = ResultMapper.forType(MyPOJO.class).map(rsToMap);
		
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
		assertEquals(Size.SMALL, pojo.enumField);
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
		
		POJOWithNullFields pojo = ResultMapper.forType(POJOWithNullFields.class).map(rsToMap);
		
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
		when(rsToMap.getString("mappableA")).thenReturn("valA");
		when(rsToMap.getString("notMappable")).thenReturn("valB");
		
		Set<Field> mappableFields = ResultMapper.determineMappableFields(rsToMap, POJOMappableFields.class, new HashMap<>());
		
		assertTrue(mappableFields.contains(POJOMappableFields.class.getDeclaredField("mappableA")));
		assertFalse(mappableFields.contains(POJOMappableFields.class.getDeclaredField("mappableB")));
	}
	
	@Test
	public void testDetermineMappableFieldsWhenCustomMappingsArePresent() throws Exception {
		
		ResultSetMetaData rsMeta = mock(ResultSetMetaData.class);
		when(rsMeta.getColumnCount()).thenReturn(2);
		when(rsMeta.getColumnLabel(1)).thenReturn("mappableA");
		when(rsMeta.getColumnLabel(2)).thenReturn("customField");
		
		ResultSet rsToMap = mock(ResultSet.class);
		when(rsToMap.getMetaData()).thenReturn(rsMeta);
		when(rsToMap.getString("mappableA")).thenReturn("valA");
		when(rsToMap.getString("customField")).thenReturn("valB");
		
		Map<String, String> customMappings = new HashMap<>();
		customMappings.put("customField", "mappableB");
		Set<Field> mappableFields = ResultMapper.determineMappableFields(rsToMap, POJOMappableFields.class, customMappings);
		
		assertTrue(mappableFields.contains(POJOMappableFields.class.getDeclaredField("mappableA")));
		assertTrue(mappableFields.contains(POJOMappableFields.class.getDeclaredField("mappableB")));
	}
	
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

	@SuppressWarnings("unchecked")
	@Test
	public void testDetermineCollectionImpl() throws Exception {
		assertEquals(new ArrayList<>(), ResultMapper.determineCollectionImpl(Collection.class));
		assertEquals(new ArrayList<>(), ResultMapper.determineCollectionImpl(List.class));
		assertEquals(new HashSet<>(), ResultMapper.determineCollectionImpl(Set.class));
		assertEquals(new TreeSet<>(), ResultMapper.determineCollectionImpl(TreeSet.class));
		assertEquals(new LinkedList<>(), ResultMapper.determineCollectionImpl(LinkedList.class));
		assertEquals(new LinkedList<>(), ResultMapper.determineCollectionImpl(Deque.class));
		assertEquals(new LinkedList<>(), ResultMapper.determineCollectionImpl(Queue.class));
		assertTrue(ResultMapper.determineCollectionImpl(PriorityQueue.class) instanceof PriorityQueue);
	}
	
}
