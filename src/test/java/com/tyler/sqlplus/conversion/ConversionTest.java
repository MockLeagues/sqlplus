package com.tyler.sqlplus.conversion;

import com.tyler.sqlplus.rule.AbstractDBRule;
import com.tyler.sqlplus.rule.H2EmployeeDBRule;
import org.junit.Rule;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;

import static com.tyler.sqlplus.test.SqlPlusTesting.assertThrows;
import static org.junit.Assert.*;

public class ConversionTest {

	@Rule
	public final AbstractDBRule h2 = new H2EmployeeDBRule();
	
	public static class TypesBag {
		int tinyInt;
		Integer bigInt;
		short tinyShort;
		Short bigShort;
		long tinyLong;
		Long bigLong;
		float tinyFloat;
		Float bigFloat;
		double tinyDouble;
		Double bigDouble;
		BigInteger hugeInt;
		BigDecimal hugeDouble;
		boolean tinyBoolean;
		Boolean bigBoolean;
		char tinyChar;
		Character bigChar;
		String string;
		Date javaUtilDate;
		Timestamp timestamp;
		LocalDate localDate;
		LocalDateTime localDateTime;
		LocalTime localTime;
	}
	
	@Test
	public void testReadPresentTinyInt() throws Exception {
		h2.batch("insert into types_table(int_field) values (10)");
		int dbResult = h2.getSQLPlus().query(s -> s.createQuery("select int_field as \"tinyInt\" from types_table").getUniqueResultAs(TypesBag.class).tinyInt);
		assertEquals(10, dbResult);
	}
	
	@Test
	public void testReadNullTinyInt() throws Exception {
		h2.batch("insert into types_table(decimal_field) values (10.5)");
		int dbResult = h2.getSQLPlus().query(s -> s.createQuery("select int_field as \"tinyInt\" from types_table").getUniqueResultAs(TypesBag.class).tinyInt);
		assertEquals(0, dbResult);
	}
	
	@Test
	public void testReadPresentBigInt() throws Exception {
		h2.batch("insert into types_table(int_field) values (10)");
		Integer dbResult = h2.getSQLPlus().query(s -> s.createQuery("select int_field as \"bigInt\" from types_table").getUniqueResultAs(TypesBag.class).bigInt);
		assertEquals(new Integer(10), dbResult);
	}
	
	@Test
	public void testReadNullBigInt() throws Exception {
		h2.batch("insert into types_table(decimal_field) values (10.5)");
		Integer dbResult = h2.getSQLPlus().query(s -> s.createQuery("select int_field as \"bigInt\" from types_table").getUniqueResultAs(TypesBag.class).bigInt);
		assertNull(dbResult);
	}
	
	@Test
	public void testReadPresentTinyShort() throws Exception {
		h2.batch("insert into types_table(int_field) values (10)");
		short dbResult = h2.getSQLPlus().query(s -> s.createQuery("select int_field as \"tinyShort\" from types_table").getUniqueResultAs(TypesBag.class).tinyShort);
		assertEquals(10, dbResult);
	}
	
	@Test
	public void testReadNullTinyShort() throws Exception {
		h2.batch("insert into types_table(decimal_field) values (10.5)");
		short dbResult = h2.getSQLPlus().query(s -> s.createQuery("select int_field as \"tinyShort\" from types_table").getUniqueResultAs(TypesBag.class).tinyShort);
		assertEquals(0, dbResult);
	}
	
	@Test
	public void testReadPresentBigShort() throws Exception {
		h2.batch("insert into types_table(int_field) values (10)");
		Short dbResult = h2.getSQLPlus().query(s -> s.createQuery("select int_field as \"bigShort\" from types_table").getUniqueResultAs(TypesBag.class).bigShort);
		assertEquals(new Short((short) 10), dbResult);
	}
	
	@Test
	public void testReadNullBigShort() throws Exception {
		h2.batch("insert into types_table(decimal_field) values (10.5)");
		Short dbResult = h2.getSQLPlus().query(s -> s.createQuery("select int_field as \"bigShort\" from types_table").getUniqueResultAs(TypesBag.class).bigShort);
		assertNull(dbResult);
	}
	
	@Test
	public void testReadPresentTinyLong() throws Exception {
		h2.batch("insert into types_table(int_field) values (10)");
		long dbResult = h2.getSQLPlus().query(s -> s.createQuery("select int_field as \"tinyLong\" from types_table").getUniqueResultAs(TypesBag.class).tinyLong);
		assertEquals(10, dbResult);
	}
	
	@Test
	public void testReadNullTinyLong() throws Exception {
		h2.batch("insert into types_table(decimal_field) values (10.5)");
		long dbResult = h2.getSQLPlus().query(s -> s.createQuery("select int_field as \"tinyLong\" from types_table").getUniqueResultAs(TypesBag.class).tinyLong);
		assertEquals(0, dbResult);
	}
	
	@Test
	public void testReadPresentBigLong() throws Exception {
		h2.batch("insert into types_table(int_field) values (10)");
		Long dbResult = h2.getSQLPlus().query(s -> s.createQuery("select int_field as \"bigLong\" from types_table").getUniqueResultAs(TypesBag.class).bigLong);
		assertEquals(new Long((long) 10), dbResult);
	}
	
	@Test
	public void testReadNullBigLong() throws Exception {
		h2.batch("insert into types_table(decimal_field) values (10.5)");
		Long dbResult = h2.getSQLPlus().query(s -> s.createQuery("select int_field as \"bigLong\" from types_table").getUniqueResultAs(TypesBag.class).bigLong);
		assertNull(dbResult);
	}
	
	@Test
	public void testReadPresentTinyFloat() throws Exception {
		h2.batch("insert into types_table(float_field) values (1.5)");
		float dbResult = h2.getSQLPlus().query(s -> s.createQuery("select float_field as \"tinyFloat\" from types_table").getUniqueResultAs(TypesBag.class).tinyFloat);
		assertEquals(1.5f, dbResult, .1);
	}
	
	@Test
	public void testReadNullTinyFloat() throws Exception {
		h2.batch("insert into types_table(int_field) values (5)");
		float dbResult = h2.getSQLPlus().query(s -> s.createQuery("select float_field as \"tinyFloat\" from types_table").getUniqueResultAs(TypesBag.class).tinyFloat);
		assertEquals(0f, dbResult, .1);
	}
	
	@Test
	public void testReadPresentBigFloat() throws Exception {
		h2.batch("insert into types_table(float_field) values (1.5)");
		Float dbResult = h2.getSQLPlus().query(s -> s.createQuery("select float_field as \"bigFloat\" from types_table").getUniqueResultAs(TypesBag.class).bigFloat);
		assertEquals(new Float(1.5), dbResult);
	}
	
	@Test
	public void testReadNullBigFloat() throws Exception {
		h2.batch("insert into types_table(decimal_field) values (10.5)");
		Float dbResult = h2.getSQLPlus().query(s -> s.createQuery("select float_field as \"bigFloat\" from types_table").getUniqueResultAs(TypesBag.class).bigFloat);
		assertNull(dbResult);
	}
	
	@Test
	public void testReadPresentTinyDouble() throws Exception {
		h2.batch("insert into types_table(decimal_field) values (1.5)");
		double dbResult = h2.getSQLPlus().query(s -> s.createQuery("select decimal_field as \"tinyDouble\" from types_table").getUniqueResultAs(TypesBag.class).tinyDouble);
		assertEquals(1.5f, dbResult, .1);
	}
	
	@Test
	public void testReadNullTinyDouble() throws Exception {
		h2.batch("insert into types_table(int_field) values (5)");
		double dbResult = h2.getSQLPlus().query(s -> s.createQuery("select decimal_field as \"tinyDouble\" from types_table").getUniqueResultAs(TypesBag.class).tinyDouble);
		assertEquals(0f, dbResult, .1);
	}
	
	@Test
	public void testReadPresentBigDouble() throws Exception {
		h2.batch("insert into types_table(decimal_field) values (1.5)");
		Double dbResult = h2.getSQLPlus().query(s -> s.createQuery("select decimal_field as \"bigDouble\" from types_table").getUniqueResultAs(TypesBag.class).bigDouble);
		assertEquals(new Double(1.5), dbResult);
	}
	
	@Test
	public void testReadNullBigDouble() throws Exception {
		h2.batch("insert into types_table(int_field) values (1)");
		Double dbResult = h2.getSQLPlus().query(s -> s.createQuery("select decimal_field as \"bigDouble\" from types_table").getUniqueResultAs(TypesBag.class).bigDouble);
		assertNull(dbResult);
	}
	
	@Test
	public void testReadPresentTinyBoolean() throws Exception {
		h2.batch("insert into types_table(tiny_int_field) values (1)");
		boolean dbResult = h2.getSQLPlus().query(s -> s.createQuery("select tiny_int_field as \"tinyBoolean\" from types_table").getUniqueResultAs(TypesBag.class).tinyBoolean);
		assertTrue(dbResult);
	}
	
	@Test
	public void testReadNullTinyBoolean() throws Exception {
		h2.batch("insert into types_table(int_field) values (5)");
		boolean dbResult = h2.getSQLPlus().query(s -> s.createQuery("select tiny_int_field as \"tinyBoolean\" from types_table").getUniqueResultAs(TypesBag.class).tinyBoolean);
		assertFalse(dbResult);
	}
	
	@Test
	public void testReadPresentBigBoolean() throws Exception {
		h2.batch("insert into types_table(tiny_int_field) values (1)");
		Boolean dbResult = h2.getSQLPlus().query(s -> s.createQuery("select tiny_int_field as \"bigBoolean\" from types_table").getUniqueResultAs(TypesBag.class).bigBoolean);
		assertTrue(dbResult);
	}
	
	@Test
	public void testReadNullBigBoolean() throws Exception {
		h2.batch("insert into types_table(int_field) values (1)");
		Boolean dbResult = h2.getSQLPlus().query(s -> s.createQuery("select tiny_int_field as \"bigBoolean\" from types_table").getUniqueResultAs(TypesBag.class).bigBoolean);
		assertNull(dbResult);
	}
	
	@Test
	public void testReadPresentTinyChar() throws Exception {
		h2.batch("insert into types_table(char_field) values ('a')");
		char dbResult = h2.getSQLPlus().query(s -> s.createQuery("select char_field as \"tinyChar\" from types_table").getUniqueResultAs(TypesBag.class).tinyChar);
		assertEquals('a', dbResult);
	}
	
	@Test
	public void testReadNullTinyChar() throws Exception {
		h2.batch("insert into types_table(int_field) values (5)");
		char dbResult = h2.getSQLPlus().query(s -> s.createQuery("select char_field as \"tinyChar\" from types_table").getUniqueResultAs(TypesBag.class).tinyChar);
		assertEquals(Character.MIN_VALUE, dbResult);
	}
	
	@Test
	public void testReadPresentBigChar() throws Exception {
		h2.batch("insert into types_table(char_field) values ('a')");
		Character dbResult = h2.getSQLPlus().query(s -> s.createQuery("select char_field as \"bigChar\" from types_table").getUniqueResultAs(TypesBag.class).bigChar);
		assertEquals(new Character('a'), dbResult);
	}
	
	@Test
	public void testReadNullBigChar() throws Exception {
		h2.batch("insert into types_table(int_field) values (1)");
		Character dbResult = h2.getSQLPlus().query(s -> s.createQuery("select char_field as \"bigChar\" from types_table").getUniqueResultAs(TypesBag.class).bigChar);
		assertNull(dbResult);
	}
	
	@Test
	public void testReadPresentString() throws Exception {
		h2.batch("insert into types_table(varchar_field) values ('abc')");
		String dbResult = h2.getSQLPlus().query(s -> s.createQuery("select varchar_field as \"string\" from types_table").getUniqueResultAs(TypesBag.class).string);
		assertEquals("abc", dbResult);
	}
	
	@Test
	public void testReadNullString() throws Exception {
		h2.batch("insert into types_table(int_field) values (1)");
		String dbResult = h2.getSQLPlus().query(s -> s.createQuery("select varchar_field as \"string\" from types_table").getUniqueResultAs(TypesBag.class).string);
		assertNull(dbResult);
	}
	
	@Test
	public void testReadPresentBigInteger() throws Exception {
		h2.batch("insert into types_table(int_field) values (1)");
		BigInteger dbResult = h2.getSQLPlus().query(s -> s.createQuery("select int_field as \"hugeInt\" from types_table").getUniqueResultAs(TypesBag.class).hugeInt);
		assertEquals(new BigInteger("1"), dbResult);
	}
	
	@Test
	public void testReadNullBigInteger() throws Exception {
		h2.batch("insert into types_table(float_field) values (1.0)");
		BigInteger dbResult = h2.getSQLPlus().query(s -> s.createQuery("select int_field as \"hugeInt\" from types_table").getUniqueResultAs(TypesBag.class).hugeInt);
		assertNull(dbResult);
	}
	
	@Test
	public void testReadPresentBigDecimal() throws Exception {
		h2.batch("insert into types_table(decimal_field) values (1.5)");
		BigDecimal dbResult = h2.getSQLPlus().query(s -> s.createQuery("select decimal_field as \"hugeDouble\" from types_table").getUniqueResultAs(TypesBag.class).hugeDouble);
		assertEquals(new BigDecimal("1.50"), dbResult);
	}
	
	@Test
	public void testReadNullBigDecimal() throws Exception {
		h2.batch("insert into types_table(float_field) values (1.0)");
		BigDecimal dbResult = h2.getSQLPlus().query(s -> s.createQuery("select decimal_field as \"hugeDouble\" from types_table").getUniqueResultAs(TypesBag.class).hugeDouble);
		assertNull(dbResult);
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void testTimestampDbFieldCanBeMappedToStandardDateTypes() throws Exception {
		
		h2.batch("insert into types_table " +
		         "(timestamp_field, date_field, datetime_field, time_field) values " +
		         "('2016-01-05 12:30:05', '2016-01-03', '2016-10-12 08:25:30', '10:30:45')");
		
		h2.getSQLPlus().transact(session -> {
			
			TypesBag bag = session.createQuery("select timestamp_field as \"javaUtilDate\" from types_table").getUniqueResultAs(TypesBag.class);
			assertEquals(116, bag.javaUtilDate.getYear());
			assertEquals(0, bag.javaUtilDate.getMonth());
			assertEquals(5, bag.javaUtilDate.getDate());
			assertEquals(12, bag.javaUtilDate.getHours());
			assertEquals(30, bag.javaUtilDate.getMinutes());
			assertEquals(5, bag.javaUtilDate.getSeconds());
			
			bag = session.createQuery("select timestamp_field as \"timestamp\" from types_table").getUniqueResultAs(TypesBag.class);
			assertEquals(Timestamp.valueOf("2016-01-05 12:30:05"), bag.timestamp);
			
			bag = session.createQuery("select timestamp_field as \"localDate\" from types_table").getUniqueResultAs(TypesBag.class);
			assertEquals(LocalDate.of(2016, 1, 5), bag.localDate);
			
			bag = session.createQuery("select timestamp_field as \"localDateTime\" from types_table").getUniqueResultAs(TypesBag.class);
			assertEquals(LocalDateTime.of(2016, 1, 5, 12, 30, 5), bag.localDateTime);
			
			bag = session.createQuery("select timestamp_field as \"localTime\" from types_table").getUniqueResultAs(TypesBag.class);
			assertEquals(LocalTime.of(12, 30, 5), bag.localTime);
		});
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void testDateDbFieldCanBeMappedToStandardDateTypes() throws Exception {
		
		h2.batch("insert into types_table " +
		         "(timestamp_field, date_field, datetime_field, time_field) values " +
		         "('2016-01-05 12:30:05', '2016-01-03', '2016-10-12 08:25:30', '10:30:45')");
		
		h2.getSQLPlus().transact(session -> {
			
			TypesBag bag = session.createQuery("select date_field as \"javaUtilDate\" from types_table").getUniqueResultAs(TypesBag.class);
			assertEquals(116, bag.javaUtilDate.getYear());
			assertEquals(0, bag.javaUtilDate.getMonth());
			assertEquals(3, bag.javaUtilDate.getDate());
			
			bag = session.createQuery("select date_field as \"timestamp\" from types_table").getUniqueResultAs(TypesBag.class);
			assertEquals(Timestamp.valueOf("2016-01-03 00:00:00"), bag.timestamp);
			
			bag = session.createQuery("select date_field as \"localDate\" from types_table").getUniqueResultAs(TypesBag.class);
			assertEquals(LocalDate.of(2016, 1, 3), bag.localDate);
			
			bag = session.createQuery("select date_field as \"localDateTime\" from types_table").getUniqueResultAs(TypesBag.class);
			assertEquals(LocalDateTime.of(2016, 1, 3, 0, 0, 0), bag.localDateTime);
			
			assertThrows(
				() -> session.createQuery("select date_field as \"localTime\" from types_table").getUniqueResultAs(TypesBag.class),
				UnsupportedOperationException.class,
				"Cannot convert date field to java.time.LocalTime field; date fields do not contain time components"
			);
		});
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void testDateTimeDbFieldCanBeMappedToStandardDateTypes() throws Exception {
		
		h2.batch("insert into types_table " +
		         "(timestamp_field, date_field, datetime_field, time_field) values " +
		         "('2016-01-05 12:30:05', '2016-01-03', '2016-10-12 08:25:30', '10:30:45')");
		
		h2.getSQLPlus().transact(session -> {
			
			TypesBag bag = session.createQuery("select datetime_field as \"javaUtilDate\" from types_table").getUniqueResultAs(TypesBag.class);
			assertEquals(116, bag.javaUtilDate.getYear());
			assertEquals(9, bag.javaUtilDate.getMonth());
			assertEquals(12, bag.javaUtilDate.getDate());
			assertEquals(8, bag.javaUtilDate.getHours());
			assertEquals(25, bag.javaUtilDate.getMinutes());
			assertEquals(30, bag.javaUtilDate.getSeconds());
			
			bag = session.createQuery("select datetime_field as \"timestamp\" from types_table").getUniqueResultAs(TypesBag.class);
			assertEquals(Timestamp.valueOf("2016-10-12 08:25:30"), bag.timestamp);
			
			bag = session.createQuery("select datetime_field as \"localDate\" from types_table").getUniqueResultAs(TypesBag.class);
			assertEquals(LocalDate.of(2016, 10, 12), bag.localDate);
			
			bag = session.createQuery("select datetime_field as \"localDateTime\" from types_table").getUniqueResultAs(TypesBag.class);
			assertEquals(LocalDateTime.of(2016, 10, 12, 8, 25, 30), bag.localDateTime);
			
			bag = session.createQuery("select datetime_field as \"localTime\" from types_table").getUniqueResultAs(TypesBag.class);
			assertEquals(LocalTime.of(8, 25, 30), bag.localTime);
		});
	}
	
	@Test
	public void testTimeDbFieldCanBeMappedToStandardDateTypes() throws Exception {
		
		h2.batch("insert into types_table " +
		         "(timestamp_field, date_field, datetime_field, time_field) values " +
		         "('2016-01-05 12:30:05', '2016-01-03', '2016-10-12 08:25:30', '10:30:45')");
		
		h2.getSQLPlus().transact(session -> {
			
			assertThrows(
				() -> session.createQuery("select time_field as \"javaUtilDate\" from types_table").getUniqueResultAs(TypesBag.class),
				UnsupportedOperationException.class,
				"Cannot convert time field to java.util.Date"
			);
			
			assertThrows(
				() -> session.createQuery("select time_field as \"timestamp\" from types_table").getUniqueResultAs(TypesBag.class),
				UnsupportedOperationException.class,
				"Cannot convert time field to java.sql.Timestamp"
			);
			
		});
	}
	
}
