package com.tyler.sqlplus.conversion;

import com.tyler.sqlplus.base.DatabaseTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;

import static com.tyler.sqlplus.base.SQLPlusTesting.assertThrows;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class ConversionTest extends DatabaseTest {

	enum Size { SMALL, MEDIUM, LARGE };
	
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
		Size enumField;
	}
	
	@Test
	public void littleIntIsReadWhenPresent() throws Exception {
		testRead("int_field", "10", "int_field", "tinyInt", 10);
	}
	
	@Test
	public void littleIntIsReadWhenNull() throws Exception {
		testRead("decimal_field", "10.5", "int_field", "tinyInt", 0);
	}
	
	@Test
	public void bigIntIsReadWhenPresent() throws Exception {
		testRead("int_field", "10", "int_field", "bigInt", new Integer(10));
	}
	
	@Test
	public void bigIntIsReadWhenNull() throws Exception {
		testRead("decimal_field", "10.5", "int_field", "bigInt", null);
	}
	
	@Test
	public void littleShortIsReadWhenPresent() throws Exception {
		testRead("int_field", "10", "int_field", "tinyShort", (short) 10);
	}
	
	@Test
	public void littleShortIsReadWhenNull() throws Exception {
		testRead("decimal_field", "10.5", "int_field", "tinyShort", (short) 0);
	}

	@Test
	public void bigShortIsReadWhenPresent() throws Exception {
		testRead("int_field", "10", "int_field", "bigShort", new Short((short) 10));
	}
	
	@Test
	public void bigShortIsReadWhenNull() throws Exception {
		testRead("decimal_field", "10.5", "int_field", "bigShort", null);
	}

	@Test
	public void littleLongIsReadWhenPresent() throws Exception {
		testRead("int_field", "10", "int_field", "tinyLong", (long) 10);
	}

	@Test
	public void littleLongIsReadWhenNull() throws Exception {
		testRead("decimal_field", "10.5", "int_field", "tinyLong", (long) 0);
	}

	@Test
	public void bigLongIsReadWhenPresent() throws Exception {
		testRead("int_field", "10", "int_field", "bigLong", new Long(10));
	}

	@Test
	public void bigLongIsReadWhenNull() throws Exception {
		testRead("decimal_field", "10.5", "int_field", "bigLong", null);
	}
	
	@Test
	public void tinyFloatIsReadWhenPresent() throws Exception {
		testRead("float_field", "1.5", "float_field", "tinyFloat", 1.5f);
	}
	
	@Test
	public void tinyFloatIsReadWhenNull() throws Exception {
		testRead("int_field", "5", "float_field", "tinyFloat", 0f);
	}

	@Test
	public void bigFloatIsReadWhenPresent() throws Exception {
		testRead("float_field", "1.5", "float_field", "bigFloat", new Float(1.5f));
	}

	@Test
	public void bigFloatIsReadWhenNull() throws Exception {
		testRead("int_field", "5", "float_field", "bigFloat", null);
	}
	
	@Test
	public void tinyDoubleIsReadWhenPresent() throws Exception {
		testRead("decimal_field", "1.5", "decimal_field", "tinyDouble", 1.5d);
	}

	@Test
	public void tinyDoubleIsReadWhenNull() throws Exception {
		testRead("int_field", "5", "decimal_field", "tinyDouble", 0d);
	}

	@Test
	public void bigDoubleIsReadWhenPresent() throws Exception {
		testRead("decimal_field", "1.5", "decimal_field", "bigDouble", new Double(1.5d));
	}

	@Test
	public void bigDoubleIsReadWhenNull() throws Exception {
		testRead("int_field", "5", "decimal_field", "bigDouble", null);
	}
	
	@Test
	public void tinyBooleanIsReadWhenPresent() throws Exception {
		testRead("tiny_int_field", "1", "tiny_int_field", "tinyBoolean", true);
	}
	
	@Test
	public void tinyBooleanIsReadAsFalseWhenNull() throws Exception {
		testRead("int_field", "5", "tiny_int_field", "tinyBoolean", false);
	}

	@Test
	public void bigBooleanIsReadWhenPresent() throws Exception {
		testRead("tiny_int_field", "1", "tiny_int_field", "bigBoolean", new Boolean(true));
	}

	@Test
	public void bigBooleanIsReadAsNull() throws Exception {
		testRead("int_field", "5", "tiny_int_field", "bigBoolean", null);
	}
	
	@Test
	public void tinyCharIsReadWhenPresent() throws Exception {
		testRead("char_field", "'a'", "char_field", "tinyChar", 'a');
	}
	
	@Test
	public void tinyCharIsReadAsMinValueWhenNull() throws Exception {
		testRead("int_field", "5", "char_field", "tinyChar", Character.MIN_VALUE);
	}

	@Test
	public void bigCharIsReadWhenPresent() throws Exception {
		testRead("char_field", "'a'", "char_field", "bigChar", new Character('a'));
	}

	@Test
	public void bigCharIsReadAsNull() throws Exception {
		testRead("int_field", "5", "char_field", "bigChar", null);
	}
	
	@Test
	public void stringIsReadWhenPresent() throws Exception {
		testRead("varchar_field", "'abc'", "varchar_field", "string", "abc");
	}
	
	@Test
	public void stringIsReadWhenNull() throws Exception {
		testRead("int_field", "1", "varchar_field", "string", null);
	}
	
	@Test
	public void bigIntegerIsReadWhenPresent() throws Exception {
		testRead("int_field", "1", "int_field", "hugeInt", new BigInteger("1"));
	}

	@Test
	public void bigIntegerIsReadWhenNull() throws Exception {
		testRead("float_field", "1.0", "int_field", "hugeInt", null);
	}

	@Test
	public void bigDecimalIsReadWhenPresent() throws Exception {
		testRead("decimal_field", "1.50", "decimal_field", "hugeDouble", new BigDecimal("1.50"));
	}

	@Test
	public void bigDecimalIsReadWhenNull() throws Exception {
		testRead("float_field", "1.5", "decimal_field", "hugeDouble", null);
	}

	@Test
	public void enumIsReadWhenPresent() throws Exception {
		testRead("enum_field", "'MEDIUM'", "enum_field", "enumField", Size.MEDIUM);
	}

	@Test
	public void enumIsReadWhenNull() throws Exception {
		testRead("int_field", "1", "enum_field", "enumField", null);
	}

	@SuppressWarnings("deprecation")
	@Test
	public void testTimestampDbFieldCanBeMappedToStandardDateTypes() throws Exception {
		
		db.batch("insert into types_table " +
		         "(timestamp_field, date_field, datetime_field, time_field) values " +
		         "('2016-01-05 12:30:05', '2016-01-03', '2016-10-12 08:25:30', '10:30:45')");
		
		db.getSQLPlus().transact(session -> {
			
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
		
		db.batch("insert into types_table " +
		         "(timestamp_field, date_field, datetime_field, time_field) values " +
		         "('2016-01-05 12:30:05', '2016-01-03', '2016-10-12 08:25:30', '10:30:45')");
		
		db.getSQLPlus().transact(session -> {
			
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
		
		db.batch("insert into types_table " +
		         "(timestamp_field, date_field, datetime_field, time_field) values " +
		         "('2016-01-05 12:30:05', '2016-01-03', '2016-10-12 08:25:30', '10:30:45')");
		
		db.getSQLPlus().transact(session -> {
			
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
		
		db.batch("insert into types_table " +
		         "(timestamp_field, date_field, datetime_field, time_field) values " +
		         "('2016-01-05 12:30:05', '2016-01-03', '2016-10-12 08:25:30', '10:30:45')");
		
		db.getSQLPlus().transact(session -> {
			
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

	/**
	 * Tests a value is read as the expected object after inserting a field value
	 */
	private void testRead(String insertCol, String insertVal, String readCol, String readAlias, Object expect) throws Exception {

		String insertSql = String.format("insert into types_table(%s) values (%s)", insertCol, insertVal);
		String readSql = String.format("select %s as \"%s\" from types_table", readCol, readAlias);

		db.batch(insertSql);
		TypesBag queryResult = db.getSQLPlus().query(s -> s.createQuery(readSql).getUniqueResultAs(TypesBag.class));
		Field resultField = TypesBag.class.getDeclaredField(readAlias);
		Object actualResult = resultField.get(queryResult);

		assertEquals(expect, actualResult);
	}

}
