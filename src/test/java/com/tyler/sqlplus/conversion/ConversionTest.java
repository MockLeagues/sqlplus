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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

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
	public void littleIntIsWritten() throws Exception {
		testWrite("int_field", 1, "1");
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
	public void bigIntIsWrittenWhenPresent() throws Exception {
		testWrite("int_field", new Integer(1), "1");
	}

	@Test
	public void nullCanBeWrittenToIntField() throws Exception {
		testWrite("int_field", null, null);
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
	public void littleShortIsWritten() throws Exception {
		testWrite("int_field", (short) 1, "1");
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
	public void bigShortIsWrittenWhenPresent() throws Exception {
		testWrite("int_field", new Short((short) 1), "1");
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
	public void littleLongIsWritten() throws Exception {
		testWrite("int_field", 1L, "1");
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
	public void bigLongIsWrittenWhenPresent() throws Exception {
		testWrite("int_field", new Long(1L), "1");
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
	public void littleFloatIsWritten() throws Exception {
		testWrite("float_field", 1.0f, "1.0");
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
	public void bigFloatIsWrittenWhenPresent() throws Exception {
		testWrite("float_field", new Float(1.0f), "1.0");
	}

	@Test
	public void nullCanBeWrittenToFloatField() throws Exception {
		testWrite("float_field", null, null);
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
	public void tinyDoubleIsWritten() throws Exception {
		testWrite("decimal_field", 1.0d, "1.00");
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
	public void bigDoubleIsWritten() throws Exception {
		testWrite("decimal_field", new Double(1.0d), "1.00");
	}

	@Test
	public void nullCanBeWrittenToDecimalField() throws Exception {
		testWrite("decimal_field", null, null);
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
	public void tinyBooleanIsWritten() throws Exception {
		testWrite("tiny_int_field", true, "1");
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
	public void bigBooleanIsWritten() throws Exception {
		testWrite("tiny_int_field", Boolean.TRUE, "1");
	}

	@Test
	public void nullCanBeWrittenToTinyIntField() throws Exception {
		testWrite("tiny_int_field", null, null);
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
	public void tinyCharIsWritten() throws Exception {
		testWrite("char_field", 'a', "a");
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
	public void bigCharIsWritten() throws Exception {
		testWrite("char_field", new Character('a'), "a");
	}

	@Test
	public void nullCanBeWrittenToCharField() throws Exception {
		testWrite("char_field", null, null);
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
	public void stringIsWritten() throws Exception {
		testWrite("varchar_field", "'abc'", "'abc'");
	}

	@Test
	public void nullCanBeWrittenToStringField() throws Exception {
		testWrite("varchar_field", null, null);
	}

	@Test
	public void bigIntegerIsReadWhenPresent() throws Exception {
		testRead("int_field", "1", "int_field", "hugeInt", new BigInteger("1"));
	}

	@Test
	public void bigIntegerIsWritten() throws Exception {
		testWrite("int_field", new BigInteger("1"), "1");
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
	public void bigDecimalIsWritten() throws Exception {
		testWrite("decimal_field", new BigDecimal("1.50"), "1.50");
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

	@Test
	public void localDateFieldIsReadWhenPresent() throws Exception {
		testRead("date_field", "'2005-01-05'", "date_field", "localDate", LocalDate.of(2005, 1, 5));
	}

	@Test
	public void localDateFieldIsReadWhenNull() throws Exception {
		testRead("int_field", "1", "date_field", "localDate", null);
	}

	@Test
	public void localDateFieldIsWritten() throws Exception {
		testWrite("date_field", LocalDate.of(2005, 1, 1), "2005-01-01");
	}

	@Test
	public void localTimeFieldIsReadWhenPresent() throws Exception {
		testRead("time_field", "'05:30:00'", "time_field", "localTime", LocalTime.of(5, 30));
	}

	@Test
	public void localTimeFieldIsReadWhenNull() throws Exception {
		testRead("int_field", "1", "time_field", "localTime", null);
	}

	@Test
	public void localTimeFieldIsWrittenWhenOnlyHoursArePresent() throws Exception {
		testWrite("time_field", LocalTime.of(5, 0), "05:00:00");
	}

	@Test
	public void localTimeFieldIsWrittenWhenHoursAndMinutesArePresent() throws Exception {
		testWrite("time_field", LocalTime.of(5, 10), "05:10:00");
	}

	@Test
	public void localTimeFieldIsWrittenWhenHoursAndMinutesAndSecondsArePresent() throws Exception {
		testWrite("time_field", LocalTime.of(5, 10, 15), "05:10:15");
	}

	@Test
	public void localDateTimeIsReadWhenPresent() throws Exception {
		testRead("datetime_field", "'2005-01-05 05:30:15'", "datetime_field", "localDateTime", LocalDateTime.of(2005, 1, 5, 5, 30, 15));
	}

	@Test
	public void localDateTimeIsReadWhenNull() throws Exception {
		testRead("int_field", "1", "datetime_field", "localDateTime", null);
	}

	private void testRead(String insertCol, String insertVal, String readCol, String readAlias, Object expect) throws Exception {

		String insertSql = String.format("insert into types_table(%s) values (%s)", insertCol, insertVal);
		String readSql = String.format("select %s as \"%s\" from types_table", readCol, readAlias);

		db.batch(insertSql);
		TypesBag queryResult = db.getSQLPlus().query(s -> s.createQuery(readSql).getUniqueResultAs(TypesBag.class));
		Field resultField = TypesBag.class.getDeclaredField(readAlias);
		Object actualResult = resultField.get(queryResult);

		assertEquals(expect, actualResult);
	}

	private void testWrite(String fieldName, Object writeValue, String expectResult) {

		String insertSQL = String.format("insert into types_table(%s) values(?)", fieldName);
		String querySQL = String.format("select %s from types_table", fieldName);

		db.getSQLPlus().transact(s -> s.createQuery(insertSQL, writeValue).executeUpdate());

		String[][] actual = db.query(querySQL);
		String[][] expect = {{ expectResult }};
		assertArrayEquals(actual, expect);
	}

}
