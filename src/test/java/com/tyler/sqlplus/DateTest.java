package com.tyler.sqlplus;

import static com.tyler.sqlplus.test.SqlPlusTesting.assertThrows;
import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.tyler.sqlplus.rule.AbstractDBRule;
import com.tyler.sqlplus.rule.H2EmployeeDBRule;

/**
 * I wanted to have a separate test class purely for testing deserialization of date types since it gets hairy
 */
public class DateTest {

	@ClassRule
	public static final AbstractDBRule H2 = new H2EmployeeDBRule();
	
	@BeforeClass
	public static void insertDateRecord() {
		try {
			H2.batch("insert into dates_dates_dates " +
			         "(timestamp_field, date_field, datetime_field, time_field) values " +
			         "('2016-01-05 12:30:05', '2016-01-03', '2016-10-12 08:25:30', '10:30:45')");
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	static class DateBag {
		Date javaUtilDate;
		Timestamp timestamp;
		LocalDate localDate;
		LocalDateTime localDateTime;
		LocalTime localTime;
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void testTimestampDbFieldCanBeMappedToStandardDateTypes() throws Exception {
		
		H2.getSQLPlus().open(session -> {
			
			DateBag bag = session.createQuery("select timestamp_field as \"javaUtilDate\" from dates_dates_dates").getUniqueResultAs(DateBag.class);
			assertEquals(116, bag.javaUtilDate.getYear());
			assertEquals(0, bag.javaUtilDate.getMonth());
			assertEquals(5, bag.javaUtilDate.getDate());
			assertEquals(12, bag.javaUtilDate.getHours());
			assertEquals(30, bag.javaUtilDate.getMinutes());
			assertEquals(5, bag.javaUtilDate.getSeconds());
			
			bag = session.createQuery("select timestamp_field as \"timestamp\" from dates_dates_dates").getUniqueResultAs(DateBag.class);
			assertEquals(Timestamp.valueOf("2016-01-05 12:30:05"), bag.timestamp);
			
			bag = session.createQuery("select timestamp_field as \"localDate\" from dates_dates_dates").getUniqueResultAs(DateBag.class);
			assertEquals(LocalDate.of(2016, 1, 5), bag.localDate);
			
			bag = session.createQuery("select timestamp_field as \"localDateTime\" from dates_dates_dates").getUniqueResultAs(DateBag.class);
			assertEquals(LocalDateTime.of(2016, 1, 5, 12, 30, 5), bag.localDateTime);
			
			bag = session.createQuery("select timestamp_field as \"localTime\" from dates_dates_dates").getUniqueResultAs(DateBag.class);
			assertEquals(LocalTime.of(12, 30, 5), bag.localTime);
		});
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void testDateDbFieldCanBeMappedToStandardDateTypes() throws Exception {
		
		H2.getSQLPlus().open(session -> {
			
			DateBag bag = session.createQuery("select date_field as \"javaUtilDate\" from dates_dates_dates").getUniqueResultAs(DateBag.class);
			assertEquals(116, bag.javaUtilDate.getYear());
			assertEquals(0, bag.javaUtilDate.getMonth());
			assertEquals(3, bag.javaUtilDate.getDate());
			
			bag = session.createQuery("select date_field as \"timestamp\" from dates_dates_dates").getUniqueResultAs(DateBag.class);
			assertEquals(Timestamp.valueOf("2016-01-03 00:00:00"), bag.timestamp);
			
			bag = session.createQuery("select date_field as \"localDate\" from dates_dates_dates").getUniqueResultAs(DateBag.class);
			assertEquals(LocalDate.of(2016, 1, 3), bag.localDate);
			
			bag = session.createQuery("select date_field as \"localDateTime\" from dates_dates_dates").getUniqueResultAs(DateBag.class);
			assertEquals(LocalDateTime.of(2016, 1, 3, 0, 0, 0), bag.localDateTime);
			
			assertThrows(
				() -> session.createQuery("select date_field as \"localTime\" from dates_dates_dates").getUniqueResultAs(DateBag.class),
				UnsupportedOperationException.class,
				"Cannot convert date field to java.time.LocalTime field; date fields do not contain time components"
			);
		});
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void testDateTimeDbFieldCanBeMappedToStandardDateTypes() throws Exception {
		
		H2.getSQLPlus().open(session -> {
			
			DateBag bag = session.createQuery("select datetime_field as \"javaUtilDate\" from dates_dates_dates").getUniqueResultAs(DateBag.class);
			assertEquals(116, bag.javaUtilDate.getYear());
			assertEquals(9, bag.javaUtilDate.getMonth());
			assertEquals(12, bag.javaUtilDate.getDate());
			assertEquals(8, bag.javaUtilDate.getHours());
			assertEquals(25, bag.javaUtilDate.getMinutes());
			assertEquals(30, bag.javaUtilDate.getSeconds());
			
			bag = session.createQuery("select datetime_field as \"timestamp\" from dates_dates_dates").getUniqueResultAs(DateBag.class);
			assertEquals(Timestamp.valueOf("2016-10-12 08:25:30"), bag.timestamp);
			
			bag = session.createQuery("select datetime_field as \"localDate\" from dates_dates_dates").getUniqueResultAs(DateBag.class);
			assertEquals(LocalDate.of(2016, 10, 12), bag.localDate);
			
			bag = session.createQuery("select datetime_field as \"localDateTime\" from dates_dates_dates").getUniqueResultAs(DateBag.class);
			assertEquals(LocalDateTime.of(2016, 10, 12, 8, 25, 30), bag.localDateTime);
			
			bag = session.createQuery("select datetime_field as \"localTime\" from dates_dates_dates").getUniqueResultAs(DateBag.class);
			assertEquals(LocalTime.of(8, 25, 30), bag.localTime);
		});
	}
	
	@Test
	public void testTimeDbFieldCanBeMappedToStandardDateTypes() throws Exception {
		
		H2.getSQLPlus().open(session -> {
			
			assertThrows(
				() -> session.createQuery("select time_field as \"javaUtilDate\" from dates_dates_dates").getUniqueResultAs(DateBag.class),
				UnsupportedOperationException.class,
				"Cannot convert time field to java.util.Date"
			);
			
			assertThrows(
				() -> session.createQuery("select time_field as \"timestamp\" from dates_dates_dates").getUniqueResultAs(DateBag.class),
				UnsupportedOperationException.class,
				"Cannot convert time field to java.sql.Timestamp"
			);
			
		});
	}
	
}
