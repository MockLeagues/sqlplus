package com.tyler.sqlplus;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.junit.Rule;
import org.junit.Test;

import com.tyler.sqlplus.QueryTest.Employee.Type;
import com.tyler.sqlplus.annotation.LoadQuery;
import com.tyler.sqlplus.exception.POJOBindException;
import com.tyler.sqlplus.exception.QuerySyntaxException;
import com.tyler.sqlplus.exception.SessionClosedException;
import com.tyler.sqlplus.exception.SqlRuntimeException;
import com.tyler.sqlplus.functional.Task;
import com.tyler.sqlplus.rule.H2EmployeeDBRule;

public class QueryTest {

	@Rule
	public H2EmployeeDBRule h2 = new H2EmployeeDBRule();
	
	protected static void assertThrows(Task t, Class<? extends Throwable> expectType) {
		assertThrows(t, expectType, null);
	}
	
	protected static void assertThrows(Task t, Class<? extends Throwable> expectType, String expectMsg) {
		try {
			t.run();
			fail("Expected test to throw instance of " + expectType.getName() + " but no error was thrown");
		}
		catch (Throwable thrownError) {
			if (!expectType.equals(thrownError.getClass())) {
				fail("Expected test to throw instance of " + expectType.getName() + " but no instead got error of type " + thrownError.getClass().getName());
			}
			if (expectMsg != null) {
				if (!Objects.equals(thrownError.getMessage(), expectMsg)) {
					fail("Expected error with message " + expectMsg + ", instead got message " + thrownError.getMessage());
				}
			}
		}
	}
	
	public static class Employee {
		
		public enum Type { HOURLY, SALARY; }
		public Integer employeeId;
		public Type type;
		public String name;
		public LocalDate hired;
		public Integer salary;

		@LoadQuery(
			"select office_id as \"office_id\", office_name as \"office_name\", employee_id as \"employee_id\", `primary` as \"primary\" " +
			"from office o " +
			"where o.employee_id = :employeeId"
		)
		public List<Office> offices;
		
		public List<Office> getOffices() {
			return offices;
		}
		
	}

	public static class Office {
		public Integer officeId;
		public String officeName;
		public boolean primary;
		public int employeeId;
	}
	
	public static class Address {
		
		public Integer addressId;
		public String street;
		public String city;
		public String state;
		public String zip;
		
		@LoadQuery(
			"select employee_id as \"employee_id\", type as \"type\", name as \"name\", hired as \"hired\", salary as \"salary\" " +
			"from employee e " +
			"where e.address_id = :addressId"
		)
		public Employee employee;
		
		public Address() {}
		
		public Address(String street, String city, String state, String zip) {
			this.street = street;
			this.city = city;
			this.state = state;
			this.zip = zip;
		}
		
		public Employee getEmployee() {
			return employee;
		}
		
	}
	
	@Test
	public void testErrorThrownIfUnkownParamAdded() throws SQLException {
		h2.getSQLPlus().open(conn -> {
			assertThrows(() -> {
				Query q = conn.createQuery("select * from employee where employee_id = :id");
				q.setParameter("idx", "123");
			}, QuerySyntaxException.class, "Unknown query parameter: idx");
		});
	}
	
	@Test
	public void testErrorThrownIfParamValueNotSet() throws Exception {
		h2.batch("insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')");
		h2.getSQLPlus().open(conn -> {
			assertThrows(() -> {
				conn.createQuery("select address_id from address where state = :state and city = :city").setParameter("state", "s").getUniqueResultAs(Address.class);
			}, QuerySyntaxException.class, "Missing parameter values for the following parameters: [city]");
		});
	}
	
	@Test
	public void testThrowsIfParamIndexOutOfRange() throws Exception {
		h2.getSQLPlus().open(conn -> {
			assertThrows(() -> {
				conn.createQuery("select address_id from address where city = ?").setParameter(1, "city").setParameter(2, "state").getUniqueResultAs(Address.class);
			}, QuerySyntaxException.class, "Parameter index 2 is out of range of this query's parameters (max parameters: 1)");
		});
	}
	
	@Test
	public void testThrowsIfDuplicateParamAdded() throws Exception {
		h2.getSQLPlus().open(conn -> {
			assertThrows(() -> {
				conn.createQuery("select address_id from address where city = :city and street = :city").setParameter(1, "city").setParameter(2, "state").getUniqueResultAs(Address.class);
			}, QuerySyntaxException.class, "Duplicate parameter 'city'");
		});
	}
	
	@Test
	public void testErrorThrownIfNoParamsSet() throws Exception {
		h2.getSQLPlus().open(conn -> {
			assertThrows(() -> {
				conn.createQuery("select * from employee where name = :name").fetch();
			}, QuerySyntaxException.class, "No parameters set");
		});
	}
	
	@Test
	public void testsQueryingWithParameterLabels() throws Exception {
		h2.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Elm Street', 'Othertown', 'CA', '54321')",
			"insert into address (street, city, state, zip) values('Main Street', 'Bakersfield', 'CA', '54321')"
		);
		h2.getSQLPlus().open(conn -> {
			Address result = conn.createQuery("select address_id as \"addressId\", street as \"street\", state as \"state\", city as \"city\", zip as \"zip\" from address a where state = :state and city = :city")
			                     .setParameter("state", "CA")
			                     .setParameter("city", "Othertown")
			                     .getUniqueResultAs(Address.class);
			assertEquals("Elm Street", result.street);
		});
	}
	
	@Test
	public void testQueryingWithWithMixtureOfParameterLabelsAndQuestionMarks() throws Exception {
		
		h2.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Elm Street', 'Othertown', 'CA', '54321')"
		);
		
		h2.getSQLPlus().open(conn -> {
			
			String sql =
				"select address_id as \"addressId\", street as \"street\", state as \"state\", city as \"city\", zip as \"zip\" " +
				"from address a " +
				"where a.city = ? and a.state = :state";
			
			Address addr = conn.createQuery(sql).setParameter(1, "Anytown").setParameter("state", "MN").getUniqueResultAs(Address.class);
			assertEquals("Maple Street", addr.street);
			assertEquals("Anytown", addr.city);
			assertEquals("MN", addr.state);
			assertEquals("12345", addr.zip);
		});
	}
	
	@Test
	public void testMappingSinglePOJOithCustomFieldMappings() throws Exception {
		h2.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Elm Street', 'Othertown', 'CA', '54321')"
		);
		
		List<Address> results = h2.getSQLPlus().query(conn -> {
			return conn.createQuery("select address_id as ADD_ID, street as STREET_NAME, state as STATE_ABBR, city as CITY_NAME, zip as POSTAL from address")
						.addColumnMapping("ADD_ID", "addressId")
						.addColumnMapping("STREET_NAME", "street")
						.addColumnMapping("STATE_ABBR", "state")
						.addColumnMapping("CITY_NAME", "city")
						.addColumnMapping("POSTAL", "zip")
						.fetchAs(Address.class);
		});
		
		assertEquals(2, results.size());
		
		Address first = results.get(0);
		assertEquals(new Integer(1), first.addressId);
		assertEquals("Maple Street", first.street);
		assertEquals("Anytown", first.city);
		assertEquals("MN", first.state);
		assertEquals("12345", first.zip);
		
		Address second = results.get(1);
		assertEquals(new Integer(2), second.addressId);
		assertEquals("Elm Street", second.street);
		assertEquals("Othertown", second.city);
		assertEquals("CA", second.state);
		assertEquals("54321", second.zip);
	}
	
	@Test
	public void testMappingSinglePOJOWithCustomFieldMappingsThrowsErrorIfUnknownFieldName() throws Exception {
		
		h2.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Elm Street', 'Othertown', 'CA', '54321')"
		);
		
		h2.getSQLPlus().open(conn -> {
			assertThrows(() -> {
				conn.createQuery("select address_id as ADD_ID, street as STREET_NAME, state as STATE_ABBR, city as CITY_NAME, zip as POSTAL from address")
					.addColumnMapping("ADD_ID", "addressId")
					.addColumnMapping("STREET_NAME", "streetName")
					.addColumnMapping("STATE_ABBR", "state")
					.addColumnMapping("CITY_NAME", "city")
					.addColumnMapping("POSTAL", "zip")
					.fetchAs(Address.class);
			}, POJOBindException.class, "Custom-mapped field streetName not found in class " + Address.class.getName() + " for result set column STREET_NAME");
		});
		
	}
	
	@Test
	public void testFetchingAsMaps() throws Exception {
		
		h2.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Elm Street', 'Othertown', 'CA', '54321')"
		);
		
		List<Map<String, Object>> rows = h2.getSQLPlus().query(conn -> conn.createQuery("select * from address").fetch());
		assertEquals(2, rows.size());
		
		Map<String, Object> row1 = rows.get(0);
		assertEquals("Maple Street", row1.get("STREET"));
		assertEquals("Anytown", row1.get("CITY"));
		assertEquals("MN", row1.get("STATE"));
		assertEquals("12345", row1.get("ZIP"));
		
		Map<String, Object> row2 = rows.get(1);
		assertEquals("Elm Street", row2.get("STREET"));
		assertEquals("Othertown", row2.get("CITY"));
		assertEquals("CA", row2.get("STATE"));
		assertEquals("54321", row2.get("ZIP"));
	}
	
	@Test
	public void testBatchExec() throws Exception {
		
		h2.getSQLPlus().batchExec(
			"insert into address (street, city, state, zip) values ('street1', 'city1', 'state1', 'zip1')",
			"insert into address (street, city, state, zip) values ('street2', 'city2', 'state2', 'zip2')",
			"insert into address (street, city, state, zip) values ('street3', 'city3', 'state3', 'zip3')",
			"insert into address (street, city, state, zip) values ('street4', 'city4', 'state4', 'zip4')"
		);
	
		String[][] results = h2.query("select * from address");
		String[][] expect = {
			{"1", "street1", "city1", "state1", "zip1"},
			{"2", "street2", "city2", "state2", "zip2"},
			{"3", "street3", "city3", "state3", "zip3"},
			{"4", "street4", "city4", "state4", "zip4"}
		};
		
		assertArrayEquals(expect, results);
	}

	@Test
	public void testBatchProcessWithEvenBatchDivision() throws Exception {
		
		int numAddress = 20;
		List<String> insertSqls = new ArrayList<>();
		for (int i = 1; i <= numAddress; i++) {
			insertSqls.add("insert into address (street, city, state, zip) values ('street" + i + "', 'city" + i + "', 'state" + i + "', 'zip" + i + "')");
		}
		h2.getSQLPlus().batchExec(insertSqls.stream().toArray(String[]::new));
		
		int[] numBatchesSeen = {0};
		
		h2.getSQLPlus().open(conn -> {
			Query q = conn.createQuery("select street as \"street\", city as \"city\" from address");
			q.batchProcess(Address.class, 4, batch -> {
				assertEquals(4, batch.size());
				numBatchesSeen[0]++;
			});
		});
		
		assertEquals(5, numBatchesSeen[0]);
	}
	
	@Test
	public void testBatchProcessWithUnevenBatchDivision() throws Exception {
		
		int numAddress = 22;
		List<String> insertSqls = new ArrayList<>();
		for (int i = 1; i <= numAddress; i++) {
			insertSqls.add("insert into address (street, city, state, zip) values ('street" + i + "', 'city" + i + "', 'state" + i + "', 'zip" + i + "')");
		}
		h2.getSQLPlus().batchExec(insertSqls.stream().toArray(String[]::new));
		
		int[] numBatchesSeen = {0};
		
		h2.getSQLPlus().open(conn -> {
			Query q = conn.createQuery("select street as \"street\", city as \"city\" from address");
			q.batchProcess(Address.class, 4, batch -> {
				if (numBatchesSeen[0] == 5) {
					assertEquals(2, batch.size());
				}
				else {
					assertEquals(4, batch.size());
				}
				numBatchesSeen[0]++;
			});
		});
		
		assertEquals(6, numBatchesSeen[0]);
	}
	
	@Test
	public void testFieldsNotPresentInResultSetAreLeftNullInPOJO() throws Exception {
		h2.batch("insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')");
		
		Address result = h2.getSQLPlus().query(sess -> sess.createQuery("select street as \"street\", city as \"city\" from address").getUniqueResultAs(Address.class));
		assertNull(result.state);
		assertNull(result.zip);
		assertNotNull(result.street);
		assertNotNull(result.city);
	}
	
	@Test
	public void testMapEnumTypes() throws Exception {
		h2.batch("insert into employee(type, name, salary, hired) values('HOURLY', 'Billy Bob', '42000', '2015-01-01')");
		Employee emp = h2.getSQLPlus().query(sess -> {
			return sess.createQuery("select employee_id as \"employeeId\", type as \"type\", name as \"name\", salary as \"salary\", hired as \"hired\" from employee")
			           .getUniqueResultAs(Employee.class);
		});
		assertEquals(Type.HOURLY, emp.type);
	}
	
	@Test
	public void testQueryIntScalar() throws Exception {
		h2.batch("insert into office(office_name, `primary`, employee_id) values ('Office A', 1, 1)");
		Integer total = h2.getSQLPlus().queryInt("select sum(office_id) from office");
		assertEquals(new Integer(1), total);
	}
	
	@Test
	public void testQueryDoubleScalar() throws Exception {
		h2.batch("insert into employee(type, name, salary, hired) values('HOURLY', 'Billy Bob', '42000', '2015-01-01')");
		Double salary = h2.getSQLPlus().queryDouble("select sum(salary) from employee");
		assertEquals(new Double(42000), salary);
	}
	
	@Test
	public void testBatchesAreAddedWhenExplicitlyAdded() throws Exception {
		
		h2.getSQLPlus().open(conn -> {
			
			conn.createQuery("insert into employee(type, name, hired, salary) values (:type, :name, :hired, :salary)")
			    .setParameter("type", Type.SALARY)
			    .setParameter("name", "test1")
			    .setParameter("hired", "2015-01-01")
			    .setParameter("salary", "100")
			    .finishBatch()
			    .setParameter("type", Type.SALARY)
			    .setParameter("name", "test2")
			    .setParameter("hired", "2015-01-02")
			    .setParameter("salary", "200")
			    .finishBatch()
			    .executeUpdate();
			
			String[][] expect = {
				{"1", "SALARY", "test1", "2015-01-01", "100", null},
				{"2", "SALARY", "test2", "2015-01-02", "200", null}
			};
			
			String[][] actual = h2.query("select employee_id, type, name, hired, salary, address_id from employee");
			assertArrayEquals(expect, actual);
		});
		
	}
	
	@Test
	public void testTheLastManualBatchIsAutoAddedIfNotExplicitlyAdded() throws Exception {
		
		h2.getSQLPlus().open(conn -> {
			
			conn.createQuery("insert into employee(type, name, hired, salary) values (:type, :name, :hired, :salary)")
			    .setParameter("type", Type.SALARY)
			    .setParameter("name", "test1")
			    .setParameter("hired", "2015-01-01")
			    .setParameter("salary", "100")
			    .finishBatch()
			    .setParameter("type", Type.SALARY)
			    .setParameter("name", "test2")
			    .setParameter("hired", "2015-01-02")
			    .setParameter("salary", "200")
			    // Don't manually add last batch, query should auto-add it
			    .executeUpdate();
			
			String[][] expect = {
				{"1", "SALARY", "test1", "2015-01-01", "100", null},
				{"2", "SALARY", "test2", "2015-01-02", "200", null}
			};
			
			String[][] actual = h2.query("select employee_id, type, name, hired, salary, address_id from employee");
			assertArrayEquals(expect, actual);
		});
	}
	
	@Test
	public void testFinishingABatchWithMissingParametersThrowsError() throws Exception {
		h2.getSQLPlus().open(conn -> {
			Query q = conn.createQuery("insert into employee(type, name, hired, salary) values (:type, :name, :hired, :salary)")
			    .setParameter("type", Type.SALARY)
			    .setParameter("name", "test1")
			    .setParameter("hired", "2015-01-01");
			
			assertThrows(q::finishBatch, QuerySyntaxException.class, "Missing parameter values for the following parameters: [salary]");
		});
	}
	
	@Test
	public void testBindingParamsFromObject() throws Exception {
		
		Employee toCreate = new Employee();
		LocalDate hiredAt = LocalDate.now();
		toCreate.hired = hiredAt;
		toCreate.name = "tester-pojo";
		toCreate.salary = 20000;
		toCreate.type = Type.HOURLY;
		h2.getSQLPlus().open(conn -> {
			
			conn.createQuery("insert into employee(type, name, hired, salary) values (:type, :name, :hired, :salary)")
			    .bind(toCreate)
			    .executeUpdate();
			
			String[] actualRow = h2.query("select type, name, hired, salary from employee")[0];
			String[] expectRow = { Type.HOURLY.name(), "tester-pojo",  hiredAt.toString(), "20000" };
			assertArrayEquals(expectRow, actualRow);
		});
	}
	
	@Test
	public void testBindingObjectParamsWhenSomeFieldIsNullSetsNull() throws Exception {
		
		Employee toCreate = new Employee();
		toCreate.type = Type.HOURLY;
		toCreate.name = "tester-pojo";
		
		h2.getSQLPlus().open(conn -> {
			
			conn.createQuery("insert into employee(type, name, hired, salary) values (:type, :name, :hired, :salary)")
			    .bind(toCreate)
			    .executeUpdate();
			
			String[] actualRow = h2.query("select type, name, hired, salary from employee")[0];
			String[] expectRow = { Type.HOURLY.name(), "tester-pojo",  null, null };
			assertArrayEquals(expectRow, actualRow);
			assertArrayEquals(expectRow, actualRow);
		});
		
	}
	
	public static class EmployeeMissingBindParam {
		public com.tyler.sqlplus.QueryTest.Employee.Type type;
		public String name;
		public Integer salary;
	}
	
	@Test
	public void testBindParamsFailsIfNoMemberForParam() throws Exception {
		
		EmployeeMissingBindParam toCreate = new EmployeeMissingBindParam();
		toCreate.name = "tester-pojo";
		toCreate.salary = 20000;
		toCreate.type = Type.HOURLY;
		
		h2.getSQLPlus().open(conn -> {
			assertThrows(() -> {
				conn.createQuery("insert into employee(hired, type, name, salary) values (:hired, :type, :name, :salary)").bind(toCreate);
			}, POJOBindException.class);
		});
	}
	
	@Test
	public void returnGeneratedKeys() throws Exception {
		h2.batch("insert into employee(type, name, hired, salary) values ('SALARY', 'tester-1', '2015-01-01', 20500)");
		
		h2.getSQLPlus().open(conn -> {
			List<Integer> keys = conn.createQuery("insert into employee(type, name, hired, salary) values (:type, :name, :hired, :salary)")
			                                   .setParameter("type", "HOURLY")
			                                   .setParameter("name", "tester-2")
			                                   .setParameter("hired", "2015-01-01")
			                                   .setParameter("salary", "10000")
			                                   .executeUpdate(Integer.class);
			assertEquals(new Integer(2), keys.get(0));
		});
	}

	@Test
	public void testNoStatementsAreCommittedIfExceptionThrownInTransact() throws Exception {
		
		class TransactionException extends RuntimeException {}
		
		try {
			h2.getSQLPlus().transact(conn -> {
				conn.createQuery("insert into employee(type, name, hired, salary) values ('SALARY', 'tester-1', '2015-01-01', 20500)").executeUpdate();
				conn.createQuery("insert into employee(type, name, hired, salary) values ('SALARY', 'tester-2', '2015-01-01', 20500)").executeUpdate();
				throw new TransactionException();
			});
			
		} catch (SqlRuntimeException e) {
			assertEquals(TransactionException.class, e.getCause().getClass()); // Make sure the error we got was from us throwing the transaction exception
			assertArrayEquals(new String[][]{}, h2.query("select * from employee"));
		}
	}
	
	@Test
	public void testLazyLoadSingleRelation() throws Exception {
		
		h2.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into employee(type, name, hired, salary, address_id) values ('SALARY', 'tester-1', '2015-01-01', 20500, 1)"
		);
		
		h2.getSQLPlus().open(conn -> {
			
			Address singleAddress = 
				conn.createQuery("select address_id as \"addressId\", street as \"street\", state as \"state\", city as \"city\", zip as \"zip\" from address a")
			        .getUniqueResultAs(Address.class);
			
			// Make sure it stays null until getter is called
			assertNull(singleAddress.employee);
			Employee lazyEmployee = singleAddress.getEmployee();
			assertNotNull(lazyEmployee);
			
			assertEquals(Type.SALARY, lazyEmployee.type);
			assertEquals("tester-1", lazyEmployee.name);
			assertEquals(LocalDate.of(2015, 1, 1), lazyEmployee.hired);
			assertEquals(new Integer(20500), lazyEmployee.salary);
		});
		
	}
	
	@Test
	public void testLazyLoadMultipleRelations() throws Exception {
		
		h2.batch(
			"insert into employee(type, name, salary, hired) values('HOURLY', 'Billy Bob', '42000', '2015-01-01')",
			"insert into office(office_name, `primary`, employee_id) values ('Office A', 0, 1)",
			"insert into office(office_name, `primary`, employee_id) values ('Office B', 1, 1)",
			"insert into office(office_name, `primary`, employee_id) values ('Office C', 0, 1)"
		);
		
		h2.getSQLPlus().open(conn -> {
			
			Employee employee = 
				conn.createQuery("select employee_id as \"employeeId\", type as \"type\", name as \"name\", hired as \"hired\", salary as \"salary\" from employee e ")
			        .getUniqueResultAs(Employee.class);
			
			// Make sure it stays null until getter is called
			assertNull(employee.offices);
			List<Office> offices = employee.getOffices();
			assertNotNull(offices);
			
			assertEquals(3, offices.size());
		});
		
	}
	
	@Test
	public void testLazyLoadFailsOutsideSession() throws Exception {
		
		h2.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into employee(type, name, hired, salary, address_id) values ('SALARY', 'tester-1', '2015-01-01', 20500, 1)"
		);
		
		Address foundAddress = h2.getSQLPlus().query(conn -> {
			return conn.createQuery("select address_id as \"addressId\", street as \"street\", state as \"state\", city as \"city\", zip as \"zip\" from address a")
			           .getUniqueResultAs(Address.class);
		});
		
		assertThrows(() -> foundAddress.getEmployee(), SessionClosedException.class,
			"Cannot lazy-load field " + Address.class.getDeclaredField("employee") + ", session is no longer open");
	}
	
}
