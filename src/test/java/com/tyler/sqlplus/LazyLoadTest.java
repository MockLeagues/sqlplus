package com.tyler.sqlplus;

import static com.tyler.sqlplus.test.SqlPlusTesting.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;

import com.tyler.sqlplus.annotation.LoadQuery;
import com.tyler.sqlplus.annotation.MapKey;
import com.tyler.sqlplus.exception.LazyLoadException;
import com.tyler.sqlplus.exception.SessionClosedException;
import com.tyler.sqlplus.rule.H2EmployeeDBRule;
import com.tyler.sqlplus.rule.H2EmployeeDBRule.Address;
import com.tyler.sqlplus.rule.H2EmployeeDBRule.Employee;
import com.tyler.sqlplus.rule.H2EmployeeDBRule.Employee.Type;
import com.tyler.sqlplus.rule.H2EmployeeDBRule.Office;

public class LazyLoadTest {

	@Rule
	public H2EmployeeDBRule h2 = new H2EmployeeDBRule();
	
	@Test
	public void testLazyLoadSingleRelationWithAnnotationOnField() throws Exception {
		
		h2.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into employee(type, name, hired, salary, address_id) values ('SALARY', 'tester-1', '2015-01-01', 20500, 1)"
		);
		
		h2.getSQLPlus().transact(conn -> {
			
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
	public void testLazyLoadMultipleRelationsWithAnnotationOnField() throws Exception {
		
		h2.batch(
			"insert into employee(type, name, salary, hired) values('HOURLY', 'Billy Bob', '42000', '2015-01-01')",
			"insert into office(office_name, `primary`, employee_id) values ('Office A', 0, 1)",
			"insert into office(office_name, `primary`, employee_id) values ('Office B', 1, 1)",
			"insert into office(office_name, `primary`, employee_id) values ('Office C', 0, 1)"
		);
		
		h2.getSQLPlus().transact(conn -> {
			
			Employee employee = 
				conn.createQuery("select employee_id as \"employeeId\", type as \"type\", name as \"name\", hired as \"hired\", salary as \"salary\" from employee e ")
			        .getUniqueResultAs(Employee.class);
			
			// Make sure it stays null until getter is called
			assertNull(employee.offices);
			List<Office> offices = employee.getOffices();
			assertNotNull(offices);
			
			assertEquals(3, offices.size());
			assertEquals("Office A", offices.get(0).officeName);
			assertEquals("Office B", offices.get(1).officeName);
			assertEquals("Office C", offices.get(2).officeName);
		});
		
	}
	
	static class EmployeeLoadFromMethod {
		
		public Integer employeeId;

		public List<Office> offices;
		
		@LoadQuery(
			"select office_id as \"officeId\", office_name as \"officeName\", employee_id as \"employeeId\", `primary` as \"primary\" " +
			"from office o " +
			"where o.employee_id = :employeeId"
		)
		public List<Office> getOffices() {
			return offices;
		}
		
	}
	
	@Test
	public void testLazyLoadWithAnnotationOnMethodWithInferredFieldName() throws Exception {
		
		h2.batch(
			"insert into employee(type, name, salary, hired) values('HOURLY', 'Billy Bob', '42000', '2015-01-01')",
			"insert into office(office_name, `primary`, employee_id) values ('Office A', 0, 1)",
			"insert into office(office_name, `primary`, employee_id) values ('Office B', 1, 1)",
			"insert into office(office_name, `primary`, employee_id) values ('Office C', 0, 1)"
		);
		
		h2.getSQLPlus().transact(conn -> {
			
			EmployeeLoadFromMethod  employee = 
				conn.createQuery("select employee_id as \"employeeId\" from employee e ")
			        .getUniqueResultAs(EmployeeLoadFromMethod.class);
			
			List<Office> offices = employee.getOffices();
			assertNotNull(offices);
			
			assertEquals(3, offices.size());
			assertEquals("Office A", offices.get(0).officeName);
			assertEquals("Office B", offices.get(1).officeName);
			assertEquals("Office C", offices.get(2).officeName);
		});
	}
	
	static class EmployeeLoadFromMethodWithExplicitField {
		
		public Integer employeeId;

		public List<Office> listOfOffices;
		
		@LoadQuery(
			value = "select office_id as \"officeId\", office_name as \"officeName\", employee_id as \"employeeId\", `primary` as \"primary\" " +
			        "from office o " +
			        "where o.employee_id = :employeeId",
			field = "listOfOffices"
		)
		public List<Office> getTheOffices() {
			return listOfOffices;
		}
		
	}
	
	@Test
	public void testLazyLoadWithAnnotationOnMethodWithExplicitFieldName() throws Exception {
		
		h2.batch(
			"insert into employee(type, name, salary, hired) values('HOURLY', 'Billy Bob', '42000', '2015-01-01')",
			"insert into office(office_name, `primary`, employee_id) values ('Office A', 0, 1)",
			"insert into office(office_name, `primary`, employee_id) values ('Office B', 1, 1)",
			"insert into office(office_name, `primary`, employee_id) values ('Office C', 0, 1)"
		);
		
		h2.getSQLPlus().transact(conn -> {
			
			EmployeeLoadFromMethodWithExplicitField  employee = 
				conn.createQuery("select employee_id as \"employeeId\" from employee e ")
			        .getUniqueResultAs(EmployeeLoadFromMethodWithExplicitField.class);
			
			List<Office> offices = employee.getTheOffices();
			assertNotNull(offices);
			
			assertEquals(3, offices.size());
			assertEquals("Office A", offices.get(0).officeName);
			assertEquals("Office B", offices.get(1).officeName);
			assertEquals("Office C", offices.get(2).officeName);
		});
	}
	
	@Test
	public void testLazyLoadCachesDataAfterFirstRetrieval() throws Exception {
		
		h2.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into employee(type, name, hired, salary, address_id) values ('SALARY', 'tester-1', '2015-01-01', 20500, 1)"
		);
		
		h2.getSQLPlus().transact(conn -> {
			
			Address singleAddress = 
			    conn.createQuery("select address_id as \"addressId\", street as \"street\", state as \"state\", city as \"city\", zip as \"zip\" from address a")
			        .getUniqueResultAs(Address.class);
			
			Employee cachedEmployee = singleAddress.getEmployee();
			h2.batch("update employee set name = 'new-name' where employee_id = 1");
			cachedEmployee = singleAddress.getEmployee(); // Should not have to make another trip to the DB
			
			assertEquals("tester-1", cachedEmployee.name);
		});
		
	}
	
	static class EmployeeLoadFromMethodWithUnresolvableField {
		
		public Integer employeeId;

		public List<Office> randomFieldName;
		
		@LoadQuery(
			value = "select office_id as \"officeId\", office_name as \"officeName\", employee_id as \"employeeId\", `primary` as \"primary\" " +
			        "from office o " +
			        "where o.employee_id = :employeeId"
		)
		public List<Office> getOffices() {
			return randomFieldName;
		}
		
	}
	
	@Test
	public void testErrorThrownIfLoadQueryOnMethodWithUnresolvableField() throws Exception {
		h2.batch("insert into employee(type, name, salary, hired) values('HOURLY', 'Billy Bob', '42000', '2015-01-01')");
		h2.getSQLPlus().transact(conn -> {
			
			EmployeeLoadFromMethodWithUnresolvableField employee = 
				conn.createQuery("select employee_id as \"employeeId\" from employee e ")
			        .getUniqueResultAs(EmployeeLoadFromMethodWithUnresolvableField.class);
			
			assertThrows(
				() -> employee.getOffices(),
				LazyLoadException.class,
				"Inferred lazy-load field 'offices' not found when executing method " + EmployeeLoadFromMethodWithUnresolvableField.class.getDeclaredMethod("getOffices")
			);
			
		});
	}
	
	public static class EmployeeLazyLoadMaps {
		
		public Integer employeeId;

		@LoadQuery(
			"select office_id as \"officeId\", office_name as \"officeName\", employee_id as \"employeeId\", `primary` as \"primary\" " +
			"from office o " +
			"where o.employee_id = :employeeId"
		)
		@MapKey("officeId")
		public Map<Integer, Office> offices;
		
		public Map<Integer, Office> getOffices() {
			return offices;
		}
		
	}
	
	@Test
	public void testLoadMapWithMapKeySpecified() throws Exception {
		
		h2.batch(
			"insert into employee(type, name, salary, hired) values('HOURLY', 'Billy Bob', '42000', '2015-01-01')",
			"insert into office(office_name, `primary`, employee_id) values ('Office A', 0, 1)",
			"insert into office(office_name, `primary`, employee_id) values ('Office B', 1, 1)",
			"insert into office(office_name, `primary`, employee_id) values ('Office C', 0, 1)"
		);
		
		h2.getSQLPlus().transact(conn -> {
			
			EmployeeLazyLoadMaps employee = 
				conn.createQuery("select employee_id as \"employeeId\", type as \"type\", name as \"name\", hired as \"hired\", salary as \"salary\" from employee e ")
			        .getUniqueResultAs(EmployeeLazyLoadMaps.class);
			
			Map<Integer, Office> offices = employee.getOffices();
			assertEquals(3, offices.size());
			
			Office officeA = offices.get(1);
			assertEquals("Office A", officeA.officeName);
			
			Office officeB = offices.get(2);
			assertEquals("Office B", officeB.officeName);
			
			Office officeC = offices.get(3);
			assertEquals("Office C", officeC.officeName);
			
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
	
	public static class EmployeeWildcardGeneric {
		
		public Integer employeeId;

		@LoadQuery(
			"select office_id as \"office_id\", office_name as \"office_name\", employee_id as \"employee_id\", `primary` as \"primary\" " +
			"from office o " +
			"where o.employee_id = :employeeId"
		)
		public List<?> offices;
		
		@SuppressWarnings("unchecked")
		public List<Office> getOffices() {
			return (List<Office>) offices;
		}
		
	}
	
	@Test
	public void testErrorThrownWhenLazyLoadWildcardGeneric() throws Exception {
		
		h2.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into employee(type, name, hired, salary, address_id) values ('SALARY', 'tester-1', '2015-01-01', 20500, 1)"
		);
		
		h2.getSQLPlus().transact(conn -> {
			
			EmployeeWildcardGeneric employeeWildcardGeneric =
				conn.createQuery("select employee_id as \"employeeId\", type as \"type\", name as \"name\", hired as \"hired\", salary as \"salary\" from employee e ")
			        .getUniqueResultAs(EmployeeWildcardGeneric.class);
			
			assertThrows(() -> employeeWildcardGeneric.getOffices(), LazyLoadException.class, "Field " + EmployeeWildcardGeneric.class.getDeclaredField("offices") + " contains " +
				"a wildcard ('?') generic type. This is not adequate for determining the type of lazy-loaded one to many collections");
		});
		
	}
	
	public static class EmployeeNoGeneric {
		
		public Integer employeeId;

		@SuppressWarnings("rawtypes")
		@LoadQuery(
			"select office_id as \"office_id\", office_name as \"office_name\", employee_id as \"employee_id\", `primary` as \"primary\" " +
			"from office o " +
			"where o.employee_id = :employeeId"
		)
		public List offices;
		
		@SuppressWarnings("unchecked")
		public List<Office> getOffices() {
			return (List<Office>) offices;
		}
		
	}
	
	@Test
	public void testErrorThrownWhenLazyLoadUntypedCollection() throws Exception {
		
		h2.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into employee(type, name, hired, salary, address_id) values ('SALARY', 'tester-1', '2015-01-01', 20500, 1)"
		);
		
		h2.getSQLPlus().transact(conn -> {
			
			EmployeeNoGeneric employeeNoGeneric =
				conn.createQuery("select employee_id as \"employeeId\", type as \"type\", name as \"name\", hired as \"hired\", salary as \"salary\" from employee e ")
			        .getUniqueResultAs(EmployeeNoGeneric.class);
			
			assertThrows(() -> employeeNoGeneric.getOffices(), LazyLoadException.class, "Field " + EmployeeNoGeneric.class.getDeclaredField("offices") + " does not contain generic type info. " +
					"This is required for determining the types of lazy-loaded one to many relations");
		});
		
	}
	
}
