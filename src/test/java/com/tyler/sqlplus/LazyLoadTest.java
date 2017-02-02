package com.tyler.sqlplus;

import com.tyler.sqlplus.annotation.LoadQuery;
import com.tyler.sqlplus.annotation.MapKey;
import com.tyler.sqlplus.base.DatabaseTest;
import com.tyler.sqlplus.base.databases.AbstractDatabase.Address;
import com.tyler.sqlplus.base.databases.AbstractDatabase.Employee;
import com.tyler.sqlplus.base.databases.AbstractDatabase.Employee.Type;
import com.tyler.sqlplus.base.databases.AbstractDatabase.Office;
import com.tyler.sqlplus.exception.AnnotationConfigurationException;
import com.tyler.sqlplus.exception.QueryInterpretationException;
import com.tyler.sqlplus.exception.SessionClosedException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.tyler.sqlplus.base.SQLPlusTesting.assertThrows;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class LazyLoadTest extends DatabaseTest {

	@Test
	public void testLazyLoadForeignKeyRelation() throws Exception {
		
		db.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into employee(type, name, hired, salary, address_id) values ('SALARY', 'tester-1', '2015-01-01', 20500, 1)"
		);
		
		db.getSQLPlus().transact(conn -> {
			
			Employee employee = 
					conn.createQuery("select employee_id as \"employeeId\", type as \"type\", name as \"name\", hired as \"hired\", salary as \"salary\", address_id as \"addressId\" from employee e ")
				        .getUniqueResultAs(Employee.class);
			
			assertNull(employee.address);
			Address lazyAddress = employee.getAddress();
			assertNotNull(employee.address);
			assertNotNull(lazyAddress);
			
			assertEquals("Maple Street", lazyAddress.street);
			assertEquals("Anytown", lazyAddress.city);
			assertEquals("MN", lazyAddress.state);
			assertEquals("12345", lazyAddress.zip);
		});
	}
	
	@Test
	public void testLazyLoadSingleRelationWithAnnotationOnField() throws Exception {
		
		db.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into employee(type, name, hired, salary, address_id) values ('SALARY', 'tester-1', '2015-01-01', 20500, 1)"
		);
		
		db.getSQLPlus().transact(conn -> {
			
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
		
		db.batch(
			"insert into employee(type, name, salary, hired) values('HOURLY', 'Billy Bob', '42000', '2015-01-01')",
			"insert into office(office_name, `primary`, employee_id) values ('Office A', 0, 1)",
			"insert into office(office_name, `primary`, employee_id) values ('Office B', 1, 1)",
			"insert into office(office_name, `primary`, employee_id) values ('Office C', 0, 1)"
		);
		
		db.getSQLPlus().transact(conn -> {
			
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
		
		db.batch(
			"insert into employee(type, name, salary, hired) values('HOURLY', 'Billy Bob', '42000', '2015-01-01')",
			"insert into office(office_name, `primary`, employee_id) values ('Office A', 0, 1)",
			"insert into office(office_name, `primary`, employee_id) values ('Office B', 1, 1)",
			"insert into office(office_name, `primary`, employee_id) values ('Office C', 0, 1)"
		);
		
		db.getSQLPlus().transact(conn -> {
			
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
		
		db.batch(
			"insert into employee(type, name, salary, hired) values('HOURLY', 'Billy Bob', '42000', '2015-01-01')",
			"insert into office(office_name, `primary`, employee_id) values ('Office A', 0, 1)",
			"insert into office(office_name, `primary`, employee_id) values ('Office B', 1, 1)",
			"insert into office(office_name, `primary`, employee_id) values ('Office C', 0, 1)"
		);
		
		db.getSQLPlus().transact(conn -> {
			
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

	public static class EmployeeLoadFromMethodWithUnresolvableField {

		public Integer employeeId;
		public List<Office> randomFieldName;

		@LoadQuery(
			"select office_id as \"officeId\", office_name as \"officeName\", employee_id as \"employeeId\", `primary` as \"primary\" " +
			"from office o " +
			"where o.employee_id = :employeeId"
		)
		public List<Office> getOffices() {
			return randomFieldName;
		}

	}

	@Test
	public void testErrorThrownIfLoadQueryOnMethodWithUnresolvableField() throws Exception {

		db.batch("insert into employee(type, name, salary, hired) values('HOURLY', 'Billy Bob', '42000', '2015-01-01')");
		db.getSQLPlus().transact(conn -> {
			
		assertThrows(
				() -> {
					conn.createQuery("select employee_id as \"employeeId\" from employee e ")
							.getUniqueResultAs(EmployeeLoadFromMethodWithUnresolvableField.class);
				},
				AnnotationConfigurationException.class,
				"Could not find lazy-load field 'offices' in " + EmployeeLoadFromMethodWithUnresolvableField.class
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
		
		db.batch(
			"insert into employee(type, name, salary, hired) values('HOURLY', 'Billy Bob', '42000', '2015-01-01')",
			"insert into office(office_name, `primary`, employee_id) values ('Office A', 0, 1)",
			"insert into office(office_name, `primary`, employee_id) values ('Office B', 1, 1)",
			"insert into office(office_name, `primary`, employee_id) values ('Office C', 0, 1)"
		);
		
		db.getSQLPlus().transact(conn -> {
			
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
		
		db.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into employee(type, name, hired, salary, address_id) values ('SALARY', 'tester-1', '2015-01-01', 20500, 1)"
		);
		
		Address foundAddress = db.getSQLPlus().query(conn -> {
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
		
		db.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into employee(type, name, hired, salary, address_id) values ('SALARY', 'tester-1', '2015-01-01', 20500, 1)"
		);
		
		db.getSQLPlus().transact(conn -> {
			
			Query query = conn.createQuery("select employee_id as \"employeeId\", type as \"type\", name as \"name\", hired as \"hired\", salary as \"salary\" from employee e ");
			EmployeeWildcardGeneric employeeWildcardGeneric = query.getUniqueResultAs(EmployeeWildcardGeneric.class);
			
			assertThrows(
				() -> employeeWildcardGeneric.getOffices(),
				QueryInterpretationException.class,
				"No valid query interpreters found for " + EmployeeWildcardGeneric.class.getDeclaredField("offices").getGenericType() + ". Make sure generic type info is present"
			);
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
		
		db.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into employee(type, name, hired, salary, address_id) values ('SALARY', 'tester-1', '2015-01-01', 20500, 1)"
		);
		
		db.getSQLPlus().transact(conn -> {
			
			Query query = conn.createQuery("select employee_id as \"employeeId\", type as \"type\", name as \"name\", hired as \"hired\", salary as \"salary\" from employee e ");
			EmployeeNoGeneric employeeNoGeneric = query.getUniqueResultAs(EmployeeNoGeneric.class);
			
			assertThrows(
				() -> employeeNoGeneric.getOffices(),
				QueryInterpretationException.class,
				"No valid query interpreters found for " + EmployeeWildcardGeneric.class.getDeclaredField("offices").getType() + ". Make sure generic type info is present"
			);
			
		});
		
	}

	public static class EmployeeMixtureOfLazyLoadGettersAndNonLazyLoadGetters {

		private Integer employeeId;

		@LoadQuery(
			"select office_id as \"office_id\", office_name as \"office_name\", employee_id as \"employee_id\", `primary` as \"primary\" " +
			"from office o " +
			"where o.employee_id = :employeeId"
		)
		private List<Office> offices = new ArrayList<>();

		private List<String> someList = new ArrayList<>();

		public List<Office> getOffices() {
			return offices;
		}

		public List<String> getSomeList() {
			return someList;
		}

	}

	@Test
	public void proxyInstanceShouldStillBeAbleToCallGetterForNonLoadQueryProperty() throws Exception {

		db.batch("insert into employee(type, name, hired, salary, address_id) values ('SALARY', 'tester-1', '2015-01-01', 20500, 1)");

		db.getSQLPlus().transact(conn -> {

			Query query = conn.createQuery("select employee_id as \"employeeId\", type as \"type\", name as \"name\", hired as \"hired\", salary as \"salary\" from employee e ");
			EmployeeMixtureOfLazyLoadGettersAndNonLazyLoadGetters employeeNoLoad = query.getUniqueResultAs(EmployeeMixtureOfLazyLoadGettersAndNonLazyLoadGetters.class);
			assertTrue(employeeNoLoad.getSomeList().isEmpty());

		});

	}

}
