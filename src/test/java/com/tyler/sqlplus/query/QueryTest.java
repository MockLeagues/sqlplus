package com.tyler.sqlplus.query;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.function.Function;

import org.junit.Test;

import com.tyler.sqlplus.annotation.Column;
import com.tyler.sqlplus.annotation.MultiRelation;
import com.tyler.sqlplus.exception.SQLSyntaxException;
import com.tyler.sqlplus.query.QueryTest.Employee.Type;

import base.EmployeeDBTest;

public class QueryTest extends EmployeeDBTest {

	@Test
	public void syntaxErrorIfUnkownParamAdded() {
		Query q = new Query("select * from myTable where id = :id", null);
		try {
			q.setParameter("idx", "123");
			fail("Excepted failure setting unknown parameter");
		} catch (SQLSyntaxException e) {
			assertEquals("Unknown query parameter: idx", e.getMessage());
		}
	}
	
	public static class Address {
		public Integer addressId;
		public String street;
		public String city;
		public String state;
		public String zip;
	}
	
	@Test
	public void mapResultsToNonRelationalPOJO() throws Exception {
		transact(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Elm Street', 'Othertown', 'CA', '54321')"
		);
		
		try (Connection conn = getConnection()) {
			List<Address> results = new Query("select address_id as addressId, street as street, state as state, city as city, zip as zip from address", conn).fetchAs(Address.class);
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
	}
	
	@Test
	public void queryWithParams() throws Exception {
		transact(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Elm Street', 'Othertown', 'CA', '54321')",
			"insert into address (street, city, state, zip) values('Main Street', 'Bakersfield', 'CA', '54321')"
		);
		try (Connection conn = getConnection()) {
			Address result =
				new Query("select address_id as addressId, street as street, state as state, city as city, zip as zip from address a where state = :state and city = :city", conn)
				.setParameter("state", "CA")
				.setParameter("city", "Othertown")
				.findAs(Address.class);
			assertEquals("Elm Street", result.street);
		}
	}
	
	@Test
	public void throwsErrorIfParamValueNotSet() throws Exception {
		transact("insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')");
		try (Connection conn = getConnection()) {
			try {
				new Query("select address_id from address where state = :state", conn).findAs(Address.class);
				fail("Expected query to fail because no parameter was set");
			} catch (SQLSyntaxException e) {
				assertEquals("Value not set for parameter state", e.getMessage());
			}
		}
	}
	
	public static class AddressWithAnnot {
		public @Column(name = "address_id") Integer addressId;
		public @Column(name = "street") String street;
		public @Column(name = "city") String city;
		public @Column(name = "state") String state;
		public @Column(name = "zip") String zip;
	}
	@Test
	public void mapResultsToNonRelationPOJOUsingAnnotations() throws Exception {
		transact(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Elm Street', 'Othertown', 'CA', '54321')"
		);
		
		try (Connection conn = getConnection()) {
			List<AddressWithAnnot> results = new Query("select * from address", conn).fetchAs(AddressWithAnnot.class);
			assertEquals(2, results.size());
			
			AddressWithAnnot first = results.get(0);
			assertEquals(new Integer(1), first.addressId);
			assertEquals("Maple Street", first.street);
			assertEquals("Anytown", first.city);
			assertEquals("MN", first.state);
			assertEquals("12345", first.zip);
			
			AddressWithAnnot second = results.get(1);
			assertEquals(new Integer(2), second.addressId);
			assertEquals("Elm Street", second.street);
			assertEquals("Othertown", second.city);
			assertEquals("CA", second.state);
			assertEquals("54321", second.zip);
		}
	}
	
	public static class Employee {
		public enum Type { HOURLY, SALARY; }
		public Integer employeeId;
		public Type type;
		public String name;
		public Integer salary;
		public Date hired;
	}
	@Test
	public void mapEnumTypes() throws Exception {
		transact("insert into employee(type, name, salary, hired) values('HOURLY', 'Billy Bob', '42000', '2015-01-01')");
		try (Connection conn = getConnection()) {
			List<Employee> es = new Query("select employee_id as employeeId, type as type, name as name, salary as salary, hired as hired from employee", conn).fetchAs(Employee.class);
			assertEquals(Type.HOURLY, es.get(0).type);
		}
	}
	
	public static class LocalDateConverter implements Function<java.sql.Date, LocalDate> {
		public LocalDate apply(java.sql.Date t) {
			return t.toLocalDate();
		}
	}
	public static class EmployeeLocalDate {
		public enum Type { HOURLY, SALARY; }
		public Integer employeeId;
		public Type type;
		public String name;
		public Integer salary;
		public @Column(name = "hired", converter = LocalDateConverter.class) LocalDate hired;
	}
	@Test
	public void useCustomConverter() throws Exception {
		transact("insert into employee(type, name, salary, hired) values('HOURLY', 'Billy Bob', '42000', '2015-01-01')");
		try (Connection conn = getConnection()) {
			List<Employee> es = new Query("select employee_id as employeeId, type as type, name as name, salary as salary, hired as hired from employee", conn).fetchAs(Employee.class);
			assertEquals("2015-01-01", es.get(0).hired.toString());
		}
	}
	
	public static class EmployeeRelations {
		public enum Type { HOURLY, SALARY; }
		public @Column(name = "employee_id", key = true) Integer employeeId;
		public Type type;
		public String name;
		public Integer salary;
		public Date hired;
		public @MultiRelation List<Office> offices;
		
		public static class Office {
			public @Column(name = "office_id", key = true) Integer officeKey;
			public @Column(name = "office_name") String name;
			public int primary;
		}
	}
	@Test
	public void mapRelations() throws Exception {
		transact(
			"insert into employee(type, name, salary, hired) values ('SALARY', 'Steve Jobs', '41000000', '1982-05-13')",
			"insert into office(office_name, `primary`, employee_id) values ('Office A', 1, 1)",
			"insert into office(office_name, `primary`, employee_id) values ('Office B', 0, 1)",
			"insert into office(office_name, `primary`, employee_id) values ('Office C', 0, 1)"
		);
		try (Connection conn= getConnection()) {
			List<EmployeeRelations> es = new Query("select * from employee e join office o on e.employee_id = o.employee_id", conn).fetchAs(EmployeeRelations.class);
			assertEquals(1, es.size());
			assertEquals(3, es.get(0).offices.size());
			assertEquals("Office A", es.get(0).offices.get(0).name);
			assertEquals("Office B", es.get(0).offices.get(1).name);
			assertEquals("Office C", es.get(0).offices.get(2).name);
		}
	}
	
	
	@Test
	public void queryScalar() throws Exception {
		transact(
			"insert into employee(type, name, salary, hired) values ('SALARY', 'Steve Jobs', '41000000', '1982-05-13')",
			"insert into office(office_name, `primary`, employee_id) values ('Office A', 1, 1)",
			"insert into office(office_name, `primary`, employee_id) values ('Office B', 0, 1)",
			"insert into office(office_name, `primary`, employee_id) values ('Office C', 0, 1)"
		);
		try (Connection conn = getConnection()) {
			Integer total = new Query("select sum(office_id) from office", conn).fetchScalar(Integer.class);
			assertEquals(new Integer(6), total);
		}
	}
	
	@Test
	public void bindParamsNonNullForCreate() throws Exception {
		Employee toCreate = new Employee();
		toCreate.hired = new Date();
		toCreate.name = "tester-pojo";
		toCreate.salary = 20000;
		toCreate.type = Type.HOURLY;
		try (Connection conn = getConnection()) {
			Query q = new Query("insert into employee(type, name, hired, salary) values (:type, :name, :hired, :salary)", conn).bindParams(toCreate, false);
			q.executeUpdate();
			Integer actual = Integer.parseInt(query("select count(*) from employee")[0][0]);
			assertEquals(new Integer(1), actual);
		}
	}
	
	@Test
	public void returnGeneratedKeys() throws Exception {
		transact("insert into employee(type, name, hired, salary) values ('SALARY', 'tester-1', '2015-01-01', 20500)");
		try (Connection conn = getConnection()) {
			Integer key =
				new Query("insert into employee(type, name, hired, salary) values (:type, :name, :hired, :salary)", conn)
				.setParameter("type", "HOURLY")
				.setParameter("name", "tester-2")
				.setParameter("hired", "2015-01-01")
				.setParameter("salary", "10000")
				.executeUpdate(Integer.class)
				.get(0);
			assertEquals(new Integer(2), key);
		}
	}
	
}
