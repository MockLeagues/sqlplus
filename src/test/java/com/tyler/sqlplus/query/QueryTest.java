package com.tyler.sqlplus.query;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;

import com.tyler.sqlplus.annotation.Column;
import com.tyler.sqlplus.annotation.Key;
import com.tyler.sqlplus.annotation.MultiRelation;
import com.tyler.sqlplus.annotation.SingleRelation;
import com.tyler.sqlplus.exception.MappingException;
import com.tyler.sqlplus.exception.SQLSyntaxException;
import com.tyler.sqlplus.query.QueryTest.Employee.Type;

import base.EmployeeDBTest;

public class QueryTest extends EmployeeDBTest {

	@Test
	public void syntaxErrorIfUnkownParamAdded() throws SQLException {
		try (Connection conn = getConnection()) {
			Query q = new Query("select * from myTable where id = :id", conn);
			q.setParameter("idx", "123");
			fail("Excepted failure setting unknown parameter");
		} catch (SQLSyntaxException e) {
			assertEquals("Unknown query parameter: idx", e.getMessage());
		}
	}
	
	@Test
	public void throwsErrorIfParamValueNotSet() throws Exception {
		transact("insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')");
		try (Connection conn = getConnection()) {
			try {
				new Query("select address_id from address where state = :state and city = :city", conn).setParameter("state", "s").getUniqueResultAs(Address.class);
				fail("Expected query to fail because no parameter was set");
			} catch (SQLSyntaxException e) {
				assertEquals("Missing parameter values for the following parameters: [city]", e.getMessage());
			}
		}
	}
	
	public static class Address {
		
		public Integer addressId;
		public String street;
		public String city;
		public String state;
		public String zip;
		
		public Address() {}
		
		public Address(String street, String city, String state, String zip) {
			this.street = street;
			this.city = city;
			this.state = state;
			this.zip = zip;
		}
		
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
	public void queryMaps() throws Exception {
		transact(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Elm Street', 'Othertown', 'CA', '54321')"
		);
		
		List<Map<String, String>> rows = SQL_PLUS.fetch("select * from address");
		
		assertEquals(2, rows.size());
		
		Map<String, String> first = rows.get(0);
		assertEquals("Maple Street", first.get("street"));
		assertEquals("Anytown", first.get("city"));
		assertEquals("MN", first.get("state"));
		assertEquals("12345", first.get("zip"));
		
		Map<String, String> second = rows.get(1);
		assertEquals("Elm Street", second.get("street"));
		assertEquals("Othertown", second.get("city"));
		assertEquals("CA", second.get("state"));
		assertEquals("54321", second.get("zip"));
	}
	
	@Test
	public void batchUpdate() throws Exception {
		
		List<Address> toInsert = Arrays.asList(
			new Address("street1", "city1", "state1", "zip1"),
			new Address("street2", "city2", "state2", "zip2"),
			new Address("street3", "city3", "state3", "zip3"),
			new Address("street4", "city4", "state4", "zip4")
		);
		
		SQL_PLUS.batchUpdate("insert into address (street, city, state, zip) values (:street, :city, :state, :zip)", toInsert);
		
		String[][] results = query("select * from address");
		String[][] expect = {
			{"1", "street1", "city1", "state1", "zip1"},
			{"2", "street2", "city2", "state2", "zip2"},
			{"3", "street3", "city3", "state3", "zip3"},
			{"4", "street4", "city4", "state4", "zip4"}
		};
		
		assertArrayEquals(expect, results);
	}
	
	@Test
	public void update() throws Exception {
		
		transact(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Elm Street', 'Othertown', 'CA', '54321')"
		);
		
		SQL_PLUS.update("delete from address where street = ?", "Maple Street");
		
		String[][] results = query("select * from address");
		String[][] expect = {
			{"2", "Elm Street", "Othertown", "CA", "54321"}
		};
		
		assertArrayEquals(expect, results);
	}
	
	@Test
	public void batchExec() throws Exception {
		
		SQL_PLUS.batchExec(
			"insert into address (street, city, state, zip) values ('street1', 'city1', 'state1', 'zip1')",
			"insert into address (street, city, state, zip) values ('street2', 'city2', 'state2', 'zip2')",
			"insert into address (street, city, state, zip) values ('street3', 'city3', 'state3', 'zip3')",
			"insert into address (street, city, state, zip) values ('street4', 'city4', 'state4', 'zip4')"
		);
	
		String[][] results = query("select * from address");
		String[][] expect = {
			{"1", "street1", "city1", "state1", "zip1"},
			{"2", "street2", "city2", "state2", "zip2"},
			{"3", "street3", "city3", "state3", "zip3"},
			{"4", "street4", "city4", "state4", "zip4"}
		};
		
		assertArrayEquals(expect, results);
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
				.getUniqueResultAs(Address.class);
			assertEquals("Elm Street", result.street);
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
	
	@Test
	public void leavesNullValuesIfCertainFieldsNotPresentInResults() throws Exception {
		transact("insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')");
		
		try (Connection conn = getConnection()) {
			Address result = new Query("select street, city from address", conn).getUniqueResultAs(Address.class);
			assertNull(result.state);
			assertNull(result.zip);
			assertNotNull(result.street);
			assertNotNull(result.city);
		}
	}
	
	public static class Employee {
		public enum Type { HOURLY, SALARY; }
		public Integer employeeId;
		public Type type;
		public String name;
		public Date hired;
		public Integer salary;
	}
	@Test
	public void mapEnumTypes() throws Exception {
		transact("insert into employee(type, name, salary, hired) values('HOURLY', 'Billy Bob', '42000', '2015-01-01')");
		try (Connection conn = getConnection()) {
			List<Employee> es = new Query("select employee_id as employeeId, type as type, name as name, salary as salary, hired as hired from employee", conn).fetchAs(Employee.class);
			assertEquals(Type.HOURLY, es.get(0).type);
		}
	}
	
	public static class EmployeeMultiRelation {
		public enum Type { HOURLY, SALARY; }
		public @Key @Column(name = "employee_id") Integer employeeId;
		public Type type;
		public String name;
		public Integer salary;
		public Date hired;
		public @MultiRelation List<Office> offices;
		
		public static class Office {
			public @Key @Column(name = "office_id") Integer officeKey;
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
			List<EmployeeMultiRelation> es = new Query("select * from employee e join office o on e.employee_id = o.employee_id", conn).fetchAs(EmployeeMultiRelation.class);
			assertEquals(1, es.size());
			assertEquals(3, es.get(0).offices.size());
			assertEquals("Office A", es.get(0).offices.get(0).name);
			assertEquals("Office B", es.get(0).offices.get(1).name);
			assertEquals("Office C", es.get(0).offices.get(2).name);
		}
	}
	
	@Test
	public void leavesMultiRelationNullIfNoFieldsForIt() throws Exception {
		transact(
			"insert into employee(type, name, salary, hired) values ('SALARY', 'Steve Jobs', '41000000', '1982-05-13')",
			"insert into office(office_name, `primary`, employee_id) values ('Office A', 1, 1)",
			"insert into office(office_name, `primary`, employee_id) values ('Office B', 0, 1)",
			"insert into office(office_name, `primary`, employee_id) values ('Office C', 0, 1)"
		);
		try (Connection conn= getConnection()) {
			List<EmployeeMultiRelation> es = new Query("select * from employee e", conn).fetchAs(EmployeeMultiRelation.class);
			assertEquals(1, es.size());
			assertNull(es.get(0).offices);
		}
	}
	
	public static class EmployeeSingleRelation {
		public enum Type { HOURLY, SALARY; }
		public Integer employeeId;
		public Type type;
		public String name;
		public Integer salary;
		public Date hired;
		public @SingleRelation Address address;
	}
	@Test
	public void leavesSingleRelationNullIfNoFieldsForIt() throws Exception {
		transact("insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')");
		transact("insert into employee(type, name, salary, hired, address_id) values('HOURLY', 'Billy Bob', '42000', '2015-01-01', 1)");
		try (Connection conn = getConnection()) {
			EmployeeSingleRelation emp = new Query("select * from employee", conn).getUniqueResultAs(EmployeeSingleRelation.class);
			assertNull(emp.address);
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
			Query q = new Query("insert into employee(type, name, hired, salary) values (:type, :name, :hired, :salary)", conn).addBatch(toCreate);
			q.executeUpdate();
			Integer actual = Integer.parseInt(query("select count(*) from employee")[0][0]);
			assertEquals(new Integer(1), actual);
		}
	}
	
	@Test
	public void bindParamsNonNullForCreateIfParamsOutOfDeclaredOrder() throws Exception {
		Employee toCreate = new Employee();
		toCreate.hired = new Date();
		toCreate.name = "tester-pojo";
		toCreate.salary = 20000;
		toCreate.type = Type.HOURLY;
		try (Connection conn = getConnection()) {
			Query q = new Query("insert into employee(hired, type, name, salary) values (:hired, :type, :name, :salary)", conn).addBatch(toCreate);
			q.executeUpdate();
			Integer actual = Integer.parseInt(query("select count(*) from employee")[0][0]);
			assertEquals(new Integer(1), actual);
		}
	}
	
	public static class EmployeeMissingBindParam {
		public enum Type { HOURLY, SALARY; }
		public com.tyler.sqlplus.query.QueryTest.Employee.Type type;
		public String name;
		public Integer salary;
	}
	@Test
	public void bindParamsFailsIfNoMemberForParam() throws Exception {
		EmployeeMissingBindParam toCreate = new EmployeeMissingBindParam();
		toCreate.name = "tester-pojo";
		toCreate.salary = 20000;
		toCreate.type = Type.HOURLY;
		try (Connection conn = getConnection()) {
			assertThrows(() -> {
				new Query("insert into employee(hired, type, name, salary) values (:hired, :type, :name, :salary)", conn).addBatch(toCreate);
			}, MappingException.class);
		}
	}
	
	@Test
	public void returnGeneratedKeys() throws Exception {
		transact("insert into employee(type, name, hired, salary) values ('SALARY', 'tester-1', '2015-01-01', 20500)");
		try (Connection conn = getConnection()) {
			Optional<List<Integer>> keys = new Query("insert into employee(type, name, hired, salary) values (:type, :name, :hired, :salary)", conn)
			                                   .setParameter("type", "HOURLY")
			                                   .setParameter("name", "tester-2")
			                                   .setParameter("hired", "2015-01-01")
			                                   .setParameter("salary", "10000")
			                                   .executeUpdate(Integer.class);
			assertTrue(keys.isPresent());
			assertEquals(new Integer(2), keys.get().get(0));
		}
	}

}
