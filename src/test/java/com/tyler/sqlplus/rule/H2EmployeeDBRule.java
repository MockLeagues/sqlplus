package com.tyler.sqlplus.rule;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.List;

import com.tyler.sqlplus.annotation.LoadQuery;
import com.tyler.sqlplus.exception.SqlRuntimeException;

public class H2EmployeeDBRule extends AbstractDBRule {

	@Override
	public Connection getConnection() {
		try {
			return DriverManager.getConnection("jdbc:h2:mem:db1;DB_CLOSE_DELAY=-1", "sa", "sa");
		} catch (SQLException e) {
			throw new SqlRuntimeException(e);
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
			"select office_id as \"officeId\", office_name as \"officeName\", employee_id as \"employeeId\", `primary` as \"primary\" " +
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
			"select employee_id as \"employeeId\", type as \"type\", name as \"name\", hired as \"hired\", salary as \"salary\" " +
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
	
	@Override
	public void setupSchema() {
		
		try (Connection conn = getConnection()) {
			
			Statement st = conn.createStatement();
			st.executeUpdate(
				"create table `address` (" +
					"`address_id` int(11) not null auto_increment," +
					"`street` varchar(45) default null," +
					"`city` varchar(45) default null," +
					"`state` varchar(45) default null," +
					"`zip` varchar(45) default null," +
					"primary key (`address_id`)" +
				")"
			);
			
			st.executeUpdate(
				"create table `meeting` (" +
					"`meeting_id` int(11) not null auto_increment," +
					"`office_id` int(11) not null," +
					"`start_time` int(11) not null," +
					"`end_time` int(11) not null," +
					"`topic` varchar(45) not null," +
					"primary key (`meeting_id`)" +
				")"
			);
			
			st.executeUpdate(
				"create table `employee` (" +
					"`employee_id` int(11) not null auto_increment," +
					"`type` varchar(45) not null," +
					"`name` varchar(45) not null," +
					"`salary` int(11) null," +
					"`hired` date null," +
					"`address_id` int(11) default null," +
					"primary key (`employee_id`)" +
				")"
			);
			
			st.executeUpdate(
				"create table `office` (" +
					"`office_id` int(11) not null auto_increment," +
					"`office_name` varchar(45) not null," +
					"`primary` tinyint(4) not null," +
					"`employee_id` int(11) default null," +
					"primary key (`office_id`)" +
				")"
			);
			
			st.executeUpdate(
				"create table `types_table` (" +
					"`int_field` int(10)," +
					"`float_field` float," +
					"`decimal_field` decimal(10, 2)," +
					"`varchar_field` varchar(15)," +
					"`char_field` varchar(1)," +
					"`tiny_int_field` tinyint," +
					"`timestamp_field` timestamp," +
					"`date_field` date," +
					"`datetime_field` datetime," +
					"`time_field` time" +
				")"
			);
		}
		catch (SQLException e) {
			throw new SqlRuntimeException(e);
		}
	}
	
	@Override
	public void destroySchema() {
		try (Connection conn = getConnection()) {
			Statement st = conn.createStatement();
			st.executeUpdate("drop table address");
			st.executeUpdate("drop table meeting");
			st.executeUpdate("drop table employee");
			st.executeUpdate("drop table office");
			st.executeUpdate("drop table types_table");
		}
		catch (SQLException e) {
			throw new SqlRuntimeException(e);
		}
	}
	
}
