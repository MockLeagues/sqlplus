package com.tyler.sqlplus;

import com.tyler.sqlplus.base.DatabaseTest;
import com.tyler.sqlplus.base.databases.AbstractDatabase.Address;
import com.tyler.sqlplus.base.databases.AbstractDatabase.Employee;
import com.tyler.sqlplus.base.databases.AbstractDatabase.Employee.Type;
import com.tyler.sqlplus.exception.QueryStructureException;
import com.tyler.sqlplus.exception.ReflectionException;
import com.tyler.sqlplus.exception.SQLRuntimeException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.tyler.sqlplus.base.SQLPlusTesting.assertThrows;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class QueryTest extends DatabaseTest {

	@Test
	public void errorThrownIfUnknownParamAdded() throws SQLException {
		db.getSQLPlus().transact(conn -> {
			assertThrows(() -> {
				Query q = conn.createQuery("select * from employee where employee_id = :id");
				q.setParameter("idx", "123");
			}, QueryStructureException.class, "Unknown query parameter: idx");
		});
	}
	
	@Test
	public void errorThrownIfParamValueNotSet() throws Exception {
		db.batch("insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')");
		db.getSQLPlus().transact(conn -> {
			assertThrows(
				() -> conn.createQuery("select address_id from address where state = :state and city = :city").setParameter("state", "s").getUniqueResultAs(Address.class),
				QueryStructureException.class,
			"Missing parameter values for the following parameters: [city]"
			);
		});
	}
	
	@Test
	public void errorThrownIfParamIndexOutOfRange() throws Exception {
		db.getSQLPlus().transact(conn -> {
			assertThrows(() -> {
				conn.createQuery("select address_id from address where city = ?").setParameter(1, "city").setParameter(2, "state").getUniqueResultAs(Address.class);
			}, QueryStructureException.class, "Parameter index 2 is out of range of this query's parameters (max parameters: 1)");
		});
	}
	
	@Test
	public void errorThrownIfDuplicateParamAdded() throws Exception {
		db.getSQLPlus().transact(conn -> {
			assertThrows(() -> {
				conn.createQuery("select address_id from address where city = :city and street = :city").setParameter(1, "city").setParameter(2, "state").getUniqueResultAs(Address.class);
			}, QueryStructureException.class, "Duplicate parameter 'city' in query:\n" +
							"select address_id from address where city = :city and street = :city");
		});
	}
	
	@Test
	public void errorThrownIfNoParamsSet() throws Exception {
		db.getSQLPlus().transact(conn -> {
			assertThrows(() -> {
				conn.createQuery("select * from employee where name = :name").fetch();
			}, QueryStructureException.class, "No parameters set");
		});
	}
	
	@Test
	public void paramsCanBeSetWithLabels() throws Exception {
		db.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Elm Street', 'Othertown', 'CA', '54321')",
			"insert into address (street, city, state, zip) values('Main Street', 'Bakersfield', 'CA', '54321')"
		);
		db.getSQLPlus().transact(conn -> {
			Address result = conn.createQuery("select address_id as \"addressId\", street as \"street\", state as \"state\", city as \"city\", zip as \"zip\" from address a where state = :state and city = :city")
			                     .setParameter("state", "CA")
			                     .setParameter("city", "Othertown")
			                     .getUniqueResultAs(Address.class);
			assertEquals("Elm Street", result.street);
		});
	}

	@Test
	public void pojoFieldsCanBeMappedByConvertingUnderscoreNamesToCorrespondingCamelCaseNames() throws Exception {

		db.batch("insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')");

		db.getSQLPlus().transact(conn -> {
			Address addr = conn.createQuery("select ADDRESS_ID, STREET, STATE, CITY, ZIP from address a").getUniqueResultAs(Address.class);
			assertEquals("Maple Street", addr.street);
			assertEquals("Anytown", addr.city);
			assertEquals("MN", addr.state);
			assertEquals("12345", addr.zip);
		});
	}

	@Test
	public void paramsCanBeSetWithMixtureOfLabelsAndQuestionmarks() throws Exception {
		
		db.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Elm Street', 'Othertown', 'CA', '54321')"
		);
		
		db.getSQLPlus().transact(conn -> {
			
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
	public void paramsCanBeSetInQueryConstructor() throws Exception {

		db.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Elm Street', 'Othertown', 'CA', '54321')"
		);

		db.getSQLPlus().transact(conn -> {
			Address addr = conn.createQuery("select * from address where city=? and state=?", "Anytown", "MN").getUniqueResultAs(Address.class);
			assertEquals("Maple Street", addr.street);
			assertEquals("Anytown", addr.city);
			assertEquals("MN", addr.state);
			assertEquals("12345", addr.zip);
		});
	}
	
	@Test
	public void resultsCanBeFetchesAsMaps() throws Exception {
		
		db.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Elm Street', 'Othertown', 'CA', '54321')"
		);
		
		List<Map<String, Object>> rows = db.getSQLPlus().query(conn -> conn.createQuery("select street as STREET, city as CITY, state as STATE, zip as ZIP from address").fetch());
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
	public void statementsCanBeExecutedInBatch() throws Exception {
		
		db.getSQLPlus().batchExec(
			"insert into address (street, city, state, zip) values ('street1', 'city1', 'state1', 'zip1')",
			"insert into address (street, city, state, zip) values ('street2', 'city2', 'state2', 'zip2')",
			"insert into address (street, city, state, zip) values ('street3', 'city3', 'state3', 'zip3')",
			"insert into address (street, city, state, zip) values ('street4', 'city4', 'state4', 'zip4')"
		);
	
		String[][] results = db.query("select * from address");
		String[][] expect = {
			{"1", "street1", "city1", "state1", "zip1"},
			{"2", "street2", "city2", "state2", "zip2"},
			{"3", "street3", "city3", "state3", "zip3"},
			{"4", "street4", "city4", "state4", "zip4"}
		};
		
		assertArrayEquals(expect, results);
	}

	@Test
	public void resultsAreProcessedInBatchWhenTotalDividesEvenlyIntoBatchSize() throws Exception {
		
		int numAddress = 20;
		List<String> insertSqls = new ArrayList<>();
		for (int i = 1; i <= numAddress; i++) {
			insertSqls.add("insert into address (street, city, state, zip) values ('street" + i + "', 'city" + i + "', 'state" + i + "', 'zip" + i + "')");
		}
		db.getSQLPlus().batchExec(insertSqls.stream().toArray(String[]::new));
		
		int[] numBatchesSeen = {0};
		
		db.getSQLPlus().transact(conn -> {
			Query q = conn.createQuery("select street as \"street\", city as \"city\" from address");
			q.batchProcess(Address.class, 4, batch -> {
				assertEquals(4, batch.size());
				numBatchesSeen[0]++;
			});
		});
		
		assertEquals(5, numBatchesSeen[0]);
	}
	
	@Test
	public void resultsAreProcessedInBatchWhenTotalDoesNotDivideEvenlyIntoBatchSize() throws Exception {
		
		int numAddress = 22;
		List<String> insertSqls = new ArrayList<>();
		for (int i = 1; i <= numAddress; i++) {
			insertSqls.add("insert into address (street, city, state, zip) values ('street" + i + "', 'city" + i + "', 'state" + i + "', 'zip" + i + "')");
		}
		db.getSQLPlus().batchExec(insertSqls.stream().toArray(String[]::new));
		
		int[] numBatchesSeen = {0};
		
		db.getSQLPlus().transact(conn -> {
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
	public void fieldsNotSelectedRemainNullInResultingPOJO() throws Exception {
		db.batch("insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')");
		
		Address result = db.getSQLPlus().query(sess -> sess.createQuery("select street as \"street\", city as \"city\" from address").getUniqueResultAs(Address.class));
		assertNull(result.state);
		assertNull(result.zip);
		assertNotNull(result.street);
		assertNotNull(result.city);
	}
	
	@Test
	public void enumTypesCanBeMapped() throws Exception {
		db.batch("insert into employee(type, name, salary, hired) values('HOURLY', 'Billy Bob', '42000', '2015-01-01')");
		Employee emp = db.getSQLPlus().query(sess -> {
			return sess.createQuery("select employee_id as \"employeeId\", type as \"type\", name as \"name\", salary as \"salary\", hired as \"hired\" from employee")
			           .getUniqueResultAs(Employee.class);
		});
		assertEquals(Type.HOURLY, emp.type);
	}
	
	@Test
	public void paramBatchesCanBeAddedExplicitly() throws Exception {

		db.getSQLPlus().transact(conn -> {
			
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
		});
		
		String[][] expect = {
			{"1", "SALARY", "test1", "2015-01-01", "100", null},
			{"2", "SALARY", "test2", "2015-01-02", "200", null}
		};
		
		String[][] actual = db.query("select employee_id, type, name, hired, salary, address_id from employee");
		assertArrayEquals(expect, actual);
	}
	
	@Test
	public void lastParamBatchGetsAutoAddedIfNotDoneExplicitly() throws Exception {
		
		db.getSQLPlus().transact(conn -> {
			
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
		});
		
		String[][] expect = {
			{"1", "SALARY", "test1", "2015-01-01", "100", null},
			{"2", "SALARY", "test2", "2015-01-02", "200", null}
		};
		
		String[][] actual = db.query("select employee_id, type, name, hired, salary, address_id from employee");
		assertArrayEquals(expect, actual);
	}
	
	@Test
	public void finishingABatchThrowsErrorWhenParamsMissing() throws Exception {
		db.getSQLPlus().transact(conn -> {
			Query q = conn.createQuery("insert into employee(type, name, hired, salary) values (:type, :name, :hired, :salary)")
			    .setParameter("type", Type.SALARY)
			    .setParameter("name", "test1")
			    .setParameter("hired", "2015-01-01");
			
			assertThrows(q::finishBatch, QueryStructureException.class, "Missing parameter values for the following parameters: [salary]");
		});
	}
	
	@Test
	public void paramsCanBeBoundFromAnObject() throws Exception {
		
		Employee toCreate = new Employee();
		LocalDate hiredAt = LocalDate.now();
		toCreate.hired = hiredAt;
		toCreate.name = "tester-pojo";
		toCreate.salary = 20000;
		toCreate.type = Type.HOURLY;
		db.getSQLPlus().transact(conn -> {
			
			conn.createQuery("insert into employee(type, name, hired, salary) values (:type, :name, :hired, :salary)")
			    .bind(toCreate)
			    .executeUpdate();
		});
		
		String[] actualRow = db.query("select type, name, hired, salary from employee")[0];
		String[] expectRow = { Type.HOURLY.name(), "tester-pojo",  hiredAt.toString(), "20000" };
		assertArrayEquals(expectRow, actualRow);
	}

	
	@Test
	public void nullParamsInBoundObjectSetsValuesToNull() throws Exception {
		
		Employee toCreate = new Employee();
		toCreate.type = Type.HOURLY;
		toCreate.name = "tester-pojo";
		
		db.getSQLPlus().transact(conn -> {
			
			conn.createQuery("insert into employee(type, name, hired, salary) values (:type, :name, :hired, :salary)")
			    .bind(toCreate)
			    .executeUpdate();
		});
		
		String[] actualRow = db.query("select type, name, hired, salary from employee")[0];
		String[] expectRow = { Type.HOURLY.name(), "tester-pojo",  null, null };
		assertArrayEquals(expectRow, actualRow);
		assertArrayEquals(expectRow, actualRow);
	}
	
	public static class EmployeeMissingBindParam {
		public Type type;
		public String name;
		public Integer salary;
	}
	
	@Test
	public void bindFailsIfNoMemberExistsForParamLabel() throws Exception {
		
		EmployeeMissingBindParam toCreate = new EmployeeMissingBindParam();
		toCreate.name = "tester-pojo";
		toCreate.salary = 20000;
		toCreate.type = Type.HOURLY;
		
		db.getSQLPlus().transact(conn -> {
			assertThrows(() -> {
				conn.createQuery("insert into employee(hired, type, name, salary) values (:hired, :type, :name, :salary)").bind(toCreate);
			}, ReflectionException.class);
		});
	}
	
	@Test
	public void executeUpdateWillReturnAListOfGeneratedKeys() throws Exception {
		db.batch("insert into employee(type, name, hired, salary) values ('SALARY', 'tester-1', '2015-01-01', 20500)");
		
		db.getSQLPlus().transact(conn -> {
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
	public void dataIsNotPersistedIfAnErrorIsThrownBeforeCommit() throws Exception {
		
		class TransactionException extends RuntimeException {}
		
		try {
			db.getSQLPlus().transact(conn -> {
				conn.createQuery("insert into employee(type, name, hired, salary) values ('SALARY', 'tester-1', '2015-01-01', 20500)").executeUpdate();
				conn.createQuery("insert into employee(type, name, hired, salary) values ('SALARY', 'tester-2', '2015-01-01', 20500)").executeUpdate();
				throw new TransactionException();
			});
			
		} catch (SQLRuntimeException e) {
			assertEquals(TransactionException.class, e.getCause().getClass()); // Make sure the error we got was from us throwing the transaction exception
			assertArrayEquals(new String[][]{}, db.query("select * from employee"));
		}
	}

	@Test
	public void deadlockIsNotEncounteredWhen2SeparateThreadsNeedEachOthersLockedTables() throws Exception {

		Thread addressOwner = new Thread(() -> {
			db.getSQLPlus().transact(sess -> {
				sess.createQuery("insert into address (street, city, state, zip) values('Maple Street', 'Othertown', 'MN', '12345')").executeUpdate();
				Thread.sleep(500);
				sess.createQuery("insert into employee(type, name, hired, salary, address_id) values ('SALARY', 'tester-2', '2015-01-01', 20500, 1)").executeUpdate();
			});
		});

		Thread employeeOwner = new Thread(() -> {
			db.getSQLPlus().transact(sess -> {
				sess.createQuery("insert into employee(type, name, hired, salary, address_id) values ('SALARY', 'tester-1', '2015-01-01', 20500, 1)").executeUpdate();
				Thread.sleep(500);
				sess.createQuery("insert into address (street, city, state, zip) values('Maple Street', 'Springfield', 'MN', '12345')").executeUpdate();
			});
		});

		addressOwner.start();
		employeeOwner.start();
		addressOwner.join();
		employeeOwner.join();
	}

	@Test
	public void singleScalarValueCanBeQueriedSuccessfully() throws Exception {
		db.batch("insert into address (street, city, state, zip) values ('street1', 'city1', 'state1', 'zip1')");
		String street = db.getSQLPlus().query(s -> s.createQuery("select street from address").getUniqueResultAs(String.class));
		assertEquals("street1", street);
	}

	@Test
	public void listsOfScalarValuesCanBeQueriedSuccessfully() throws Exception {

		db.batch(
			"insert into address (street, city, state, zip) values ('street1', 'city1', 'state1', 'zip1')",
			"insert into address (street, city, state, zip) values ('street2', 'city2', 'state2', 'zip2')",
			"insert into address (street, city, state, zip) values ('street3', 'city3', 'state3', 'zip3')",
			"insert into address (street, city, state, zip) values ('street4', 'city4', 'state4', 'zip4')"
		);

		List<String> streets = db.getSQLPlus().query(s -> s.createQuery("select street from address").fetchAs(String.class));
		assertEquals(Arrays.asList("street1", "street2", "street3", "street4"), streets);
	}

	@Test
	public void locksAreReleasedIfExceptionIsThrownBeforeQueryIsExecuted() throws Exception {
		
		try {
			db.getSQLPlus().transact(sess -> {
				sess.createQuery("insert into employee(type, name, hired, salary, address_id) values ('SALARY', 'tester-1', '2015-01-01', 20500, 1)");
				throw new RuntimeException("test-exception!");
			});
		} catch (Exception e) {
			assertEquals(e.getCause().getMessage(), "test-exception!");
			db.batch("insert into employee(type, name, hired, salary, address_id) values ('SALARY', 'tester-1', '2015-01-01', 20500, 1)");
		}
		
	}
	
	@Test
	public void flushWillWriteDataToDatabase() throws Exception {
		
		String[][] actual = db.getSQLPlus().query(sess -> {
			sess.createQuery("insert into employee(type, name, hired, salary, address_id) values ('SALARY', 'tester-1', '2015-01-01', 20500, 1)").executeUpdate();
			sess.flush();
			return db.query("select type, name, hired, salary from employee");
		});
		
		String[][] expect = {{ "SALARY", "tester-1", "2015-01-01", "20500" }};
		assertArrayEquals(expect, actual);
	}
	
}
