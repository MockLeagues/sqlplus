package com.tyler.sqlplus.proxy;

import static com.tyler.sqlplus.test.SQLPlusTesting.assertThrows;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import com.tyler.sqlplus.SQLPlus;
import com.tyler.sqlplus.annotation.BindObject;
import com.tyler.sqlplus.annotation.BindParam;
import com.tyler.sqlplus.annotation.Database;
import com.tyler.sqlplus.annotation.DAOQuery;
import com.tyler.sqlplus.annotation.DAOUpdate;
import com.tyler.sqlplus.annotation.Transactional;
import com.tyler.sqlplus.exception.AnnotationConfigurationException;
import com.tyler.sqlplus.exception.SQLRuntimeException;
import com.tyler.sqlplus.rule.H2EmployeeDBRule;
import com.tyler.sqlplus.rule.H2EmployeeDBRule.Address;

public class TransactionalServiceTest {

	@Rule
	public H2EmployeeDBRule h2 = new H2EmployeeDBRule();
	
	static class TransactionAwareService {

		@Database
		private SQLPlus sqlplus;

		@Transactional
		public List<Address> getAddresses() {
			return sqlplus.getCurrentSession()
			              .createQuery("select address_id as \"addressId\", street as \"street\", state as \"state\", city as \"city\", zip as \"zip\" from address")
			              .fetchAs(Address.class);
		}
		
	}
	
	@Test
	public void testTransactionAwareServicesExecuteMethodsInTransaction() throws Exception {
		
		h2.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Elm Street', 'Othertown', 'CA', '54321')",
			"insert into address (street, city, state, zip) values('Main Street', 'Bakersfield', 'CA', '54321')"
		);
		
		TransactionAwareService service = h2.getSQLPlus().createService(TransactionAwareService.class);
		List<Address> addresses = service.getAddresses();
		assertEquals(3, addresses.size());
	}
	
	static class TransactionAwareServiceBadField {
		@Database
		private String notASession;
	}
	
	@Test
	public void testTransactionAwareServiceThrowsErrorIfInjectFieldTypeIsNotSqlPlus() throws Exception {
		assertThrows(
			() -> h2.getSQLPlus().createService(TransactionAwareServiceBadField.class),
			AnnotationConfigurationException.class,
			"@" + Database.class.getSimpleName() + " annotated field " + TransactionAwareServiceBadField.class.getDeclaredField("notASession") + " must be of type " + SQLPlus.class
		);
	}

	static class SupportedService extends TransactionalServiceSupport {

		@Transactional
		public List<Address> getAddresses() {
			return session().createQuery("select * from address").fetchAs(Address.class);
		}

	}

	@Test
	public void testServiceClassExtendingTransactionServiceSupport() throws Exception {
		h2.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Elm Street', 'Othertown', 'CA', '54321')",
			"insert into address (street, city, state, zip) values('Main Street', 'Bakersfield', 'CA', '54321')"
		);

		SupportedService service = h2.getSQLPlus().createService(SupportedService.class);
		List<Address> addresses = service.getAddresses();
		assertEquals(3, addresses.size());
	}
	
	static interface InterfaceService {
		
		@DAOQuery("select * from address")
		List<Address> getAddresses();
		
	}
	
	@Test
	public void proxyCanBeCreatedForInterface() throws Exception {
		h2.batch("insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')");
		InterfaceService service = h2.getSQLPlus().createService(InterfaceService.class);
		List<Address> addresses = service.getAddresses();
		assertEquals(1, addresses.size());
	}
	
	static abstract class QueryingService {

		@DAOQuery("select * from address")
		public abstract void voidQuery();
		
		@DAOQuery("select * from address where street = 'Main Street'")
		public abstract Address getMainStreet();
		
		@DAOQuery("select count(*) from address")
		public abstract int countAddress();
		
		@DAOQuery("select * from address")
		public abstract List<Address> getAddresses();
		
		@DAOQuery("select * from address where street = :street and state = :state")
		public abstract Address getAddress(@BindParam("street") String street, @BindParam("state") String city);
		
		@DAOUpdate("insert into address (street, city, state, zip) values (:street, :city, :state, :zip)")
		public abstract void createAddress(@BindObject Address address);
		
		@DAOUpdate(
			value = "insert into address (street, city, state, zip) values (:street, :city, :state, :zip)",
			returnKeys = true
		)
		public abstract Integer createAddressWithKey(@BindObject Address address);
		
		@DAOUpdate(
			value = "insert into address (street, city, state, zip) values (:street, :city, :state, :zip)",
			returnKeys = true
		)
		public abstract List<Integer> createAddressesWithKeys(@BindObject Collection<Address> addresses);
		
		@DAOUpdate(
			value = "insert into address (street, city, state, zip) values (:street, :city, :state, :zip)",
			returnKeys = true
		)
		public abstract List<Integer> createAddressesWithKeysVarargs(@BindObject Address... addresses);
		
	}
	
	@Test
	public void queryOnVoidMethodThrowsException() throws Exception {
		QueryingService service = h2.getSQLPlus().createService(QueryingService.class);
		assertThrows(
			() -> service.voidQuery(),
			SQLRuntimeException.class,
			AnnotationConfigurationException.class.getName() + ": @" + DAOQuery.class.getSimpleName() + " annotated method " + QueryingService.class.getDeclaredMethod("voidQuery") + " must declare a return type"
		);
	}
	
	@Test
	public void queryScalarValue() throws Exception {
		h2.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Main Street', 'Bakersfield', 'CA', '54321')"
		);
		
		QueryingService service = h2.getSQLPlus().createService(QueryingService.class);
		assertEquals(2, service.countAddress());
	}
	
	@Test
	public void queryAnnotatedMethodPerformsQueryForSingleResultWithNoParams() throws Exception {
		
		h2.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Main Street', 'Bakersfield', 'CA', '54321')"
		);
		
		QueryingService service = h2.getSQLPlus().createService(QueryingService.class);
		Address address = service.getMainStreet();
		assertEquals("Main Street", address.street);
	}
	
	@Test
	public void queryAnnotatedMethodPerformsQueryForCollectionResultWithNoParams() throws Exception {
		
		h2.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Main Street', 'Bakersfield', 'CA', '54321')"
		);
		
		QueryingService service = h2.getSQLPlus().createService(QueryingService.class);
		List<Address> addresses = service.getAddresses();
		assertEquals("Maple Street", addresses.get(0).street);
		assertEquals("Main Street", addresses.get(1).street);
	}
	
	@Test
	public void queryAnnotatedMethodPerformsQueryWithBindParams() throws Exception {
		
		h2.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Main Street', 'Bakersfield', 'CA', '54321')",
			"insert into address (street, city, state, zip) values('Maple Street', 'Bakersfield', 'CA', '54321')"
		);
		
		QueryingService service = h2.getSQLPlus().createService(QueryingService.class);
		Address address = service.getAddress("Maple Street", "CA");
		assertEquals("Maple Street", address.street);
		assertEquals("CA", address.state);
	}

	@Test
	public void updateMethodShouldPerformInsert() throws Exception {
		
		Address add = new Address();
		add.city = "test-city";
		add.state = "test-state";
		add.street = "test-street";
		add.zip = "test-zip";
		
		QueryingService service = h2.getSQLPlus().createService(QueryingService.class);
		service.createAddress(add);
		
		String[][] expect = {{ "test-city", "test-state", "test-street", "test-zip" }};
		String[][] actual = h2.query("select city, state, street, zip from address");
		assertArrayEquals(expect, actual);
	}
	
	@Test
	public void updateMethodShouldInsertAndReturnSingleKey() throws Exception {
		
		Address add = new Address();
		add.city = "test-city";
		add.state = "test-state";
		add.street = "test-street";
		add.zip = "test-zip";
		
		QueryingService service = h2.getSQLPlus().createService(QueryingService.class);
		Integer key = service.createAddressWithKey(add);
		
		String[][] expect = {{ "test-city", "test-state", "test-street", "test-zip" }};
		String[][] actual = h2.query("select city, state, street, zip from address");
		assertArrayEquals(expect, actual);
		assertEquals(new Integer(1), key);
	}
	
	@Test
	public void updateMethodShouldInsertAndReturnMultipleKeysWhenCollectionGiven() throws Exception {
		
		Address add1 = new Address();
		add1.city = "test-city";
		add1.state = "test-state";
		add1.street = "test-street";
		add1.zip = "test-zip";
		
		Address add2 = new Address();
		add2.city = "test2-city";
		add2.state = "test2-state";
		add2.street = "test2-street";
		add2.zip = "test2-zip";
		
		QueryingService service = h2.getSQLPlus().createService(QueryingService.class);
		List<Integer> keys = service.createAddressesWithKeys(Arrays.asList(add1, add2));
		
		String[][] expect = {
			{ "test-city", "test-state", "test-street", "test-zip" },
			{ "test2-city", "test2-state", "test2-street", "test2-zip" }
		};
		String[][] actual = h2.query("select city, state, street, zip from address");
		assertArrayEquals(expect, actual);
		assertEquals(new Integer(1), keys.get(0));
		assertEquals(new Integer(2), keys.get(1));
	}
	
	@Test
	public void updateMethodShouldInsertAndReturnMultipleKeysWhenVarargsGiven() throws Exception {
		
		Address add1 = new Address();
		add1.city = "test-city";
		add1.state = "test-state";
		add1.street = "test-street";
		add1.zip = "test-zip";
		
		Address add2 = new Address();
		add2.city = "test2-city";
		add2.state = "test2-state";
		add2.street = "test2-street";
		add2.zip = "test2-zip";
		
		QueryingService service = h2.getSQLPlus().createService(QueryingService.class);
		List<Integer> keys = service.createAddressesWithKeysVarargs(add1, add2);
		
		String[][] expect = {
			{ "test-city", "test-state", "test-street", "test-zip" },
			{ "test2-city", "test2-state", "test2-street", "test2-zip" }
		};
		String[][] actual = h2.query("select city, state, street, zip from address");
		assertArrayEquals(expect, actual);
		assertEquals(new Integer(1), keys.get(0));
		assertEquals(new Integer(2), keys.get(1));
	}
	
}
