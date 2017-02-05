package com.tyler.sqlplus.proxy;

import com.tyler.sqlplus.SQLPlus;
import com.tyler.sqlplus.annotation.*;
import com.tyler.sqlplus.annotation.SQLUpdate.ReturnInfo;
import com.tyler.sqlplus.exception.AnnotationConfigurationException;
import com.tyler.sqlplus.exception.SQLRuntimeException;
import com.tyler.sqlplus.base.AbstractDatabase.Address;
import com.tyler.sqlplus.base.DatabaseTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.tyler.sqlplus.base.SQLPlusTesting.assertThrows;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TransactionalServiceTest extends DatabaseTest {

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
		
		db.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Elm Street', 'Othertown', 'CA', '54321')",
			"insert into address (street, city, state, zip) values('Main Street', 'Bakersfield', 'CA', '54321')"
		);
		
		TransactionAwareService service = db.getSQLPlus().createService(TransactionAwareService.class);
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
			() -> db.getSQLPlus().createService(TransactionAwareServiceBadField.class),
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
		db.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Elm Street', 'Othertown', 'CA', '54321')",
			"insert into address (street, city, state, zip) values('Main Street', 'Bakersfield', 'CA', '54321')"
		);

		SupportedService service = db.getSQLPlus().createService(SupportedService.class);
		List<Address> addresses = service.getAddresses();
		assertEquals(3, addresses.size());
	}
	
	interface InterfaceService {
		
		@SQLQuery("select * from address")
		List<Address> getAddresses();
		
	}
	
	@Test
	public void proxyCanBeCreatedForInterface() throws Exception {
		db.batch("insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')");
		InterfaceService service = db.getSQLPlus().createService(InterfaceService.class);
		List<Address> addresses = service.getAddresses();
		assertEquals(1, addresses.size());
	}
	
	static abstract class QueryingService {

		@SQLQuery("select * from address")
		public abstract void voidQuery();
		
		@SQLQuery("select * from address where street = 'Main Street'")
		public abstract Address getMainStreet();
		
		@SQLQuery("select count(*) from address")
		public abstract int countAddress();
		
		@SQLQuery("select * from address")
		public abstract List<Address> getAddresses();
		
		@SQLQuery("select * from address where street = :street and state = :state")
		public abstract Address getAddress(@BindParam("street") String street, @BindParam("state") String city);

		@SQLQuery("select street from address")
		public abstract List<String> getAddressStreets();

		@SQLUpdate("insert into address (street, city, state, zip) values (:street, :city, :state, :zip)")
		public abstract void createAddress(@BindObject Address address);

		@SQLUpdate(value = "insert into address (address_id, street, city, state, zip) values (:addressId, :street, :city, :state, :zip)", keyQuery = "select 100")
		public abstract Integer createAddressWith100KeyProvider(@BindObject Address address);

		@SQLUpdate(value = "select field from blah", keyQuery = "select 1")
		public abstract void createObjWithKeyQueryPresentButGivenTypeHasNoKeyField(@BindObject String someStr);

		@SQLUpdate(
			value = "insert into address (street, city, state, zip) values (:street, :city, :state, :zip)",
			returnInfo = ReturnInfo.GENERATED_KEYS
		)
		public abstract Integer createAddressWithKey(@BindObject Address address);

		@SQLUpdate(
			value = "insert into address (street, city, state, zip) values (:street, :city, :state, :zip)",
			returnInfo = ReturnInfo.GENERATED_KEYS
		)
		public abstract List<Integer> createAddressesWithKeys(@BindObject Collection<Address> addresses);
		
		@SQLUpdate(
			value = "insert into address (street, city, state, zip) values (:street, :city, :state, :zip)",
			returnInfo = ReturnInfo.GENERATED_KEYS
		)
		public abstract List<Integer> createAddressesWithKeysVarargs(@BindObject Address... addresses);

		@SQLUpdate(
			value = "insert into address (street, city, state, zip) values (:street, :city, :state, :zip)",
			returnInfo = ReturnInfo.AFFECTED_ROWS
		)
		public abstract int[] createAddressesWithAffectedRowsArray(@BindObject Address... address);

		@SQLUpdate(
			value = "insert into address (street, city, state, zip) values (:street, :city, :state, :zip)",
			returnInfo = ReturnInfo.AFFECTED_ROWS
		)
		public abstract int createAddressesWithAffectedRowsSingleInt(@BindObject Address... address);

	}
	
	@Test
	public void queryOnVoidMethodThrowsException() throws Exception {
		QueryingService service = db.getSQLPlus().createService(QueryingService.class);
		assertThrows(
			() -> service.voidQuery(),
			SQLRuntimeException.class,
			AnnotationConfigurationException.class.getName() + ": @" + SQLQuery.class.getSimpleName() + " annotated method " + QueryingService.class.getDeclaredMethod("voidQuery") + " must declare a return type"
		);
	}
	
	@Test
	public void querySingleScalarValue() throws Exception {
		db.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Main Street', 'Bakersfield', 'CA', '54321')"
		);
		
		QueryingService service = db.getSQLPlus().createService(QueryingService.class);
		assertEquals(2, service.countAddress());
	}

	@Test
	public void queryScalarCollection() throws Exception {

		db.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Main Street', 'Bakersfield', 'CA', '54321')"
		);

		List<String> streets = db.getSQLPlus().createService(QueryingService.class).getAddressStreets();
		assertEquals(Arrays.asList("Maple Street", "Main Street"), streets);
	}

	@Test
	public void queryAnnotatedMethodPerformsQueryForSingleResultWithNoParams() throws Exception {
		
		db.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Main Street', 'Bakersfield', 'CA', '54321')"
		);
		
		QueryingService service = db.getSQLPlus().createService(QueryingService.class);
		Address address = service.getMainStreet();
		assertEquals("Main Street", address.street);
	}
	
	@Test
	public void queryAnnotatedMethodPerformsQueryForCollectionResultWithNoParams() throws Exception {
		
		db.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Main Street', 'Bakersfield', 'CA', '54321')"
		);
		
		QueryingService service = db.getSQLPlus().createService(QueryingService.class);
		List<Address> addresses = service.getAddresses();
		assertEquals("Maple Street", addresses.get(0).street);
		assertEquals("Main Street", addresses.get(1).street);
	}
	
	@Test
	public void queryAnnotatedMethodPerformsQueryWithBindParams() throws Exception {
		
		db.batch(
			"insert into address (street, city, state, zip) values('Maple Street', 'Anytown', 'MN', '12345')",
			"insert into address (street, city, state, zip) values('Main Street', 'Bakersfield', 'CA', '54321')",
			"insert into address (street, city, state, zip) values('Maple Street', 'Bakersfield', 'CA', '54321')"
		);
		
		QueryingService service = db.getSQLPlus().createService(QueryingService.class);
		Address address = service.getAddress("Maple Street", "CA");
		assertEquals("Maple Street", address.street);
		assertEquals("CA", address.state);
	}

	@Test
	public void updateMethodWithKeySQLShouldSetKeyUsingGivenKeySQL() throws Exception {

		Address add = new Address();
		add.city = "test-city";
		add.state = "test-state";
		add.street = "test-street";
		add.zip = "test-zip";

		QueryingService service = db.getSQLPlus().createService(QueryingService.class);
		service.createAddressWith100KeyProvider(add);

		String[][] expect = {{ "100", "test-city", "test-state", "test-street", "test-zip" }};
		String[][] actual = db.query("select address_id, city, state, street, zip from address");
		assertArrayEquals(expect, actual);
	}

	@Test
	public void updateMethodWithAnArgumentTypeThatDoesNotHaveAKeyFieldShouldThrowAnException() throws Exception {
		QueryingService service = db.getSQLPlus().createService(QueryingService.class);
		assertThrows(
			() -> service.createObjWithKeyQueryPresentButGivenTypeHasNoKeyField("asdf"),
			SQLRuntimeException.class,
			AnnotationConfigurationException.class.getName() + ": No @" + KeyField.class.getSimpleName() + " annotation found in " + String.class + " to bind a key value to"
		);
	}

	@Test
	public void updateMethodShouldPerformInsert() throws Exception {
		
		Address add = new Address();
		add.city = "test-city";
		add.state = "test-state";
		add.street = "test-street";
		add.zip = "test-zip";
		
		QueryingService service = db.getSQLPlus().createService(QueryingService.class);
		service.createAddress(add);
		
		String[][] expect = {{ "test-city", "test-state", "test-street", "test-zip" }};
		String[][] actual = db.query("select city, state, street, zip from address");
		assertArrayEquals(expect, actual);
	}
	
	@Test
	public void updateMethodShouldInsertAndReturnSingleKey() throws Exception {
		
		Address add = new Address();
		add.city = "test-city";
		add.state = "test-state";
		add.street = "test-street";
		add.zip = "test-zip";
		
		QueryingService service = db.getSQLPlus().createService(QueryingService.class);
		Integer key = service.createAddressWithKey(add);
		
		String[][] expect = {{ "test-city", "test-state", "test-street", "test-zip" }};
		String[][] actual = db.query("select city, state, street, zip from address");
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
		
		QueryingService service = db.getSQLPlus().createService(QueryingService.class);
		List<Integer> keys = service.createAddressesWithKeys(Arrays.asList(add1, add2));
		
		String[][] expect = {
			{ "test-city", "test-state", "test-street", "test-zip" },
			{ "test2-city", "test2-state", "test2-street", "test2-zip" }
		};
		String[][] actual = db.query("select city, state, street, zip from address");
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
		
		QueryingService service = db.getSQLPlus().createService(QueryingService.class);
		List<Integer> keys = service.createAddressesWithKeysVarargs(add1, add2);
		
		String[][] expect = {
			{ "test-city", "test-state", "test-street", "test-zip" },
			{ "test2-city", "test2-state", "test2-street", "test2-zip" }
		};
		String[][] actual = db.query("select city, state, street, zip from address");
		assertArrayEquals(expect, actual);
		assertEquals(new Integer(1), keys.get(0));
		assertEquals(new Integer(2), keys.get(1));
	}

	@Test
	public void updateShouldReturnArrayOfUpdateCountsWithArrayReturnType() throws Exception {

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

		QueryingService service = db.getSQLPlus().createService(QueryingService.class);
		int[] updates = service.createAddressesWithAffectedRowsArray(add1, add2);

		assertArrayEquals(updates, new int[]{ 1, 1} );
	}

	@Test
	public void updateShouldReturnTotalUpdateCountWithSingleIntegerReturnType() throws Exception {

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

		QueryingService service = db.getSQLPlus().createService(QueryingService.class);
		int updates = service.createAddressesWithAffectedRowsSingleInt(add1, add2);

		assertEquals(updates, 2);
	}

}
