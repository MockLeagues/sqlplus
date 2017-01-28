package com.tyler.sqlplus.proxy;

import static com.tyler.sqlplus.test.SqlPlusTesting.assertThrows;
import static org.junit.Assert.assertEquals;

import java.lang.reflect.ParameterizedType;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import com.tyler.sqlplus.SQLPlus;
import com.tyler.sqlplus.annotation.Bind;
import com.tyler.sqlplus.annotation.BindParam;
import com.tyler.sqlplus.annotation.SQLPlusInject;
import com.tyler.sqlplus.annotation.SQLPlusQuery;
import com.tyler.sqlplus.annotation.SQLPlusUpdate;
import com.tyler.sqlplus.annotation.Transactional;
import com.tyler.sqlplus.exception.ReflectionException;
import com.tyler.sqlplus.rule.H2EmployeeDBRule;
import com.tyler.sqlplus.rule.H2EmployeeDBRule.Address;

public class ProxyServiceTest {

	@Rule
	public H2EmployeeDBRule h2 = new H2EmployeeDBRule();
	
	static class TransactionAwareService {

		@SQLPlusInject
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
		@SQLPlusInject
		private String notASession;
	}
	
	@Test
	public void testTransactionAwareServiceThrowsErrorIfInjectFieldTypeIsNotSqlPlus() throws Exception {
		assertThrows(
			() -> h2.getSQLPlus().createService(TransactionAwareServiceBadField.class),
			ReflectionException.class,
			"@" + SQLPlusInject.class.getSimpleName() + " annotated field " + TransactionAwareServiceBadField.class.getDeclaredField("notASession") + " must be of type " + SQLPlus.class
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
	
	static abstract class QueryingService {

		@SQLPlusQuery("select * from address where street = 'Main Street'")
		public abstract Address getMainStreet();
		
		@SQLPlusQuery("select * from address")
		public abstract List<Address> getAddresses();
		
		@SQLPlusQuery("select * from address where street = :street and state = :state")
		public abstract Address getAddress(@BindParam("street") String street, @BindParam("state") String city);
		
		@SQLPlusUpdate("insert into address (street, city, state, zip) values (:street, :city, :state, :zip)")
		public abstract void createAddress(@Bind Address address);
		
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
	
	static class Foo {
		
		List<String> bar;
		
		String baz;
		
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println(Foo.class.getDeclaredField("bar").getGenericType() instanceof Class);
		System.out.println(Foo.class.getDeclaredField("bar").getType() instanceof Class);
		System.out.println(Foo.class.getDeclaredField("baz").getGenericType());
		System.out.println(Foo.class.getDeclaredField("baz").getType());
		
		ParameterizedType p = (ParameterizedType) Foo.class.getDeclaredField("bar").getGenericType();
		System.out.println(p.getRawType());
	}
	
}
