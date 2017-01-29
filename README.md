# Overview
SQLPlus is a developer-friendly utility library for working with JDBC. It does not implement ORM features such as SQL generation or automatic cascading; rather, it provides a clean and relatively low-level layer of abstraction for managing persistence in your application.

While there are already  a number of great libraries out there for this sort of thing (JDBI, MyBatis, etc.), this project aims to build a JDBC library which leverages the full power of Java 8's streaming features.

# Basic Usage

The SQLPlus library provides a very simple workflow. You first instantiate a ```SQLPlus``` object representing your data store, which you then use to open up transactions against a JDBC connection. Here is a simple example showing how you can query for a raw list of maps:

```java
SQLPlus sqlPlus = new SQLPlus("dbUrl", "user", "password");
List<Map<String, Object>> results = sqlPlus.query(session -> {
	return session.createQuery("select * from widgets").fetch();
});
```

For a section of code that does not return a value, use ```transact()``` instead of ```query()```:

```java
SQLPlus sqlPlus = new SQLPlus("dbUrl", "user", "password");
sqlPlus.transact(session -> {
	List<Integer> returnedKeys = session.createQuery("insert into widget (name, color) values ('hoozit', 'fire-red')").executeUpdate(Integer.class);
	Integer generatedKey = returnedKeys.get(0);
	session.createQuery("insert into widget_audit (widget_id, execution_time, action) values (:id, now(), 'create')")
	       .setParameter("id", generatedKey)
	       .executeUpdate();
});
```

All JDBC actions performed through SQLPlus are transactional. If for some reason you need to commit your current work in the middle of a transaction, you can call ```flush()``` on your session object:

```java
SQLPlus SQLPlus = new SQLPlus("dbUrl", "user", "password");
SQLPlus.transact(session -> {
	session.createQuery("insert into widget (name, color) values ('hoozit', 'fire-red')").executeUpdate();
	session.flush();
	// The fire-red hoozit is now persisted to the database
});
```

These 2 methods (```query()``` and ```transact()```) are the primary APIs into the SQLPlus library. They take care of obtaining connections and executing rollbacks if exceptions occur.

Note that all SQLPlus exceptions extend ```RuntimeException```, so you don't have to worry about annoying boilerplate try-catch blocks which munch or re-brand the 100s of SQLExceptions that are potentially thrown from simple, everyday JDBC method calls. This is typically the convention for exceptions which are non-recoverable; if an error occurs while interacting with the database, there really isn't much you could do in code to try and recover from it without re-gathering user input from scratch, implementing stronger validation or fixing your query syntax. Therefore, it doesn't make much sense to force you to handle the error.


# POJO mapping support
SQLPlus provides features for automatically mapping result sets to lists of plain-old-java-objects (POJOs). This is done using a functional, streaming approach, which creates literally unlimited possibilities in how you can manipulate your data.

The workflow for querying objects is very simple:

```java
SQLPlus sqlPlus = new SQLPlus("dbUrl", "user", "password");
List<Widget> allWidgets = sqlPlus.query(session -> session.createQuery("select * from widget").fetchAs(Widget.class));
```

By default, SQLPlus will attempt to map the column names present in the result set directly to the class's field names. If that fails, it will then try to see if there are any columns matching the result of converting the field name from camelcase to its underscore equivalent (for instance, 'myField' would convert to 'MY_FIELD'). If this fails, the field will remain null. If you need additional customization for how fields are mapped, you must specify concrete field aliases in 'as' clauses in your SQL.

The previous example fetched the entire list into memory at once. What if you had millions of widgets? You'd blow your memory in no time! The solution is to STREAM over the results using Java 8's streaming API:

```java
SQLPlus sqlPlus = new SQLPlus("dbUrl", "user", "password");
sqlPlus.transact(session -> {
  session.createQuery("select * from widget").streamAs(Widget.class).forEach(widget -> {
    // Do something with your widget, ensuring there is only 1 object in memory at a time
  });
});
```

However, we can do even better. When you have a large number of results, you may prefer to process them in batch:

```java
SQLPlus sqlPlus = new SQLPlus("dbUrl", "user", "password");
sqlPlus.transact(session -> {
  session.createQuery("select * from widget").batchProcess(Widget.class, 1000, batchOf1000Widgets -> {
    // Consume widgets in batches of 1000
  });
});
```

Keep in mind that since SQLPlus allows you to create a plain object stream over the query result set, you can perform ANY sort of map-reduce operations on the resulting collection. The following example demonstrates how you can group query results in memory:

```java
SQLPlus sqlPlus = new SQLPlus("dbUrl", "user", "password");
Map<String, List<Widget>> widgetsByColor = sqlPlus.query(session -> {
  return session.createQuery("select * from widget").streamAs(Widget.class).collect(groupingBy(Widget::getColor));
});
```

# Working with collections

In any serious application, you'll find yourself working with entities which have relations to one or multiple other entities. SQLPlus solves this problem via a lazy-loading solution which will load related entities on-demand (i.e. the first time they are accessed via a getter method inside of a session).

Lazy-loaded fields are specified with a @LoadQuery annotation on either the field itself or a specific method in the class (typically the getter) which specifies the SQL used to load them. Any parameters referenced in the load query are bound to the enclosing object. In the following example, this means the 'orderId' parameter will be bound to the value of the orderId field of the order object which caused the related collection to be loaded:

```java
class Order {

  private String orderId;
  
  private String issuer;
  
  private LocalDate dateCreated;
  
  @LoadQuery("select * from order_comment oc where oc.order_id = :orderId")
  private List<OrderComment> comments;

  public List<OrderComment> getComments() {
    return comments;
  }

}

SQLPlus sqlPlus = new SQLPlus("dbUrl", "user", "password");
sqlPlus.transact(session -> {
	List<Order> myOrders = session.createQuery("select * from order o where o.issuer = :name")
	                              .setParameter("name", "twill")
	                              .fetchAs(Order.class);
										  
	Order myFirstOrder = myOrders.get(0);
  
	// Order comments will be loaded on-demand on invocation of this method. Note that the 'orderId' parameter of the load query
	// will be bound to the value of 'myFirstOrder's orderId
	List<OrderComments> comments = myFirstOrder.getComments();
	
	// Do work with comments
  
});
```

The following example shows how you can specify the loading query directly on a getter method to cause a specific field to load on that method's invocation:

```java
class Order {

	private String orderId;
	
	private String issuer;
	
	private LocalDate dateCreated;

	private List<OrderComment> comments;
	
	@LoadQuery(
		value = "select * from order_comment oc where oc.order_id = :orderId",
		field = "comments"
	)
	public List<OrderComment> getOrderComments() {
		return comments;
	}

}
```

In this case, the load query is configured explicitly to load data into the "comments" field. By default, SQLPlus will try to infer the field name from the method name if no field value is given by removing the 'get' prefix from the method name and taking the resulting string with the first character in lower-case. In this example, this would have produced an inferred field name of 'orderComments'.

You may also load a collection into a map whose keys will be the values of a given field of the related class. In the following example, each order comment will be added to the map using the value of its 'orderCommentId' field as the key:

```java
class Order {

  private String orderId;
  
  private String issuer;
  
  private LocalDate dateCreated;
  
  @MapKey("orderCommentId")
  @LoadQuery("select * from order_comment oc where oc.order_id = :orderId")
  private Map<String, OrderComment> commentsById;

  public Map<String, OrderComment> getCommentsById() {
    return Collections.unmodifiableMap(commentsById);
  }

}

SQLPlus sqlPlus = new SQLPlus("dbUrl", "user", "password");
sqlPlus.transact(session -> {
	List<Order> myOrders = session.createQuery("select * from order o where o.issuer = :name")
	                              .setParameter("name", "twill")
	                              .fetchAs(Order.class);
										  
	Order myFirstOrder = myOrders.get(0);
	OrderComment commentABC123 = myFirstOrder.getCommentsById().get("ABC123");
});
```

Take caution that if you make a call to a lazy-loading getter method outside of a session without having loaded its associated data, an exception will be thrown.

# Creating service classes

SQLPlus provides a feature to create service / data access object classes whose methods are wrapped inside of transactions. Consider this example which defines a single method to query widgets by their color:

```java
class WidgetDao {
  
	@Database
	private SQLPlus sqlPlus;
	
	@Transactional
	public List<Widget> getWidgets(String color) {
		return sqlPlus.getCurrentSession()
		              .createQuery("select * from widget where color = :color")
		              .setParameter("color", color)
		              .fetchAs(Widget.class);
	}

}

SQLPlus sqlPlus = new SQLPlus("dbUrl", "user", "password");
WidgetDao widgetDao = sqlPlus.createService(WidgetDao.class);
List<Widget> redWidgets = widgetDao.getWidgets("red");
```

The service returned via the call to ```createService()``` will have any invocations to methods annotated with ```@Transactional``` wrapped in a transaction, same as if you were to execute them inside of a call to ```transact()```. To execute work against the current session wrapping the method, you can call ```sqlPlus.getCurrentSession()```, as shown, on the injected sqlPlus instance.

You can also annotate abstract methods or interface class methods to map them directly to SQL queries / updates. The following shows a basic service class which implements some of these features:

```java
abstract class WidgetService {

	@Database
	private SQLPlus sqlPlus;

	/**
	 * This is an alternate implementation of the getWidgets() method shown above which uses a more declarative, annotation-driven approach
	 */
	@DAOQuery("select * from widget where color = :color")
	public abstract List<Widget> getWidgets(@BindObjectParam("color") String color);
	 
	/**
	 * This method will bind all parameters in the given object to the annotated query, same as if you were to manually call the bind() method on a query
	 */
	@DAOUpdate("insert into widget(name, color) values (:name, :color)")
	public abstract void createWidget(@BindObject Widget widget);
	  
	/**
	 * You may also bind parameters in batches by passing an iterable collection or array of bind objects
	 */
	@DAOUpdate("insert into widget(name, color) values (:name, :color)")
	public abstract void createWidgets(@BindObject Collection<Widget> widget);
	  
	/**
	 * If you want to retrieve generated keys for an insert, you can specify a return info of GENERATED_KEYS:
	 */
	@DAOUpdate(value = "insert into widget(name, color) values (:name, :color)", returnInfo = ReturnInfo.GENERATED_KEYS)
	public abstract Integer createWidgetWithKey(@BindObject Widget widget);
	
	/**
	 * You can also retrieve the total update count by passing AFFECTED_ROWS for the return info:
	 */
	@DAOUpdate(value = "delete from widgets where color = :color", returnInfo = ReturnInfo.AFFECTED_ROWS)
	public abstract int deleteWidgets(@BindParam("color") String color);
	
	/**
	 * Of course, you still have the ability to execute a custom transactional method
	 */
	@Transactional
	public int countWidgets() {
		sqlPlus.getCurrentSession().createQuery("select count(*) from widget").getUniqueResultAs(Integer.class);
	}

}

SQLPlus sqlPlus = new SQLPlus("dbUrl", "user", "password");
WidgetService service = sqlPlus.createService(WidgetService.class);
int generatedKey = service.createWidget(new Widget("zing-zang", "light-blue"));
assert generatedKey == 1
assert service.countWidgets() == 1
// etc...
```

# Spring Integration

SQLPlus provides seamless integration with the spring dependency injection framework. This is most easily done by passing a data source bean as a constructor argument to a new SQLPlus bean. The following beans.xml shows what this might look like:

```xml

<beans
  xmlns="http://www.springframework.org/schema/beans"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xmlns:context="http://www.springframework.org/schema/context"
  xsi:schemaLocation="http://www.springframework.org/schema/beans
  http://www.springframework.org/schema/beans/spring-beans-2.5.xsd
  http://www.springframework.org/schema/context
  http://www.springframework.org/schema/context/spring-context-2.5.xsd">

	<!--
	Here, we create a data source bean using a very basic data source implementation provided by SQLPlus.
	It is recommended to use an application-server provided data source in production to take advantage
	of features such as connection pooling
	-->
	<bean id="dataSource" class="com.tyler.sqlPlus.BasicDataSource">
		<property name="url"         value="jdbc:h2:mem:db1;DB_CLOSE_DELAY=-1" />
		<property name="username"    value="sa" />
		<property name="password"    value="sa" />
		<property name="driverClass" value="org.h2.Driver" />
	</bean>

	<bean id="SQLPlus" class="com.tyler.sqlPlus.SQLPlus">
		<constructor-arg>
			<ref bean="dataSource" />
		</constructor-arg>
	</bean>

	<!-- A SQLPlus bean is now available for @Autowire injection -->
	<context:component-scan base-package="your.app.package" />

</beans>

```

# Implementation Notes

SQLPlus maintains the notion of the 'current' session. This is done using a thread local variable to allow specific sessions to be bound to the currently executing thread. This means if a transactional block of code attempts to invoke another transactional block of code in the same thread, that block will use the existing transaction from its parent method. Consider this example:

```java

//
// Initialize DB driver, etc...
//

final SQLPlus sqlPlus = new SQLPlus("dbUrl", "user", "password");

final Runnable childTask = () -> {
	sqlPlus.transact(session -> {
		System.out.println("Session in child: " + session)
	});
};


sqlPlus.transact(session -> {
	System.out.println("Session in parent: " + session)
	childTask.run();
});

// This will print out something similar to the following:
// >> Session in parent: com.tyler.sqlPlus.Session@497470ed
// >> Session in child: com.tyler.sqlPlus.Session@497470ed
```

Notice how a session is opened in both the parent method invocation as well as the child method invocation. However, because both methods execute within the same thread, they will reuse the existing transaction.