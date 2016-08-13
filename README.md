# Overview
sqlplus is a developer-friendly utility library for working with JDBC. It does not implement ORM features such as SQL generation or automatic cascading; rather, it provides a clean and relatively low-level layer of abstraction for managing persistence in your application.

# Why another database library?
Why make another database utility library, when we already have things like JDBI or MyBatis? True enough, there are already great libraries out there that get the job done, but there were 2 main reasons I went ahead and created sqplus anyways:
* Database persistence is a topic that naturally interests me. I attempted to create my own ORM awhile back but quickly realized I bit off way more than I could chew, so this is my middle ground
* I wanted to create a JDBC library which leveraged the enormous power of Java 8's functional features

# Basic Usage

The sqlplus library follows the same workflow as most other JDBC libraries. You first instantiate a sqlplus object, which you then use to open up transactions against a database connection. However, since you're a developer, you didn't come here to read documentation - you want to see code! Here is a simple example

```java
SqlPlus sqlPlus = new SqlPlus("dbUrl", "user", "password");
List<Map<String, Object>> results = sqlPlus.query(session -> {
	return session.createQuery("select * from widgets").fetch());
});
```

This method follows the so-called 'loaner' pattern: the method 'loans' you a session to use for your work without requiring all the ceremony we've all been through 100 times when connecting to a database.

The companion to the query method is the open method, which works exactly the same but is not declared to return anything:

```java
SqlPlus sqlPlus = new SqlPlus("dbUrl", "user", "password");
sqlPlus.open(session -> {
	session.createQuery("insert into widget (name, color) values ('hoozit', 'fire-red')").executeUpdate();
});
```

Of course, we also need the ability to execute atomic work inside of a transaction:

```java
SqlPlus sqlPlus = new SqlPlus("dbUrl", "user", "password");
sqlPlus.transact(session -> {
	List<Integer> returnedKeys = session.createQuery("insert into widget (name, color) values ('hoozit', 'fire-red')").executeUpdate(Integer.class);
	Integer generatedKey = returnedKeys.get(0);
	session.createQuery("insert into widget_audit (widget_id, execution_time, action) values (:id, now(), 'create')")
	       .setParameter("id", generatedKey)
	       .executeUpdate();
});
```

These 3 methods (query, open, and transact) and their respective use-cases (execute value-returning work, execute arbitrary work, execute atomic work inside a transaction) are the primary APIs into the sqlplus library. They take care of obtaining connections and executing rollbacks if exceptions occur so all you need to worry about is the work you need to do with your session.

Note that all sqplus exceptions extend RuntimeException, so you don't have to worry about annoying boilerplate try-catch blocks which munch or re-brand the 100s of SQLExceptions that are potentially thrown from simple, everyday JDBC method calls. This is typically the convention for exceptions which are non-recoverable; if an error occurs while interacting with the database, there really isn't much you could do in code to try and recover from it without re-gathering user input from scratch, implementing stronger validation or fixing your query syntax. Therefore, it doesn't make much sense to force you to handle the error.

## POJO mapping support

sqlplus provides features for automatically mapping result sets to lists of plain-old-java-objects (POJOs). This is done using a functional, streaming approach, which creates literally unlimited possibilities in how you can manipulate your data.

The workflow for querying objects is very simple:

```java
SqlPlus sqlPlus = new SqlPlus("dbUrl", "user", "password");
List<Widget> allWidgets = sqlPlus.query(session -> session.createQuery("select * from widget").fetchAs(Widget.class));
```

By default, sqlplus will attempt to map the column names present in the result set directly to the class's field names. If you need additional customization for how this mapping is done, you have 3 main options to choose from:
* Specify custom field mappings on the query object before execution
* Specify concrete field aliases in 'as' clauses in your SQL
* Configure sqlplus to use underscore to camel case conversion. This will convert a database field name like 'date_modified' to the equivalent camel-cased 'dateModified' when trying to find the appropriate field to set on a POJO

The previous example fetched the entire list into memory at once. What if you had millions of widgets? You'd blow your memory in no time! The solution is to STREAM over the results using Java 8's streaming API:

```java
SqlPlus sqlPlus = new SqlPlus("dbUrl", "user", "password");
sqlPlus.open(session -> {
  session.createQuery("select * from widget").streamAs(Widget.class).forEach(widget -> {
    // Do something with your widget, ensuring there is only 1 object in memory at a time
  });
});
```

However, we can do even better. Usually when you have a large number of results, you'd prefer to process them in batch:

```java
SqlPlus sqlPlus = new SqlPlus("dbUrl", "user", "password");
sqlPlus.open(session -> {
  session.createQuery("select * from widget").batchProcess(Widget.class, 1000, batchOf1000Widgets -> {
    // Consume widgets in batches of 1000
  });
});
```

Keep in mind that since sqplus allows you to create a plain object stream over the query result set, you can perform ANY sort of map-reduce operations on the resulting collection. This is particularly useful if you have some sort of data manipulation you want to perform on your results which would be difficult or impossible to express directly at the SQL level, such as a group by operation:

```java
SqlPlus sqlPlus = new SqlPlus("dbUrl", "user", "password");
Map<String, List<Widget>> widgetsByColor = sqlPlus.query(session -> {
  return session.createQuery("select * from widget").streamAs(Widget.class).collect(groupingBy(Widget::getColor));
});
```

# Working with collections

In any serious application, you'll find yourself working with entities which have relations to one or multiple other entities. sqlplus solves this problem via a lazy-loading solution which will load related entities on-demand (i.e. the first time they are accessed via a getter method inside of a session).

Lazy-loaded fields are specified with a @LoadQuery annotation on either the field itself or a specific method in the class (typically the getter) which specifies the sql used to load them. Any parameters referenced in the load query are bound to the enclosing object. In the following example, this means the 'orderId' parameter will be bound to the value of the orderId field of the order object which caused the related collection to be loaded:

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

SqlPlus sqlPlus = new SqlPlus("dbUrl", "user", "password");
sqlPlus.open(session -> {
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

In this case, the load query is configured explicitly to load data into the "comments" field. Note that by default, sqlplus will try to infer the field name from the method name if no field value is given by removing the 'get' prefix from the method name and taking the resulting string with the first character in lower-case. In this example, this would have produced an inferred field name of 'orderComments'.

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

SqlPlus sqlPlus = new SqlPlus("dbUrl", "user", "password");
sqlPlus.open(session -> {
	List<Order> myOrders = session.createQuery("select * from order o where o.issuer = :name")
	                              .setParameter("name", "twill")
	                              .fetchAs(Order.class);
										  
	Order myFirstOrder = myOrders.get(0);
	OrderComment commentABC123 = myFirstOrder.getCommentsById().get("ABC123");
});
```

Note that if you make a call to a lazy-loading getter method outside of a session without having loaded its associated data, an exception will be thrown.

# Creating service classes

sqlplus provides a feature to create service / data access object classes whose methods are wrapped inside of transactions. Consider this example:

```java
class WidgetDao {

	@ServiceSession
	private Session session;
	
	@Transactional
	public List<Widget> getWidgets(String color) {
		return session.createQuery("select * from widget where color = :color")
		              .setParameter("color", color)
		              .fetchAs(Widget.class);
	}

}

SqlPlus sqlPlus = new SqlPlus("dbUrl", "user", "password");
WidgetDao widgetDao = sqlPlus.createTransactionAwareService(WidgetDao.class);
List<Widget> redWidgets = widgetDao.getWidgets("red");
```

The service returned via the call to createTransactionAwareService() will have any invocations to methods annotated with @Transactional wrapped in a transaction, same as if you were to execute them inside of a call to sqlPlus.transact(). Additionally, the current active session will be bound to the first field in the service class found with the @ServiceSession annotation.

# Implementation Notes

Methods invoked inside of a call to one of the 'loaner' methods (query, transact or open) which themselves call a method that invokes query, transact or open will re-use the existing session instead of opening their own child session. Since this is a confusing concept to describe purely in text, here is an example:

```java
public static void main(String[] args) {

	//
	// Initialize DB driver, etc...
	//
	
	final SqlPlus sqlPlus = new SqlPlus("dbUrl", "user", "password");
	
	final Runnable childTask = () -> {
		sqlPlus.open(session -> {
			System.out.println("Session in child: " + session)
		});
	};
	
	
	sqlPlus.open(session -> {
		System.out.println("Session in parent: " + session)
		childTask.run();
	});
}

// This will print out something similar to the following:
// >> Session in parent: com.tyler.sqlplus.Session@497470ed
// >> Session in child: com.tyler.sqlplus.Session@497470ed
```

Notice how a session is opened in both the parent method invocation as well as the child method invocation. sqlplus handles this case by maintaining a concept of the 'current' active session for a sqlplus instance. This is done by associating an active session to the current running thread. This means that the first, and only the first call to do work against a session in a thread will actually cause a new session to be created, but any subsequent child calls made within the parent call context that also request to open work against a session will simply re-use the existing session bound to the current thread.