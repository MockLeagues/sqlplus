# sqlplus
Utility library for working with JDBC

## Why another database library?
Why make another database utility library, when we already have things like JDBI or MyBatis? 2 main reasons:
* For some reason, this is a topic that greatly interests me. I attempted to create my own ORM a la Hibernate awhile back but quickly realized I bit off way more than I could chew, so this is the perfect middle ground. I guess at the root of it all, this is something I made purely for fun and for the challenge
* I wanted to create a JDBC library which leveraged the power of Java 8's functional features. The ResultSet object is just screaming for Java 8's streaming features since it is, at its core, basically a collection

## Basic Usage

The sqlplus library follows the same workflow as most other JDBC libraries. You first instantiate a sqlplus object, which you then use to open up transactions against a database connection. However, since you're a developer, you didn't come here to read documentation - you want to see code! Here is a simple example

```java
SqlPlus sqlPlus = new SqlPlus("dbUrl", "user", "password");
List<Map<String, Object>> results = sqlPlus.query(session -> session.createQuery("select * from widgets").fetch());
```

Aren't lambdas great? The query method follows the so-called 'loaner' pattern which, thanks to Java 8 lambdas, is now painless to implement. The method 'loans' you a session to use for your work without requiring all the ceremony we've all been through 100 times when connecting to a database.

The companion to the query method is the open method, which works exactly the same but is not declared to return anything:

```java
SqlPlus sqlPlus = new SqlPlus("dbUrl", "user", "password");
sqlPlus.open(session -> {
	session.createQuery("insert into widget (name, color) values ('hoozit', 'fire-red')").executeUpdate();
});
```

Of course, we also need the ability to execute work inside of a transaction. No problem! Simply call transact:
```java
SqlPlus sqlPlus = new SqlPlus("dbUrl", "user", "password");
sqlPlus.transact(session -> {
  session.createQuery("insert into widget (name, color) values ('hoozit', 'fire-red')").executeUpdate());
  session.createQuery("insert into widget (name, color) values ('whatsit', 'sky-blue')").executeUpdate());
});
```

This workflow is probably best executed as a batch (another available feature), but it demonstrates the notion of a transaction well enough.

## POJO mapping support

sqlplus provides features for mapping result sets to lists of plain-old-java-objects (POJOs). This is where the magic of the library really resides in my opinion since it takes a functional, streaming approach to how this is done, creating literally unlimited possibilities in how you can manipulate your data.

The workflow for querying objects is simple enough:
```java
SqlPlus sqlPlus = new SqlPlus("dbUrl", "user", "password");
List<Widget> allWidgets = sqlPlus.query(session -> session.createQuery("select * from widget").fetchAs(Widget.class));
```

By default, sqlplus will attempt to map the column names present in the result set to the classe's field names. You may also specify custom field mappings on the query object before execution, or in 'as' clauses in your field selection list.

This, however, is only the tip of the iceberg. The previous example fetched the entire list into memory at once. What if you had millions of widgets? You'd blow your heap in no time! The solution is to STREAM over the results using Java 8's streaming API:
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

Keep in mind, since sqplus allows you to create a stream over the query result set, you can perform ANY sort of map-reduce operations on the resulting collection. This is particularly useful if you have some sort of data manipulation you want to perform on your results which would be difficult to express directly at the SQL level, such as a complex group by operation:
```java
SqlPlus sqlPlus = new SqlPlus("dbUrl", "user", "password");
Map<String, List<Widget>> widgetsByColor = sqlPlus.query(session -> {
  return session.createQuery("select * from widget").streamAs(Widget.class).collect(groupingBy(Widget::getColor));
});
```

## Working with collections

In any serious application, you'll find yourself working with entities which have relations to one or multiple other entities. sqlplus solves this problem via a lazy-loading solution which will load related entities on-demand (i.e. the first time they are accessed via a getter method inside of a session). If you've worked with Hibernate, this should sound very familiar; in fact, it works exactly the same!

Lazy-loaded relations are specified on the class member with a @LoadQuery annotation, which specifies the sql which will load the appropriate relation(s):
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
  
	// Order comments will be loaded on-demand on invocation of this method
	List<OrderComments> comments = myFirstOrder.getComments();
	
	// Do work with comments
  
});
```

Note that if you make a call to getComments() outside of a session, an exception will be thrown.

## Creating service classes

sqlplus provides a feature to create service classes whose methods are wrapped inside of transactions. Consider this example:
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

## Implementation Notes

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