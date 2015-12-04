# JDBC DSL for Kotlin

KDBC is a simple, lightweight and elegant DSL for SQL Queries in Kotlin. The mini-framework
is centered around extension functions to the `java.sql.Connection` class, extending it
with powerful and easy to use, concise query and update capabilities.

### Usage examples

Let's get a connection to a database and create a Customer table with some hard coded data:

```kotlin
	val db = DriverManager.getConnection("jdbc:h2:mem:")
	
	with(db) {
		execute("CREATE TABLE customers (id integer not null primary key, name text)")
		execute("INSERT INTO customers VALUES (1, 'John')")
		execute("INSERT INTO customers VALUES (2, 'Jill')")
	}
```

The query DSL is centered around the following workflow:

* Get a database connection and execute a query
* Bind named parameters
* Ask for a list or a single object
* Map the ResultSet to a domain object or simply use the ResultSet object as you wish

We want to work with domain objects, so we define a `Customer` class:

```kotlin
	data class Customer(val id: Int, val name: String)
```

Let's define a DAO function to query for customers by id and return a fully mapped `Customer`:

```kotlin
	fun getCustomerById(id: Int): Customer = with(db) {
		query("SELECT * FROM customers WHERE id = :id") {
			param("id", id)
		} single {
			Customer(getInt("id"), getString("name"))
		}
	}
```

The `query` function is an extension on `java.sql.Connection`. It takes an SQL query string
that supports named parameters. The second parameter is an operation on the `ParameterizedStatement` object
where you can map values to the named parameters. Then the `ParameterizedStatement` is returned. On this object you
can either call `executeQuery` to access the ResultSet, or use any of the convenience functions `list`, `single` or
`first` that operates on the ResultSet and lets you map the result set to a domain object. In the example above,
the `id` and `name` columns are extracted and passed to the `Customer` data class constructor.
 
Let's update customer number 2 with a new name:

```kotlin
	db.update("UPDATE customers SET name = :name WHERE id = :id") {
		param("name", "Johnnie")
		param("id", 1)
	}
```
    
The above example is a short hand form of `db.query`, setting parameters and then calling `update`. Similar
shortcuts are available for `insert` and `delete` as well.

Let's delete customer number 1:

```kotlin
        db.delete("DELETE FROM customers WHERE id = :id") {
            param("id", 1)
        }
```

If you want to set a value that might be null or you want to explicitly set the type parameter from `java.sql.Types`,
 you add the type as the third parameter to `param`:
 
```kotlin
	db.insert("INSERT INTO customers VALUES (:id, :name)" {
		param("id", 4, Types.INTEGER)
		...
	}
```

That really is all. Every aspect of the JDBC API is still available to you right inside the DSL.