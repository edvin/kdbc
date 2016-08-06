# SQL DSL for Kotlin

KDBC is a simple, lightweight and elegant DSL for SQL Queries in Kotlin. The mini-framework
is centered around extension functions to the `java.sql.Connection` class, extending it
with powerful and easy to use, concise query and update capabilities.

### Usage examples

Let's get a connection to a database and create a Customer table with some hard coded data:

```kotlin
	val db = DriverManager.getConnection("jdbc:h2:mem:")
	
	db.use {
		execute { "CREATE TABLE customers (id integer not null primary key, name text)") }
		execute { "INSERT INTO customers VALUES (1, 'John')" }
		execute { "INSERT INTO customers VALUES (2, 'Jill')" }
	}
```

The query DSL is centered around the following workflow:

* Get a database connection and create an sql string using the `p` interpolator function to quote values
* Ask for a list or a single object
* Map the ResultSet to a domain object or simply use the ResultSet object as you wish

We want to work with domain objects, so we define a `Customer` class:

```kotlin
	data class Customer(val id: Int, val name: String)
```

Let's define a DAO function to query for customers by id and return a fully mapped `Customer`:

```kotlin
	fun getCustomerById(id: Int) = db {
		"SELECT * FROM customers WHERE id = ${p(id)}"
    } single {
        Customer(getInt("id"), getString("name"))
    }
```

The `query` function is an extension on `java.sql.Connection`. It takes a function that should create an SQL query string.
It supports interpolating variables via the `${p()}` parameter function. This function returns a `PreparedStatement` object. 
On this object you can either call `executeQuery` to access the ResultSet, or use any of the convenience functions `list`, `single` or
`first` that operates on the ResultSet and lets you map the result set to a domain object. In the example above,
the `id` and `name` columns are extracted and passed to the `Customer` data class constructor.
 
Let's update customer number 2 with a new name:

```kotlin
    val name = "Johnnie"
    val id = 1
	db.update { 
	    "UPDATE customers SET name = ${p(name)} WHERE id = ${p(id)}" 
	}
```
    
The above example is a short hand form of `db.query` and then calling `update`. Similar
shortcuts are available for `insert` and `delete` as well. You can even operate on a `DataSource` just like if it was
a `Connection`.

Let's delete customer number 1:

```kotlin
    val id = 1
    
	db.delete { 
	    "DELETE FROM customers WHERE id = ${p(id)}" 
	}
```

If you want to set a value that might be null or you want to explicitly set the type parameter from `java.sql.Types`,
 you add the type as the second parameter to `p`:
 
```kotlin
    function insertCustomer(customer: Customer) {
        db.insert {
            "INSERT INTO customers VALUES (${p(customer.id, Types.INTEGER})), ${p(customer.name})"
        }
    }
```

The `p` function can handle most data types, and it's easy to extend it with your custom types and mappings.

That really is all. Every aspect of the JDBC API is still available to you right inside the DSL.

Convert a `ResultSet` to a `Sequence<T>`:

```kotlin
    db.query { "SELECT * FROM customers" } sequence { Customer(this) }
```