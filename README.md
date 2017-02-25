# SQL DSL for Kotlin

KDBC provides type safe SQL queries for Kotlin. Features:

- 100% control of executed SQL
- Column selections and joins can be easily reused in multiple queries
- Explicit, but convenient O/R mapping
- Optional DDL generation

To query or update a table you need a `Table` object that represents the database table.

```kotlin
class CUSTOMER : Table() {
    val id by column<Int>()
    val name by column<String>()
    val zip by column<String>()
    val city by column<String>()
}
```

You will probably also have a corresponding domain object:

```kotlin
data class Customer(
    var id: Int,
    var name: String,
    var zip: String,
    var city: String
)
```


A Query is encapsulated in a class. Every table you mention in the
query needs an alias, defined by instantiating one or more `Table` instances.

You override the `rowItem` function to tell the query how to turn a result set
into your domain object. You don't need to work with the `ResultSet` directly,
the table aliases can be used to extract the sql column values in a type safe manner.

```kotlin
class SelectCustomer : Query<Customer> {
    val c = CUSTOMER()

    init {
        select(c.id, c.name, c.zip, c.city)
        from(c)
    }

    override fun rowItem() = Customer(c.id(), c.name(), c.zip(), c.city())
}
```

Notice how we call `alias.columnName()` to extract the value for the current column for the current row.

To execute the query you instantiate the query class and call one of the execute actions `first()`, `firstOrNull()`, `list()`.

```kotlin
val allCustomers = SelectCustomer().list()
```

The query code we wrote in the init block can be reused for multiple queries. Let's add a `byId()` function to our `SelectCustomer` query class:

```kotlin
fun byId(id: Int) = first {
    where {
        c.id `=` id
    }
}
```

We use the table alias `c` to construct the SQL `WHERE c.id = :id` in a type safe manner. We can now get a specific customer:

```kotlin
val customer = SelectCustomer().byId(42)
```

### Insert and Update

These query classes normally takes one or more input parameters, and can extend `Insert`, `Update` or `Delete` instead of `Query`. There really isn't
much of a difference, expect that the three first doesn't require a type parameter, like `Query` does.

The following `InsertCustomer` query takes a `customer` as a parameter, sets up a customer table alias and sets the name column to the
name property of the input `Customer` object.

```kotlin
class InsertCustomer(customer: Customer) : Insert() {
    val c = CUSTOMER()

    init {
        insert(c) {
            c.name `=` customer.name
        }
        generatedKeys {
            customer.id = getInt(1)
        }
    }
}
```

The insert returns a generated key for the `id` column. This is the first and only generated key, and we assign it to the `id` property of the input `Customer` object inside the `generatedKeys` block.
This block is consulted after the insert is executed:

```kotlin
InsertCustomer(customer).execute()
```

## Joins

Let's do a join! We'll introduce a `STATE` table and `State` domain object:

```kotlin
class STATE : Table() {
    val id by column<UUID>()
    val code by column<String>()
    val name by column<String>()
}

data class State(
    var id: UUID,
    var code: String,
    var name: String
)
```

We modify our Customer to include a state:

```kotlin
data class Customer(
    var id: Int,
    var name: String,
    var zip: String,
    var city: String,
    var state: State
)

class CUSTOMER : Table() {
    val id by column<Int>()
    val name by column<String>()
    val zip by column<String>()
    val city by column<String>()
    val state by column<UUID>()
}
```

Let's modify our `SelectCustomer` query so it joins `State` and populates the complete `Customer`
together with the `State`. Notice that since we want all columns in both tables, we just
mention the alias once instead of mentioning all the columns.

```kotlin
class SelectCustomer : Query<Customer> {
    val c = CUSTOMER()
    val s = STATE()

    init {
        select(c, s)
        from(c)
        join (s) on {
            s.id `=` c.state
        }
    }

    override fun rowItem() {
        val state = State(s.id(), s.code(), s.name())
        return Customer(c.id(), c.name(), c.zip(), c.city(), state)
    }
}
```

If you use `State` and/or `Customer` from other queries as well, consider
creating a secondary constructor that accepts the table object. That way the `rowItem` function
would look like:

```kotlin
override fun rowItem() = Customer(c, State(s))
```

This example showcases some of the corner stones of KDBC:

*You are 100% in control of what is fetched from your database, and you
construct your domain objects explicitly.*

## Custom column definitions

Let's revisit the first column we made, the `ID` property of our `CUSTOMER` table object:

```kotlin
class CUSTOMER : Table() {
    val id by column<Int>()
}
```

We have seen that the `Table` objects can retrieve values from our `ResultSet`, but how exactly does it work?

The `column()` delegate function above can take an optional `getter` function that tells KDBC how to extract
the column value for a given `ResultSet` object. The `getter` function operates on a `ResultSet` and is passed
the column name. Therefore, the `ID` column could also have been constructed like this:

```kotlin
val id by column { getString(it) }
```

`getString()` operates on a `ResultSet` and `it` represents the column name.
When you don't supply a `getter` function, KDBC tries to do the right thing by using the `getXXX` function
of the `ResultSet` class, based on the type of the `column`. For example, a `column<Int>()` will
do `getInt(it)` and a `column<String>()` will do `getString(it)`. There are defaults for all known
SQL data types, but you can easily call any function on the `ResultSet` object if you have a custom
requirement.

## Dynamic queries

Some times you want to pass multiple parameters to a search function and some of them might be nullable.

Consider the following function that can search for customers with a certain name, and optionally of atleast a given age.

```kotlin
fun search(name: String, minAge: Int?) = list {
    where {
        upper(c.name) like upper("%$name%")
        if (minAge != null) {
            and {
                c.age gte minAge
            }
        }
    }
}

```
> Yes, `name` is parameterized in the underlying prepared statement. SQL injection is not welcome here! :)

## DDL

The `column()` delegate also takes an optional `ddl` parameter. This is a string that can be used to
generate DDL, which can be automatically executed to create your database table.

The following example is taken from the test suite of KDBC:

```kotlin
class CUSTOMER : Table() {
    val id by column<Int>("integer not null primary key auto_increment")
    val name by column<String>("text")
}
```
> Customer definition with DDL

The DDL is then used when the test suite fires up:

```kotlin
val dataSource = JdbcDataSource()
dataSource.setURL("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1")
KDBC.setDataSource(dataSource)
CUSTOMER().create()
```

A `DataSource` is generated and configured as the default data source via `KDBC.setDataSource()`. Then
we call `CUSTOMER().create()`, which generates the DDL and executes it to construct our table.

## Transactions

If the current connection has `autoCommit = true`, each query will be committed upon completion. This is the default for a manually created
`java.sql.Connection`. A connection pool may change this behavior. For example, a JavaEE application will control transactions according to
the JavaEE life cycle.

KDBC has means to manually control the transaction context as well. To run multiple queries in the same transactions, wrap them in a `transaction` block:

```kotlin
transaction {
    CreateCustomer(customer).execute()
    CreateOrder(order).execute()
}
```

To create a new connection that will participate in the same transaction, nest another `transaction` block inside the existing one.

The `transaction` block takes an optional `transactionType` parameter.

This is by default set to `TransactionType.REQUIRED` which indicates that the transaction can participate in an already active transaction or create it's own if no transaction is active.

Changing to TransactionType.REQUIRES_NEW will temporarily suspend any active transactions and resume them after the code inside the `transaction` block completes.

If no connection is specified for the queries inside the block, the connection retrieved for the first query executed inside the transaction block will be used for all subsequent queries.
