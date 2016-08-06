import kdbc.execute
import kdbc.query
import kdbc.transaction
import kdbc.update
import org.h2.jdbcx.JdbcDataSource
import org.junit.Assert
import org.junit.Assert.assertEquals
import org.junit.Test
import java.sql.ResultSet
import java.sql.SQLException
import javax.sql.DataSource

data class Customer(var id: Int? = null, var name: String? = null) {
    constructor(rs: ResultSet) : this(rs.getInt("id"), rs.getString("name"))
}

class QueryTests {
    companion object {
        private val db: DataSource

        init {
            db = JdbcDataSource().apply {
                setURL("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1")
                execute { "CREATE TABLE customers (id integer not null primary key auto_increment, name text)" }
                execute { "INSERT INTO customers (name) VALUES ('John')" }
                execute { "INSERT INTO customers (name) VALUES ('Jill')" }
            }
        }
    }

    fun getCustomerById(id: Int): Customer = db.query {
        "SELECT * FROM customers WHERE id = ${p(id)}"
    } single {
        Customer(getInt("id"), getString("name"))
    }

    @Test
    fun queryTest() {
        val john = getCustomerById(1)
        assertEquals(1, john.id)
        assertEquals("John", john.name)
    }

    @Test
    fun updateTest() {
        val id = 1
        val name = "Johnnie"

        val updateCount = db.update {
            """UPDATE customers SET
            name = ${name.q}
            WHERE id = ${id.q}
            """
        }
        assertEquals(1, updateCount)

        val updatedName = getCustomerById(1).name

        assertEquals("Johnnie", updatedName)
    }

    @Test
    fun insertTest() {
        val c = Customer(name = "Jane")

        db.execute {
            generatedKeys {
                c.id = getInt(1)
            }
            "INSERT INTO customers (name) VALUES (${p(c.name)})"
        }

        val id = c.id!!
        println("Generated id was $id")
        val jane = getCustomerById(id)

        assertEquals("Customer(id=$id, name=Jane)", jane.toString())
    }

    @Test(expected = SQLException::class)
    fun deleteTest() {
        val id = 1
        db.update {
            "DELETE FROM customers WHERE id = ${p(id)}"
        }
        getCustomerById(1)
    }

    @Test
    fun resultSetTest() {
        db.query {
            "SELECT * FROM customers"
        } execute {
            while (resultSet.next()) println(resultSet.getString("name"))
        }
    }

    @Test
    fun sequenceTest() {
        val seq = db.query { "SELECT * FROM customers" } sequence { Customer(this) }
        seq.forEach {
            println("Found customer in seq: $it")
        }
    }

    @Test
    fun transaction_rollback() {
        try {
            db.transaction {
                execute { "UPDATE customers SET name = 'Blah' WHERE id = 1" }
                execute { "SELECT i_will_fail" }
            }
        } catch (ex: SQLException) {
            // Expected
            println("As expected")
        }
        val customer = db.query { "SELECT * FROM customers WHERE id = 1" } single { Customer(this) }
        Assert.assertNotEquals("Name should not be changed", "Blah", customer.name)
    }
}