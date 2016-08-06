import kdbc.*
import org.h2.jdbcx.JdbcDataSource
import org.junit.Assert.assertEquals
import org.junit.Test
import java.sql.ResultSet
import java.sql.SQLException
import javax.sql.DataSource

data class Customer(val id: Int, val name: String) {
    constructor(rs: ResultSet) : this(rs.getInt("id"), rs.getString("name"))
}

class QueryTests {
    companion object {
        private val db: DataSource

        init {
            db = JdbcDataSource().apply {
                setURL("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1")
                use {
                    execute { "CREATE TABLE customers (id integer not null primary key, name text)" }
                    execute { "INSERT INTO customers VALUES (1, 'John')" }
                    execute { "INSERT INTO customers VALUES (2, 'Jill')" }
                }
            }
        }
    }

    fun getCustomerById(id: Int) = db {
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
        val c = Customer(3, "Jane")

        db.insert {
            "INSERT INTO customers VALUES (${p(c.id)}, ${p(c.name)})"
        }

        val jane = getCustomerById(3)

        assertEquals("Customer(id=3, name=Jane)", jane.toString())
    }

    @Test(expected = SQLException::class)
    fun deleteTest() {
        val id = 1
        db.delete {
            "DELETE FROM customers WHERE id = ${p(id)}"
        }
        getCustomerById(1)
    }

    @Test
    fun resultSetTest() {
        val conn = db.connection
        val rs = conn.query { "SELECT * FROM customers" }.executeQuery()
        while (rs.next()) println(rs.getString("name"))
        conn.close()
    }

    @Test
    fun sequenceTest() {
        db.query { "SELECT * FROM customers" } sequence { Customer(this) }
    }
}