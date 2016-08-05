import kdbc.*
import org.h2.jdbcx.JdbcDataSource
import org.junit.Assert.assertEquals
import org.junit.Test
import java.sql.SQLException

data class Customer(val id: Int, val name: String)

class QueryTests {
    companion object {
        private val ds: JdbcDataSource

        init {
            ds = JdbcDataSource().apply {
                setURL("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1")
                use {
                    execute("CREATE TABLE customers (id integer not null primary key, name text)")
                    execute("INSERT INTO customers VALUES (1, 'John')")
                    execute("INSERT INTO customers VALUES (2, 'Jill')")
                }
            }
        }
    }

    fun getCustomerById(id: Int): Customer = ds.use {
        query("SELECT * FROM customers WHERE id = :id") {
            param("id", id)
        } single {
            Customer(getInt("id"), getString("name"))
        }
    }

    @Test
    fun queryTest() {
        val john = getCustomerById(1)
        assertEquals(1, john.id)
        assertEquals("John", john.name)
    }

    @Test
    fun updateTest() {
        val updateCount = ds.use {
            update("UPDATE customers SET name = :name WHERE id = :id") {
                param("name", "Johnnie")
                param("id", 1)
            }
        }

        assertEquals(1, updateCount)

        val updatedName = getCustomerById(1).name

        assertEquals("Johnnie", updatedName)
    }

    @Test
    fun insertTest() {
        ds.use {
            insert("INSERT INTO customers VALUES (:id, :name)") {
                param("id", 3)
                param("name", "Jane")
            }
        }

        val jane = getCustomerById(3)

        assertEquals("Customer(id=3, name=Jane)", jane.toString())
    }

    @Test(expected = SQLException::class)
    fun deleteTest() {
        ds.use {
            delete("DELETE FROM customers WHERE id = :id") {
                param("id", 1)
            }
        }
        getCustomerById(1)
    }

    @Test
    fun resultSetTest() {
        val conn = ds.connection
        val rs = conn.query("SELECT * FROM customers").executeQuery()
        while (rs.next()) println(rs.getString("name"))
        conn.close()
    }
}