import kdbc.*
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import kotlin.test.assertEquals

data class Customer(val id: Int, val name: String)

class QueryTests {
    lateinit private var db: Connection

    @Before
    fun connect() {
        db = DriverManager.getConnection("jdbc:h2:mem:")

        with(db) {
            execute("CREATE TABLE customers (id integer not null primary key, name text)")
            execute("INSERT INTO customers VALUES (1, 'John')")
            execute("INSERT INTO customers VALUES (2, 'Jill')")
        }
    }

    @After
    fun disconnect() {
        db.close()
    }

    @Test
    fun queryTest() {
        val john = db.query("SELECT * FROM customers WHERE id = :id") {
            param("id", 1)
        } single {
            Customer(getInt("id"), getString("name"))
        }

        assertEquals(1, john.id)
        assertEquals("John", john.name)
    }

    @Test
    fun updateTest() {
        val updateCount = db.update("UPDATE customers SET name = :name WHERE id = :id") {
            param("name", "Johnnie")
            param("id", 1)
        }

        assertEquals(1, updateCount)

        val updatedName = db.query("SELECT name FROM customers WHERE id = :id") {
            param("id", 1)
        } single {
            getString("name")
        }

        assertEquals("Johnnie", updatedName)
    }

    @Test
    fun insertTest() {
        db.insert("INSERT INTO customers VALUES (:id, :name)") {
            param("id", 3)
            param("name", "Jane")
        }

        val jane = db.query("SELECT * FROM customers WHERE id = :id") {
            param("id", 3)
        } single {
            Customer(getInt("id"), getString("name"))
        }

        assertEquals("Customer(id=3, name=Jane)", jane.toString())
    }

    @Test(expected = SQLException::class)
    fun deleteTest() {
        db.delete("DELETE FROM customers WHERE id = :id") {
            param("id", 1)
        }

        db.query("SELECT * FROM customers WHERE id = :id") {
            param("id", 1)
        } single {
            Customer(getInt("id"), getString("name"))
        }
    }
}