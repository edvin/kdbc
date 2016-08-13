import kdbc.KDBC
import kdbc.execute
import kdbc.transaction
import models.*
import org.h2.jdbcx.JdbcDataSource
import org.junit.Assert.*
import org.junit.Test
import java.sql.SQLException

class QueryTests {
    companion object {
        init {
            JdbcDataSource().apply {
                setURL("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1")
                execute("CREATE TABLE customer (id integer not null primary key auto_increment, name text)")
                execute("INSERT INTO customer (name) VALUES ('John')")
                execute("INSERT INTO customer (name) VALUES ('Jill')")

                KDBC.setConnectionFactory { connection }
            }
        }
    }

    @Test
    fun queryTest() {
        val john = SelectCustomer().byId(1)!!
        assertEquals(1, john.id)
        assertEquals("John", john.name)
    }

    @Test
    fun updateTest() {
        UpdateCustomer(Customer(1, "Johnnie")).execute()

        val updatedName = SelectCustomer().byId(1)?.name

        assertEquals("Johnnie", updatedName)
    }

    @Test
    fun insertTest() {
        val newCustomer = Customer(name = "Jane")

        InsertCustomer(newCustomer).execute()

        val fromDatabase = SelectCustomer().byId(newCustomer.id!!)

        assertEquals("Customer(id=${newCustomer.id}, name=Jane)", fromDatabase.toString())
    }

    @Test
    fun deleteTest() {
        DeleteCustomer(1)()
        assertNull(SelectCustomer().byId(1))
    }

    @Test
    fun adhocResultSetTest() {
        SelectCustomer().apply {
            execute()
            val rs = resultSet
            while (rs.next()) println(rs.getString("name"))
        }
    }

    @Test
    fun transaction_rollback() {
        try {
            transaction {
                DeleteCustomer(1).execute()
                throw SQLException("I'm naughty")
            }
        } catch (ex: SQLException) {

        }
        assertNotNull("Customer 1 should still be available", SelectCustomer().byId(1))
    }

}