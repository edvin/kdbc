import kdbc.KDBC
import kdbc.execute
import models.*
import org.h2.jdbcx.JdbcDataSource
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class QueryTests {
    companion object {
        init {
            JdbcDataSource().apply {
                setURL("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1")
                execute("CREATE TABLE customer (id integer not null primary key auto_increment, name text)")
                execute("INSERT INTO customer (name) VALUES ('John')")
                execute("INSERT INTO customer (name) VALUES ('Jill')")

                KDBC.DataSourceProvider = { this }
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
        val id = 1
        DeleteCustomer(id).execute()
        assertNull(SelectCustomer().byId(id))
    }

    @Test
    fun adhocResultSetTest() {
        SelectCustomer().apply {
            execute()
            val rs = resultSet
            while (rs.next()) println(rs.getString("name"))
        }
    }

//
//    @Test
//    fun sequenceTest() {
//        val seq = db.query { "SELECT * FROM customers" } sequence { Customer(this) }
//        seq.forEach {
//            println("Found customer in seq: $it")
//        }
//    }
//
//    @Test
//    fun transaction_rollback() {
//        try {
//            db.transaction {
//                execute { "UPDATE customers SET name = 'Blah' WHERE id = 1" }
//                execute { "SELECT i_will_fail" }
//            }
//        } catch (ex: SQLException) {
//            // Expected
//            println("As expected")
//        }
//        val customer = db.query { "SELECT * FROM customers WHERE id = 1" } single { Customer(this) }
//        Assert.assertNotEquals("Name should not be changed", "Blah", customer.name)
//    }

}