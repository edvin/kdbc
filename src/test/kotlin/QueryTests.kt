import org.junit.After
import org.junit.Before
import java.sql.Connection
import java.sql.DriverManager

class QueryTests {
    lateinit private var connection: Connection

    @Before
    fun connect() {
        connection = DriverManager.getConnection("jdbc:h2:mem:")
    }

    @After
    fun disconnect() {
        connection.close()
    }

}