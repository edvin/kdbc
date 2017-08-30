package kdbc

import java.io.InputStream
import java.math.BigDecimal
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.SQLException
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.*
import javax.sql.DataSource
import kotlin.reflect.KClass

fun Connection.execute(sql: String) = prepareStatement(sql).execute()
fun DataSource.execute(sql: String, autoclose: Boolean = true): Boolean {
    val c = connection
    try {
        return c.execute(sql)
    } finally {
        if (autoclose) c.close()
    }
}

fun DataSource.select(sql: String, vararg v: Any) = connection.prepareStatement(sql).let {
    it.processParameters(v)
    it.executeQuery()
}

fun PreparedStatement.processParameters(v: Array<out Any>) = v.forEachIndexed { pos, v ->
    when (v) {
        is UUID -> setObject(pos+1, v)
        is Int -> setInt(pos+1, v)
        is Long -> setLong(pos+1, v)
        is Float -> setFloat(pos+1, v)
        is Double -> setDouble(pos+1, v)
        is String -> setString(pos+1, v)
        is BigDecimal -> setBigDecimal(pos+1, v)
        is Boolean -> setBoolean(pos+1, v)
        is LocalTime -> setTime(pos+1, java.sql.Time.valueOf(v))
        is LocalDate -> setDate(pos+1, java.sql.Date.valueOf(v))
        is LocalDateTime -> setTimestamp(pos+1, java.sql.Timestamp.valueOf(v))
        is InputStream -> setBinaryStream(pos+1, v)
        is Enum<*> -> setObject(pos+1, v)
    }
}

class ConnectionFactory {
    companion object {
        internal val transactionContext = ThreadLocal<TransactionContext>()
        internal val isTransactionActive: Boolean get() = transactionContext.get() != null
    }

    internal var factoryFn: (Query<*>) -> Connection = {
        throw SQLException("No default data source is configured. Use Query.db() or configure `KDBC.dataSourceProvider.\n${it.describe()}")
    }

    internal fun borrow(query: Query<*>): Connection {
        val activeTransaction = transactionContext.get()
        if (activeTransaction?.connection != null) return activeTransaction.connection!!
        val connection = factoryFn(query)
        activeTransaction?.trackConnection(connection)
        return connection
    }
}

fun <T : Table> Connection.createTable(tableClass: KClass<T>, skipIfExists: Boolean = false, dropIfExists: Boolean = false) =
        execute(tableClass.java.newInstance().ddl(skipIfExists, dropIfExists))

