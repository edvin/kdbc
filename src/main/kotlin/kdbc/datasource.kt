package kdbc

import java.sql.Connection
import java.sql.SQLException
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

fun <T : Table> Connection.createTable(tableClass: KClass<T>, dropIfExists: Boolean = false) =
        execute(tableClass.java.newInstance().ddl(dropIfExists))

