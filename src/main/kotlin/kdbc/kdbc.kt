package kdbc

import java.sql.Connection
import java.util.logging.Level
import java.util.logging.Logger
import javax.sql.DataSource

class KDBC {
    companion object {
        val connectionFactory = ConnectionFactory()
        var debug: Boolean = false

        fun setConnectionFactory(factoryFn: (Query<*>) -> Connection) {
            connectionFactory.factoryFn = factoryFn
        }

        fun setDataSource(dataSource: DataSource) {
            setConnectionFactory { dataSource.connection }
        }

    }
}

internal val logger = Logger.getLogger("KDBC")

internal fun logErrors(msg: String, op: () -> Unit) {
    try {
        op()
    } catch (ex: Throwable) {
        logger.log(Level.WARNING, msg, ex)
    }
}