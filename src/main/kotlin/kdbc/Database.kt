package kdbc

import java.sql.DriverManager

class Database(val connectionProvider: ConnectionProvider) {
    fun select(sql: String): SelectBuilder = SelectBuilder(sql, connectionProvider.get())

    companion object {
        fun from(url: String) = object: ConnectionProvider {
            private val conn = DriverManager.getConnection(url)
            override fun get() = conn
            override fun close() = conn.close()
        }.let { Database(it) }
    }
}