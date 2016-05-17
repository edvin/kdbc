package kdbc

import java.sql.Connection

interface ConnectionProvider {

    fun get(): Connection
    fun close(): Unit
}

