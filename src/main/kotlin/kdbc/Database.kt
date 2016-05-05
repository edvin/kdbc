package kdbc

import java.io.InputStream
import java.math.BigDecimal
import java.sql.*
import java.time.LocalDate
import java.time.LocalDateTime

class Database private constructor(val cp: ConnectionProvider) {

    fun select(sql: String, params: Array<*> = arrayOf<Any>()) =
        cp.get().prepareStatement(sql).apply {
            params.forEachIndexed { index, value ->
                val pos = index + 1
                when (value) {
                    is Int -> setInt(pos, value)
                    is String -> setString(pos, value)
                    is Double -> setDouble(pos, value)
                    is Float -> setFloat(pos, value)
                    is Long -> setLong(pos, value)
                    is LocalDate -> setDate(pos, java.sql.Date.valueOf(value))
                    is LocalDateTime -> setTimestamp(pos, java.sql.Timestamp.valueOf(value))
                    is BigDecimal -> setBigDecimal(pos, value)
                    is InputStream -> setBinaryStream(pos, value)
                    value == null -> setObject(pos,null)
                    else -> throw SQLException("Don't know how to handle parameters of type ${value?.javaClass}")
                }
            }
        }.asSequence()
}


fun PreparedStatement.asSequence(): Sequence<ResultSet> {
    return object: Iterator<ResultSet> {
        val rs = executeQuery()
        var hasMore = true
        init {
            hasMore = rs.next()
        }
        override fun next() = rs

        override fun hasNext():Boolean {
            hasMore = rs.next()
            if (!hasMore)
                rs.close()
            return hasMore
        }

    }.asSequence()
}