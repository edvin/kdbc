package kdbc

import java.io.InputStream
import java.math.BigDecimal
import java.sql.Connection
import java.sql.Date
import java.sql.SQLException
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*


class QueryBuilder(val sql: String, val connection: Connection) {
    private val params: Map<String, List<Int>> = HashMap()
    val stmt = connection.prepareStatement(sql)

    /**
     * Set value for named parameter, which can be null. JDBC Type must be given explicitly.
     *
     * @see java.sql.Types
     *
     */
    fun param(name: String, value: Any?, type: Int) {
        for (pos in getPos(name)) {
            if (value == null)
                stmt.setNull(pos, type)
            else
                stmt.setObject(pos, value, type)
        }
    }

    /**
     * Set non-null value for named parameter.
     */
    fun param(name: String, value: Any) {
        for (pos in getPos(name)) {
            writeParam(pos, value)
        }
    }

    private fun writeParam(pos: Int, value: Any) {
        when (value) {
            is Int -> stmt.setInt(pos, value)
            is String -> stmt.setString(pos, value)
            is Double -> stmt.setDouble(pos, value)
            is Float -> stmt.setFloat(pos, value)
            is Long -> stmt.setLong(pos, value)
            is LocalDate -> stmt.setDate(pos, Date.valueOf(value))
            is LocalDateTime -> stmt.setTimestamp(pos, Timestamp.valueOf(value))
            is BigDecimal -> stmt.setBigDecimal(pos, value)
            is InputStream -> stmt.setBinaryStream(pos, value)
            else -> throw SQLException("Don't know how to handle parameters of type ${value.javaClass}")
        }
    }

    fun params(vararg params: Any) {
        for (param in params.withIndex()) {
            writeParam(param.index + 1, param.value)
        }
    }

    private fun getPos(name: String) =
            params[name] ?: throw SQLException("$name is not a valid parameter name, choose one of ${params.keys}")
}