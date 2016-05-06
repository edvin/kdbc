package kdbc

import java.io.InputStream
import java.math.BigDecimal
import java.sql.*
import java.sql.Date
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*

interface IQueryBuilder {
    val sql : String
    val connection: Connection
    val stmt: PreparedStatement
    fun param(name: String, value: Any?, type: Int)
    fun param(name: String, value: Any)
    fun params(vararg params: Any)
}

class QueryBuilder(override val sql: String, override val connection: Connection): IQueryBuilder {
    private val params: Map<String, List<Int>> = HashMap()
    override val stmt = connection.prepareStatement(sql)

    /**
     * Set value for named parameter, which can be null. JDBC Type must be given explicitly.
     *
     * @see java.sql.Types
     *
     */
    override fun param(name: String, value: Any?, type: Int) {
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
    override fun param(name: String, value: Any) {
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

    override fun params(vararg params: Any) {
        for (param in params.withIndex()) {
            writeParam(param.index + 1, param.value)
        }
    }

    private fun getPos(name: String) =
            params[name] ?: throw SQLException("$name is not a valid parameter name, choose one of ${params.keys}")
}