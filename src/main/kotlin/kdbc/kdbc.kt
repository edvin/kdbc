package kdbc

import java.io.InputStream
import java.math.BigDecimal
import java.sql.*
import java.sql.Date
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*

/**
 * Create a ParameterizedStatement using the given query. Example:
 *
 *     connection.query("SELECT * FROM customer WHERE id = :id") {
 *        param("id", 42)
 *     }
 *
 */
fun Connection.select(sql: String, op: ParameterizedStatement.() -> Unit = {}): ParameterizedStatement {
    val params = HashMap<String, ArrayList<Int>>()

    var query = StringBuffer()

    val matcher = Regex(":\\w+").toPattern().matcher(sql)

    var paramPos = 1

    while (matcher.find()) {
        val key = matcher.group().substring(1)

        if (params.containsKey(key))
            throw SQLException("Repeated parameter $key")

        var paramsForKey = params.computeIfAbsent(key, { ArrayList() })
        paramsForKey.add(paramPos++)
        matcher.appendReplacement(query, "?")
    }

    matcher.appendTail(query)

    return ParameterizedStatement(query.toString(), params, this).apply(op)
}

fun Connection.execute(sql: String, op: ParameterizedStatement.() -> Unit = {})
        = select(sql, op).execute()

fun Connection.update(sql: String, op: ParameterizedStatement.() -> Unit = {})
        = select(sql, op).update()

fun Connection.delete(sql: String, op: ParameterizedStatement.() -> Unit = {})
        = select(sql, op).delete()

fun Connection.insert(sql: String, op: ParameterizedStatement.() -> Unit = {})
        = select(sql, op).insert()

fun PreparedStatement.delete(): Int = executeUpdate()
fun PreparedStatement.update(): Int = executeUpdate()
fun PreparedStatement.insert(): Int = executeUpdate()

/**
 * Execute the query and transform each result set entry via the supplied function.
 */
infix fun <T> PreparedStatement.list(op: ResultSet.() -> T): List<T> {
    val list = ArrayList<T>()
    val rs = executeQuery()
    while (rs.next())
        list.add(op(rs))
    return list
}

/**
 * Execute the query and transform the first result set entry via the supplied function.
 * If you entries are found, return null
 */
infix fun <T> PreparedStatement.first(op: ResultSet.() -> T): T? {
    val rs = executeQuery()
    return if (rs.next()) op(rs) else null
}

/**
 * Execute the query and transform the first, required result set entry via the supplied function.
 *
 * @throws SQLException If there are no result data
 */
infix fun <T> PreparedStatement.single(op: ResultSet.() -> T): T =
        first(op) ?: throw SQLException("No result")

/**
 * A PreparedStatement wrapper with the ability to reference parameters by name instead of position.
 */
class ParameterizedStatement(sql: String, private val params: Map<String, List<Int>>, connection: Connection)
: PreparedStatement by connection.prepareStatement(sql) {

    /**
     * Set value for named parameter, which can be null. JDBC Type must be given explicitly.
     *
     * @see java.sql.Types
     *
     */
    fun param(name: String, value: Any?, type: Int) {
        for (pos in getPos(name)) {
            if (value == null)
                setNull(pos, type)
            else
                setObject(pos, value, type)
        }
    }

    /**
     * Set non-null value for named parameter.
     */
    fun param(name: String, value: Any): ParameterizedStatement {
        for (pos in getPos(name)) {
            writeParam(pos, value)
        }
        return this
    }

    private fun writeParam(pos: Int, value: Any): ParameterizedStatement {
        when (value) {
            is Int -> setInt(pos, value)
            is String -> setString(pos, value)
            is Double -> setDouble(pos, value)
            is Float -> setFloat(pos, value)
            is Long -> setLong(pos, value)
            is LocalDate -> setDate(pos, Date.valueOf(value))
            is LocalDateTime -> setTimestamp(pos, Timestamp.valueOf(value))
            is BigDecimal -> setBigDecimal(pos, value)
            is InputStream -> setBinaryStream(pos, value)
            else -> throw SQLException("Don't know how to handle parameters of type ${value.javaClass}")
        }
        return this
    }

    fun params(vararg params: Any): ParameterizedStatement {
        for (param in params.withIndex()) {
            writeParam(param.index + 1, param.value)
        }
        return this
     }

    fun getPos(name: String) =
            params[name] ?: throw SQLException("$name is not a valid parameter name, choose one of ${params.keys}")

}

public inline fun <T : AutoCloseable, R> T.use(block: (T) -> R): R {
    var closed = false
    try {
        return block(this)
    } catch (e: Exception) {
        closed = true
        try {
            this.close()
        } catch (closeException: Exception) {
        }
        throw e
    } finally {
        if (!closed) {
            this.close()
        }
    }
}

fun PreparedStatement.asSequence(): Sequence<ResultSet> {
    return object: Iterator<ResultSet> {
        val rs = executeQuery()
        var hasMore = true

        override fun next() = rs

        override fun hasNext():Boolean {
            hasMore = rs.next()
            if (!hasMore)
                rs.close()
            return hasMore
        }

    }.asSequence()
}