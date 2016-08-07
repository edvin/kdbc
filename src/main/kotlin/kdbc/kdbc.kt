@file:Suppress("UNCHECKED_CAST")

package kdbc

import java.io.InputStream
import java.math.BigDecimal
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import javax.sql.DataSource

/**
 * Create a ParameterizedStatement using the given query. Example:
 *     val id = 42
 *
 *     connection.query {
 *      "SELECT * FROM customer WHERE id = ${p(id)}"
 *     }
 *
 */
class QueryContext(val autoclose: Boolean) {
    class Param(val value: Any?, val type: Int? = null)

    val sql = StringBuilder()

    val params = mutableListOf<Param>()
    var withGeneratedKeys: (ResultSet.(Int) -> Unit)? = null

    infix fun generatedKeys(op: ResultSet.(Int) -> Unit) {
        withGeneratedKeys = op
    }

    fun SELECT(string: String) : String {
        sql.append("SELECT " + string)
        return ""
    }

    fun FROM(string: String) : String {
        sql.append(" FROM " + string)
        return ""
    }

    fun UPDATE(op: QueryContext.() -> String): String {
        sql.append("UPDATE " + op())
        return ""
    }

    fun INSERT(op: QueryContext.() -> String): String {
        sql.append("INSERT " + op())
        return ""
    }

    fun DELETE(op: QueryContext.() -> String): String {
        sql.append("DELETE " + op())
        return ""
    }

    fun WHERE(op: QueryBlock.() -> Unit): String {
        val s = QueryBlock()
        op(s)
        if (!s.isEmpty()) s.insert(0, " WHERE ")
        return s.toString()
    }

    /**
     * Set value for named parameter, which can be null. JDBC Type must be given explicitly.
     *
     * @see java.sql.Types
     *
     */
    fun p(value: Any?, type: Int): String {
        params.add(Param(value, type))
        return "?"
    }

    /**
     * Set non-null value for named parameter.
     */
    infix fun p(value: Any?): String {
        params.add(Param(value))
        return "?"
    }

    operator fun Any?.not(): String {
        params.add(Param(this))
        return "?"
    }

    operator fun Any?.invoke(): String {
        params.add(Param(this))
        return "?"
    }

    val Any?.p: String get() {
        params.add(Param(this))
        return "?"
    }
}

class QueryResult(val context: QueryContext, val stmt: PreparedStatement) {

    infix fun <T> execute(op: PreparedStatement.(Boolean) -> T): T {
        val isResultSet = stmt.execute()
        val value = op(stmt, isResultSet)
        handleGeneratedKeys()
        if (context.autoclose) stmt.connection.close()
        return value
    }

    private fun handleGeneratedKeys() {
        if (context.withGeneratedKeys != null) {
            val keysRs = stmt.generatedKeys
            val counter = AtomicInteger()
            while (keysRs.next())
                context.withGeneratedKeys?.invoke(keysRs, counter.andIncrement)
        }
    }

    infix fun <T> single(op: ResultSet.() -> T): T = first(op) ?: throw SQLException("No result")

    infix fun <T> first(op: ResultSet.() -> T): T? = execute {
        val rs = resultSet
        if (rs.next()) op(rs) else null
    }

    infix fun <T> list(op: ResultSet.() -> T): List<T> = execute {
        val list = mutableListOf<T>()
        val rs = resultSet
        while (rs.next()) list.add(op(rs))
        list
    }

    infix fun <T> sequence(op: ResultSet.() -> T): Sequence<T>
            = ResultSetIterator(context.autoclose, stmt.executeQuery(), op).asSequence()
}

fun Connection.query(autoclose: Boolean = false, sqlOp: QueryContext.() -> String): QueryResult {
    val context = QueryContext(autoclose)
    val last = sqlOp(context)
    val sql = context.sql.toString() + last

    val keyStrategy = if (context.withGeneratedKeys != null) PreparedStatement.RETURN_GENERATED_KEYS
    else PreparedStatement.NO_GENERATED_KEYS

    val stmt = prepareStatement(sql, keyStrategy)

    context.params.forEachIndexed { index, param ->
        val pos = index + 1
        if (param.type != null) {
            if (param.value == null)
                stmt.setNull(pos, param.type)
            else
                stmt.setObject(pos, param.value, param.type)
        } else {
            when (param.value) {
                is UUID -> stmt.setObject(pos, param.value)
                is Int -> stmt.setInt(pos, param.value)
                is String -> stmt.setString(pos, param.value)
                is Double -> stmt.setDouble(pos, param.value)
                is Boolean -> stmt.setBoolean(pos, param.value)
                is Float -> stmt.setFloat(pos, param.value)
                is Long -> stmt.setLong(pos, param.value)
                is LocalTime -> stmt.setTime(pos, java.sql.Time.valueOf(param.value))
                is LocalDate -> stmt.setDate(pos, java.sql.Date.valueOf(param.value))
                is LocalDateTime -> stmt.setTimestamp(pos, java.sql.Timestamp.valueOf(param.value))
                is BigDecimal -> stmt.setBigDecimal(pos, param.value)
                is InputStream -> stmt.setBinaryStream(pos, param.value)
                is Enum<*> -> stmt.setObject(pos, param.value)
                else -> throw SQLException("Don't know how to handle parameters of type ${param.value?.javaClass}")
            }
        }
    }
    return QueryResult(context, stmt)
}

fun Connection.execute(autoclose: Boolean = false, sqlOp: QueryContext.() -> String): Boolean = query(autoclose, sqlOp).execute { it }

fun DataSource.query(sqlOp: QueryContext.() -> String): QueryResult = connection.query(true, sqlOp)
fun DataSource.execute(sqlOp: QueryContext.() -> String): Boolean = connection.execute(true, sqlOp)

class ResultSetIterator<out T>(val autoclose: Boolean, val rs: ResultSet, val op: ResultSet.() -> T) : Iterator<T> {
    override fun hasNext(): Boolean {
        val isLast = rs.isLast
        if (isLast && autoclose)
            rs.statement.connection.close()
        return !isLast
    }

    override fun next(): T {
        rs.next()
        return op(rs)
    }
}

fun <R> Connection.use(transactional: Boolean = false, block: Connection.() -> R): R {
    val wasAutoCommit = autoCommit
    if (transactional && wasAutoCommit) autoCommit = false
    var failed = false
    try {
        return block(this)
    } catch (e: Exception) {
        failed = true
        throw e
    } finally {
        if (transactional) {
            if (failed) rollback() else commit()
        }
        this.close()
    }
}

fun <T> DataSource.use(transactional: Boolean = false, block: Connection.() -> T) = connection.use(transactional, block)
fun <T> DataSource.transaction(block: Connection.() -> T) = connection.use(true, block)
fun <T> Connection.transaction(block: Connection.() -> T) = use(true, block)

val ResultSet.asInt: Int get() = getInt(1)
val ResultSet.asString: String get() = getString(1)
val ResultSet.asLong: Long get() = getLong(1)
val ResultSet.asDouble: Double get() = getDouble(1)
val ResultSet.asFloat: Float get() = getFloat(1)
val ResultSet.asLocalTime: LocalTime get() = getTime(1).toLocalTime()
val ResultSet.asLocalDate: LocalDate get() = getDate(1).toLocalDate()
val ResultSet.asLocalDateTime: LocalDateTime get() = getTimestamp(1).toLocalDateTime()
fun ResultSet.getUUID(label: String) = UUID.fromString(getString(label))

class QueryBlock {
    private val content = StringBuilder()
    private var firstAndOrSkipped = false

    fun append(value: String) = content.append(value)
    fun isNotEmpty() = content.isNotEmpty()
    fun insert(pos: Int, value: String) = content.insert(pos, value)
    fun isEmpty() = content.isEmpty()
    override fun toString() = content.toString()

    fun QueryBlock.AND(op: QueryBlock.() -> Unit) {
        val s = QueryBlock()
        op(s)
        if (s.isNotEmpty()) {
            addWordIfNotFirst("AND")
            append("(${s.toString()})")
        }
    }

    private fun addWordIfNotFirst(word: String) {
        if (firstAndOrSkipped) {
            append(" $word ")
        } else {
            firstAndOrSkipped = true
        }
    }

    fun AND(s: String) {
        addWordIfNotFirst("AND")
        append(s)
    }

    fun OR(s: String) {
        addWordIfNotFirst("OR")
        append(s)
    }

}

inline fun <T, R> Iterable<T>.OR(transform: (T) -> R): String {
    return map(transform).joinToString { " OR " }
}

inline fun <T, R> Iterable<T>.AND(transform: (T) -> R): String {
    return map(transform).joinToString { " AND " }
}