@file:Suppress("UNCHECKED_CAST")

package kdbc

import java.io.InputStream
import java.math.BigDecimal
import java.sql.*
import java.time.LocalDate
import java.time.LocalDateTime
import javax.sql.DataSource
import kotlin.reflect.KMutableProperty1

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

    val params = mutableListOf<Param>()
    var withGeneratedKeys: (ResultSet.() -> Unit)? = null
    val intos = mutableMapOf<String, KMutableProperty1<Any, Any>>()

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
    fun p(value: Any): String {
        params.add(Param(value))
        return "?"
    }

    val Any.q: String get() {
        params.add(Param(this))
        return "?"
    }

    fun <S, T>into(prop: KMutableProperty1<S, T>): String {
        val key = "${prop.name}_${intos.size}".toUpperCase()
        intos.put(key, prop as KMutableProperty1<Any, Any>)
        return "as " + key
    }

}

class QueryResult(val context: QueryContext, val stmt: PreparedStatement) {
    infix fun <T> execute(op: PreparedStatement.(Boolean) -> T): T {
        val isResultSet = stmt.execute()
        val value = op(stmt, isResultSet)
        context.withGeneratedKeys?.invoke(stmt.generatedKeys)
        if (context.autoclose) stmt.connection.close()
        return value
    }

    infix fun <T> update(op: PreparedStatement.(Int) -> T): T {
        val updated = stmt.executeUpdate()
        context.withGeneratedKeys?.invoke(stmt.generatedKeys)
        val value = op(stmt, updated)
        if (context.autoclose) stmt.connection.close()
        return value
    }

    infix fun <T> intoSingle(op: () -> T) = single {
        mapIntos(op(), TypeCache(metaData), this, context.intos as MutableMap<String, KMutableProperty1<T, Any>>) as T
    }

    private fun <T> mapIntos(receiver: T, typeCache: TypeCache, resultSet: ResultSet, intos: MutableMap<String, KMutableProperty1<T, Any>>): Any {
        intos.forEach {
            val alias = it.key
            val prop = it.value

            when (typeCache.cache[alias]) {
                Types.INTEGER -> prop.set(receiver, resultSet.getInt(it.key))
                Types.VARCHAR -> prop.set(receiver, resultSet.getString(it.key))
                Types.CLOB -> prop.set(receiver, resultSet.getString(it.key))
            }
        }

        return receiver!!
    }

    class TypeCache(meta: ResultSetMetaData) {
        val cache = mutableMapOf<String, Int>()

        init {
            (1..meta.columnCount).forEach {
                val label: String = meta.getColumnLabel(it).toUpperCase()
                val type: Int = meta.getColumnType(it)
                cache[label] = type
            }
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
    val sql = sqlOp(context)

    val keyStrategy = if(context.withGeneratedKeys == null) PreparedStatement.NO_GENERATED_KEYS
    else PreparedStatement.RETURN_GENERATED_KEYS

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
                is Int -> stmt.setInt(pos, param.value)
                is String -> stmt.setString(pos, param.value)
                is Double -> stmt.setDouble(pos, param.value)
                is Float -> stmt.setFloat(pos, param.value)
                is Long -> stmt.setLong(pos, param.value)
                is LocalDate -> stmt.setDate(pos, java.sql.Date.valueOf(param.value))
                is LocalDateTime -> stmt.setTimestamp(pos, java.sql.Timestamp.valueOf(param.value))
                is BigDecimal -> stmt.setBigDecimal(pos, param.value)
                is InputStream -> stmt.setBinaryStream(pos, param.value)
                else -> throw SQLException("Don't know how to handle parameters of type ${param.value?.javaClass}")
            }
        }
    }
    return QueryResult(context, stmt)
}

fun Connection.execute(autoclose: Boolean = false, sqlOp: QueryContext.() -> String): Boolean = query(autoclose, sqlOp).execute { it }
fun Connection.update(autoclose: Boolean = false, sqlOp: QueryContext.() -> String): Int = query(autoclose, sqlOp).update { it }

fun DataSource.query(sqlOp: QueryContext.() -> String): QueryResult = connection.query(true, sqlOp)
fun DataSource.execute(sqlOp: QueryContext.() -> String): Boolean = connection.execute(true, sqlOp)
fun DataSource.update(sqlOp: QueryContext.() -> String): Int = connection.update(true, sqlOp)

class ResultSetIterator<out T>(val autoclose: Boolean, val rs: ResultSet, val op: ResultSet.() -> T) : Iterator<T> {
    override fun hasNext() : Boolean {
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

fun <T : AutoCloseable, R> T.use(block: T.() -> R): R {
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

fun <T> DataSource.use(block: Connection.() -> T) = connection.use(block)