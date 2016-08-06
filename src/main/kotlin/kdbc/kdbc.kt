package kdbc

import java.io.InputStream
import java.math.BigDecimal
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*
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
class SqlContext {
    class Param(val value: Any?, val type: Int? = null)
    val params = mutableListOf<Param>()

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

    val Any.q: String  get() {
        params.add(Param(this))
        return "?"
    }

    operator fun invoke(value: Any?): String {
        println("Adding parameter $value")
        params.add(Param(value))
        return "?"
    }
}

fun Connection.query(sqlOp: SqlContext.() -> String): PreparedStatement {
    val context = SqlContext()
    val sql = sqlOp(context)
    val stmt = prepareStatement(sql)
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
    return stmt
}

fun DataSource.query(sqlOp: SqlContext.() -> String) = connection.query(sqlOp)
fun DataSource.execute(sqlOp: SqlContext.() -> String) = connection.execute(sqlOp)
fun DataSource.update(sqlOp: SqlContext.() -> String) = connection.update(sqlOp)
fun DataSource.delete(sqlOp: SqlContext.() -> String) = connection.delete(sqlOp)
fun DataSource.insert(sqlOp: SqlContext.() -> String) = connection.insert(sqlOp)

fun Connection.execute(sqlOp: SqlContext.() -> String) = query(sqlOp).execute()
fun Connection.update(sqlOp: SqlContext.() -> String) = query(sqlOp).update()
fun Connection.delete(sqlOp: SqlContext.() -> String) = query(sqlOp).delete()
fun Connection.insert(sqlOp: SqlContext.() -> String) = query(sqlOp).insert()
operator fun Connection.invoke(sqlOp: SqlContext.() -> String) = query(sqlOp)

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
operator fun DataSource.invoke(sqlOp: SqlContext.() -> String) = query(sqlOp)