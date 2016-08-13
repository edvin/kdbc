@file:Suppress("UNCHECKED_CAST", "unused", "HasPlatformType")

package kdbc

import kdbc.KDBC.Companion.connectionFactory
import java.io.InputStream
import java.math.BigDecimal
import java.sql.*
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Level
import java.util.logging.Logger
import javax.sql.DataSource
import kotlin.properties.ReadOnlyProperty
import kotlin.reflect.KClass
import kotlin.reflect.KProperty

internal val logger = Logger.getLogger("KDBC")

class JoinDiscriminatorExpr(val discriminator: String, parent: Expr) : Expr(parent) {
    override fun render(s: StringBuilder) {
        s.append("\n$discriminator")
        super.render(s)
    }
}
class JoinOnExpr(parent: Expr) : Expr(parent)

class JoinExpr(val table: Table, parent: Expr) : Expr(parent) {
    init {
        query.addTable(table)
    }

    infix fun ON(op: (JoinOnExpr) -> Unit): JoinExpr {
        add(StringExpr("ON", this))
        add(JoinOnExpr(this), op)
        return this
    }

    override fun render(s: StringBuilder) {
        if (parent !is JoinDiscriminatorExpr) s.append("\n")
        s.append("JOIN ").append(table)
        super.render(s)
    }
}

class SelectExpr(val columns: List<Column<*>>, parent: Expr) : Expr(parent) {
    override fun render(s: StringBuilder) {
        s.append("SELECT ")
        s.append(columns.map { it.asAlias }.joinToString(", "))
        super.render(s)
    }
}

class InsertExpr(val table: Table, parent: Expr) : Expr(parent) {
    override fun render(s: StringBuilder) {
        s.append("INSERT INTO ${table.tableName} (")
        val pairs = expressions.map { it as? ComparisonExpr }.filterNotNull()
        s.append(pairs.map { it.column }.joinToString(", "))
        s.append(") VALUES (")
        s.append(pairs.map { "?" }.joinToString(", "))
        s.append(")")
    }
}

class UpdateExpr(val table: Table, parent: Expr) : Expr(parent) {
    override fun render(s: StringBuilder) {
        s.append("UPDATE ").append(table.tableName).append(" ")
        super.render(s)
    }
}

// TODO: Type safe
class GroupByExpr(sql: String, parent: Expr) : Expr(parent) {
    init {
        add(StringExpr(sql, this))
    }
    override fun render(s: StringBuilder) {
        s.append("\nGROUP BY ")
        super.render(s)
    }
}

class HavingExpr(parent: Expr) : Expr(parent) {
    override fun render(s: StringBuilder) {
        s.append("\nHAVING ")
        super.render(s)
    }
}

class SetExpr(parent: Expr) : Expr(parent) {
    override fun render(s: StringBuilder) {
        s.append("SET ")
        expressions.forEachIndexed { i, expr ->
            prefixWithSpace(s)
            expr.render(s)
            if (i < expressions.size - 1) s.append(",\n")
        }
    }
}

class DeleteExpr(val table: Table, parent: Expr) : Expr(parent) {
    override fun render(s: StringBuilder) {
        s.append("DELETE FROM $table ")
        if (expressions.firstOrNull() !is WhereExpr) s.append("\nWHERE ")
        super.render(s)
    }
}

class FromExpr(val fromTables: List<Table>, parent: Expr) : Expr(parent) {
    init {
        query.apply {
            fromTables.forEach { addTable(it) }
        }
    }

    override fun render(s: StringBuilder) {
        if (parent is Query<*>) s.append("\n")
        s.append("FROM ")
        if (fromTables.isNotEmpty())
            s.append(fromTables.joinToString(", "))
        super.render(s)
    }
}

class WhereExpr(parent: Expr) : Expr(parent) {
    override fun render(s: StringBuilder) {
        if (expressions.isEmpty()) return
        if (parent is Query<*>) s.append("\n")
        s.append("WHERE ")
        super.render(s)
    }
}

class InExpr(val column: Column<*>, parent: Expr) : Expr(parent) {
    override fun render(s: StringBuilder) {
        s.append(column.fullName)
        s.append(" IN (")
        super.render(s)
        s.append(")")
    }
}

class AndExpr(parent: Expr) : Expr(parent) {
    override fun render(s: StringBuilder) {
        if (expressions.isEmpty()) return
        if ((parent is WhereExpr || parent is AndExpr || parent is OrExpr) && parent.expressions.indexOf(this) == 0) {
            // Skip adding AND for the first parent of WHERE/AND/OR
        } else {
            s.append("AND ")
        }

        val wrap = expressions.size > 1
        if (wrap) s.append("(")
        super.render(s)
        if (wrap) s.append(")")
    }
}

class OrExpr(parent: Expr) : Expr(parent) {
    override fun render(s: StringBuilder) {
        if (expressions.isEmpty()) return
        if ((parent is WhereExpr || parent is AndExpr || parent is OrExpr) && parent.expressions.indexOf(this) == 0) {
            // Skip adding OR for the first parent of WHERE/AND/OR
        } else {
            s.append("OR ")
        }
        val wrap = expressions.size > 1
        if (wrap) s.append("(")
        super.render(s)
        if (wrap) s.append(")")
    }

}

class StringExpr(val sql: String, parent: Expr) : Expr(parent = parent) {
    override fun render(s: StringBuilder) {
        s.append(sql)
    }
}

class ComparisonExpr(val column: Any?, val sign: String, val value: Any?, parent: Expr) : Expr(parent = parent) {
    val param = Param(if (value is TextTransform) value.value else value)
    val transform: TextTransform.Type? = if (value is TextTransform) value.type else null

    override fun render(s: StringBuilder) {
        s.append(column).append(" ").append(sign)
        val spacer = if (transform != null) "" else " "
        if (transform != null) s.append(" ").append(transform).append("(")
        if (value is Column<*>) s.append(spacer).append(value.fullName)
        else s.append(spacer).append("?")
        if (transform != null) s.append(")")
    }
}
abstract class Expr(val parent: Expr?) {
    companion object {
        val NoSpaceWhenLastChar = arrayOf(' ', '(', ')', '\n')
    }

    val query: Query<*> get() = if (this is Query<*>) this else if (parent is Query<*>) parent else parent!!.query

    val expressions = mutableListOf<Expr>()

    fun gatherParams(params: MutableList<Param>) {
        if (this is ComparisonExpr) params.add(param)
        expressions.forEach { it.gatherParams(params) }
    }

    internal fun <T : Expr> add(expression: T, op: (T.() -> Unit)? = null): T {
        op?.invoke(expression)
        expressions.add(expression)
        return expression
    }

    fun render(): String {
        val s = StringBuilder()
        render(s)
        return s.toString()
    }

    open fun render(s: StringBuilder) {
        renderChildren(s)
    }

    protected fun renderChildren(s: StringBuilder) {
        expressions.forEach {
            prefixWithSpace(s)
            it.render(s)
        }
    }

    protected fun prefixWithSpace(s: StringBuilder) {
        if (s.isNotEmpty() && !NoSpaceWhenLastChar.contains(s.last()))
            s.append(" ")
    }

    fun append(sql: String) = add(StringExpr(sql, this))
    operator fun String.unaryPlus() = append(this)

    infix fun Expr.JOIN(table: Table): JoinExpr {
        query.addTable(table)
        return add(JoinExpr(table, this))
    }

    val Expr.LEFT: JoinDiscriminatorExpr get() = add(JoinDiscriminatorExpr("LEFT", this))
    val Expr.RIGHT: JoinDiscriminatorExpr get() = add(JoinDiscriminatorExpr("RIGHT", this))
    val Expr.OUTER: JoinDiscriminatorExpr get() = add(JoinDiscriminatorExpr("OUTER", this))
    val Expr.INNER: JoinDiscriminatorExpr get() = add(JoinDiscriminatorExpr("INNER", this))

    infix fun JoinExpr.OUTER(joinExpr: JoinExpr): JoinExpr {
        joinExpr.expressions.add(0, JoinDiscriminatorExpr("OUTER", joinExpr))
        return joinExpr
    }

    infix fun JoinExpr.INNER(joinExpr: JoinExpr): JoinExpr {
        joinExpr.expressions.add(0, JoinDiscriminatorExpr("INNER", joinExpr))
        return joinExpr
    }

    fun GROUPBY(sql: String) = add(GroupByExpr(sql, this))

    fun HAVING(op: HavingExpr.() -> Unit) = add(HavingExpr(this), op)

    fun SELECT(vararg columns: Column<*>, op: (SelectExpr.() -> Unit)? = null) =
            add(SelectExpr(columns.toList(), this), op)

    fun SELECT(columns: Iterable<Column<*>>, op: (SelectExpr.() -> Unit)? = null) =
            add(SelectExpr(columns.toList(), this), op)

    fun <T: Table> UPDATE(table: T, op: SetExpr.() -> Unit): UpdateExpr {
        val updateExpr = add(UpdateExpr(table, this))
        updateExpr.add(SetExpr(updateExpr), op)
        return updateExpr
    }

    fun <T: Table> INSERT(table: T, op: InsertExpr.() -> Unit)
        = add(InsertExpr(table, this), op)

    fun DELETE(table: Table, op: (DeleteExpr.() -> Unit)? = null) =
            add(DeleteExpr(table, this), op)

    fun FROM(vararg tables: Table, op: (FromExpr.() -> Unit)? = null) =
            add(FromExpr(tables.toList(), this), op)

    fun WHERE(op: WhereExpr.() -> Unit) =
            add(WhereExpr(this), op)

    infix fun Column<*>.IN(op: InExpr.() -> Unit) {
        add(InExpr(this, parent!!), op)
    }

    fun AND(op: AndExpr.() -> Unit) = add(AndExpr(this), op)

    fun OR(op: OrExpr.() -> Unit) = add(OrExpr(this), op)

    infix fun Expr.assuming(predicate: Boolean) {
        if (!predicate) parent?.expressions?.remove(this)
    }

    infix fun Expr.assumingNotNull(o: Any?) = assuming(o != null)

    infix fun ComparisonExpr.type(type: Int): ComparisonExpr {
        param.type = type
        return this
    }

    infix fun <T> ComparisonExpr.handler(handler: TypeHandler<T>): ComparisonExpr {
        // Same issues as above
        param.handler = handler as TypeHandler<Any?>
        return this
    }

    infix fun <T : Any> ComparisonExpr.handler(type: KClass<T>): ComparisonExpr {
        // Same issues as above
        param.handler = typeHandlers[type] as TypeHandler<Any?>
        return this
    }

    infix fun Any.TO(param: Any?) = createComparison(this, param, "=")
    infix fun Any.EQ(param: Any?) = createComparison(this, param, "=")
    infix fun Any.LIKE(param: Any?) = createComparison(this, param, "LIKE")
    infix fun Any.GT(param: Any?) = createComparison(this, param, ">")
    infix fun Any.GTE(param: Any?) = createComparison(this, param, ">=")
    infix fun Any.LT(param: Any?) = createComparison(this, param, "<")
    infix fun Any.LTE(param: Any?) = createComparison(this, param, "<=")

    fun UPPER(value: Any?) = TextTransform(TextTransform.Type.UPPER, value)
    fun LOWER(value: Any?) = TextTransform(TextTransform.Type.LOWER, value)

    private fun createComparison(receiver: Any?, param: Any?, sign: String)
            = add(ComparisonExpr(receiver, sign, param, this@Expr))

}


class TextTransform(val type: Type, val value: Any?) {
    enum class Type { UPPER, LOWER }

    override fun toString(): String {
        val s = StringBuilder()
        s.append("$type($value)")
        return s.toString()
    }
}

class Param(val value: Any?) {
    var type: Int? = null
    var handler: TypeHandler<Any?>? = null

    override fun toString() = StringBuilder(value.toString()).let {
        if (type != null) it.append(" (SQL Type $type)")
        it.toString()
    }

}

abstract class Insert : Query<Any>() {
    final override fun map(rs: ResultSet) = throw UnsupportedOperationException()
}

abstract class Update : Query<Any>() {
    final override fun map(rs: ResultSet) = throw UnsupportedOperationException()
}

abstract class Delete : Query<Any>() {
    final override fun map(rs: ResultSet) = throw UnsupportedOperationException()
}

fun <Q : Query<*>> Q.db(db: Wrapper?): Q {
    when (db) {
        is Connection -> {
            connection = db
            close = false
        }
        is DataSource -> {
            connection = db.connection
            close = true
        }
        else -> throw SQLException("db must be either java.sql.Connection or java.sql.DataSource")
    }
    return this
}

fun <Q: Query<*>> Q.close(close: Boolean): Q {
    this.close = close
    return this
}

internal fun logErrors(msg: String, op: () -> Unit) {
    try {
        op()
    } catch (ex: Throwable) {
        logger.log(Level.WARNING, msg, ex)
    }
}

enum class TransactionType { REQUIRED, REQUIRES_NEW }

/**
 * Make sure the surrounded code is executed within a transaction.
 *
 * All connections borrowed from the ConnectionFactory are automatically tracked,
 * as well as connections added to the this function via the `connection` parameter.
 *
 * By default, the TransactionType.REQUIRED attribute indicates that this transaction
 * can participate in an already active transaction or create it's own.
 *
 * Changing to TransactionType.REQUIRES_NEW will temporarily suspend any action transactions,
 * and resume them after this block completes.
 *
 */
fun transaction(vararg connection: Connection, type: TransactionType = TransactionType.REQUIRED, op: () -> Unit) {
    val context = TransactionContext(type)
    for (c in connection) context.trackConnection(c)
    context.execute(op)
}

internal class ConnectionFactory {
    val transactionContext = ThreadLocal<TransactionContext>()

    internal var factoryFn: (Query<*>) -> Connection = {
        throw SQLException("No default data source factoryFn is configured. Use Query.db() or configure `KDBC.dataSourceProvider.\n${it.describe()}")
    }

    internal fun borrow(query: Query<*>): Connection {
        val connection = factoryFn(query)
        transactionContext.get()?.trackConnection(connection)
        return connection
    }
}

class KDBC {
    companion object {
        internal val connectionFactory = ConnectionFactory()

        fun setConnectionFactory(factoryFn: (Query<*>) -> Connection) {
            connectionFactory.factoryFn = factoryFn
        }
    }
}

abstract class Query<out T>() : Expr(null) {
    private var withGeneratedKeys: (ResultSet.(Int) -> Unit)? = null
    val tables = mutableListOf<Table>()
    lateinit var stmt: PreparedStatement
    var connection: Connection? = null
    var close = false

    /**
     * Convert a result row into the query result object. Instead of extracting
     * data from the supplied ResultSet you should extract the data from
     * the Table instances you used to construct the query.
     */
    abstract fun map(rs: ResultSet): T

    fun generatedKeys(op: ResultSet.(Int) -> Unit) {
        withGeneratedKeys = op
    }

    /**
     * Add table and configure alias if more than one table. Also configure alias for the first table if you add a second
     */
    fun addTable(table: Table) {
        if (!tables.contains(table)) {
            tables.add(table)
            if (tables.size > 1) table.configureAlias()
            if (tables.size == 2) tables.first().configureAlias()
        }
    }

    private fun Table.configureAlias() {
        if (tableAlias == null) {
            if (tables.find { it.tableAlias == tableName } == null)
                tableAlias = tableName
            else
                tableAlias = "${tableName}_${tables.indexOf(this) + 1}"
        }
    }

    override fun render(s: StringBuilder) = renderChildren(s)

    fun first() = firstOrNull()!!
    fun firstOrNull(): T? {
        val rs = requireResultSet()
        try {
            return if (rs.next()) {
                tables.forEach { it.rs = rs }
                map(rs)
            } else null
        } finally {
            checkClose()
        }
    }

    fun list(): List<T> {
        val rs = requireResultSet()
        val list = mutableListOf<T>()
        while (rs.next()) {
            tables.forEach { it.rs = rs }
            list.add(map(rs))
        }
        try {
            return list
        } finally {
            checkClose()
        }
    }

    private fun requireResultSet(): ResultSet {
        val configuredToClose = close
        close = false
        try {
            val hasResultSet = execute()
            if (!hasResultSet) {
                checkClose()
                throw SQLException("List was requested but query returned no ResultSet.\n${describe()}")
            }
        } finally {
            close = configuredToClose
        }
        return stmt.resultSet
    }

    private fun checkClose() {
        if (close) logErrors("Closing connection") { connection!!.close() }
    }

    val resultSet: ResultSet get() = stmt.resultSet!!

    operator fun invoke() = execute()

    /**
     * Gather parameters, render the SQL, prepare the statement and execute the query.
     */
    fun execute(): Boolean {
        if (connection == null) db(connectionFactory.borrow(this))
        var hasResultSet: Boolean? = null
        try {
            val sql = render()
            val keyStrategy = if (withGeneratedKeys != null) PreparedStatement.RETURN_GENERATED_KEYS
            else PreparedStatement.NO_GENERATED_KEYS
            stmt = connection!!.prepareStatement(sql, keyStrategy)
            applyParameters()
            hasResultSet = stmt.execute()
            handleGeneratedKeys()
            return hasResultSet
        } finally {
            if (close && hasResultSet == false) connection!!.close()
        }
    }

    private fun handleGeneratedKeys() {
        withGeneratedKeys?.apply {
            val keysRs = stmt.generatedKeys
            val counter = AtomicInteger()
            while (keysRs.next())
                this.invoke(keysRs, counter.andIncrement)
        }
    }

    val params: List<Param> get() {
        val list = mutableListOf<Param>()
        gatherParams(list)
        return list
    }

    fun applyParameters() {
        val paramCounter = AtomicInteger(0)
        params.forEach { param ->
            if (param.value !is Column<*>) {
                // TODO: Clean up type handler code when the TypeHandler interface has stabilized
                val handler: TypeHandler<Any?>? = param.handler ?: if (param.value != null) typeHandlers[param.value.javaClass.kotlin] else null
                applyParameter(handler, param, paramCounter.incrementAndGet(), param.value)
            }
        }
    }

    private fun applyParameter(handler: TypeHandler<Any?>?, param: Param, pos: Int, value: Any?) {
        if (handler != null) {
            handler.setParam(stmt, pos, value)
        } else if (param.type != null) {
            if (param.value == null)
                stmt.setNull(pos, param.type!!)
            else
                stmt.setObject(pos, value, param.type!!)
        } else {
            when (value) {
                is UUID -> stmt.setObject(pos, value)
                is Int -> stmt.setInt(pos, value)
                is String -> stmt.setString(pos, value)
                is Double -> stmt.setDouble(pos, value)
                is Boolean -> stmt.setBoolean(pos, value)
                is Float -> stmt.setFloat(pos, value)
                is Long -> stmt.setLong(pos, value)
                is LocalTime -> stmt.setTime(pos, java.sql.Time.valueOf(value))
                is LocalDate -> stmt.setDate(pos, java.sql.Date.valueOf(value))
                is LocalDateTime -> stmt.setTimestamp(pos, java.sql.Timestamp.valueOf(value))
                is BigDecimal -> stmt.setBigDecimal(pos, value)
                is InputStream -> stmt.setBinaryStream(pos, value)
                is Enum<*> -> stmt.setObject(pos, value)
                null -> throw SQLException("Parameter #$pos is null, you must provide a handler or sql type.\n${describe()}")
                else -> throw SQLException("Don't know how to handle parameters of type ${value.javaClass}.\n${describe()}")
            }
        }
    }

    fun describe(): String {
        val s = StringBuilder()
        s.append("Query    : ${this}\n")
        s.append("SQL      : ${render()}\n")
        s.append("Params   : $params\n")
        return s.toString()
    }

}

interface TypeHandler<in T> {
    fun setParam(stmt: PreparedStatement, pos: Int, value: T)
}

val typeHandlers = mutableMapOf<KClass<*>, TypeHandler<Any?>>()

fun Connection.execute(sql: String) = prepareStatement(sql).execute()
fun DataSource.execute(sql: String, autoclose: Boolean = true): Boolean {
    val c = connection
    try {
        return c.execute(sql)
    } finally {
        if (autoclose) c.close()
    }
}

fun ResultSet.getUUID(label: String) = UUID.fromString(getString(label))
fun ResultSet.getLocalTime(label: String) = getTime(label).toLocalTime()
fun ResultSet.getLocalDateTime(label: String) = getTimestamp(label).toLocalDateTime()
fun ResultSet.getLocalDate(label: String) = getDate(label).toLocalDate()

internal class TransactionContext(val type: TransactionType) {
    private val connections = mutableListOf<Connection>()
    private val childContexts = mutableListOf<TransactionContext>()

    fun trackConnection(connection: Connection) {
        connection.autoCommit = false
        connections.add(connection)
    }

    fun trackChildContext(context: TransactionContext) {
        childContexts.add(context)
    }

    fun rollback() {
        connections.forEach { silentlyRollback(it) }
        childContexts.forEach { it.rollback() }
        cleanup()
    }

    fun commit() {
        connections.forEach { silentlyCommit(it) }
        childContexts.forEach { it.commit() }
        cleanup()
    }

    private fun cleanup() {
        connections.clear()
        childContexts.clear()
    }

    private fun silentlyCommit(connection: Connection) {
        logErrors("Committing connection $connection") {
            connection.commit()
            connection.close() // TODO: Detect if we should close or not
        }
    }

    private fun silentlyRollback(connection: Connection) {
        logErrors("Rolling back connection $connection") {
            connection.rollback()
            connection.close() // TODO: Detect if we should close or not
        }
    }

    fun execute(op: () -> Unit) {
        val activeContext = connectionFactory.transactionContext.get()

        if (type == TransactionType.REQUIRED) {
            if (activeContext != null) trackChildContext(this)
            else connectionFactory.transactionContext.set(this)
        } else if (type == TransactionType.REQUIRES_NEW) {
            connectionFactory.transactionContext.set(this)
        }

        var failed = false
        try {
            op()
        } catch (e: Exception) {
            failed = true
            throw e
        } finally {
            if (failed) {
                if (type == TransactionType.REQUIRED) {
                    if (activeContext != null) {
                        activeContext.rollback()
                    } else {
                        rollback()
                    }
                } else if (type == TransactionType.REQUIRES_NEW) {
                    rollback()
                }
            } else {
                if (type == TransactionType.REQUIRED) {
                    if (activeContext != null) {
                        activeContext.commit()
                    } else {
                        commit()
                    }
                } else if (type == TransactionType.REQUIRES_NEW) {
                    commit()
                }
            }

            connectionFactory.transactionContext.set(activeContext)
        }

    }
}

class Column<out T>(val table: Table, val name: String, val getter: ResultSet.(String) -> T?, var rs: () -> ResultSet) {
    override fun toString() = fullName
    val fullName: String get() = if (table.tableAlias != null) "${table.tableAlias}.$name" else name
    val alias: String get() = fullName.replace(".", "_")
    val asAlias: String get() = if (table.tableAlias != null) "$fullName $alias" else alias
    val value: T? get() = getter(rs(), alias)
    val v: T get() = getter(rs(), alias)!!
    operator fun invoke(): T = v
}

class ColumnDelegate<T>(val getter: ResultSet.(String) -> T) : ReadOnlyProperty<Table, Column<T>> {
    var instance: Column<T>? = null
    override fun getValue(thisRef: Table, property: KProperty<*>): Column<T> {
        if (instance == null) instance = Column(thisRef, property.name, getter, {
            thisRef.rs ?: throw SQLException("ResultSet was not configured when column value was requested")
        })
        return instance!!
    }
}

abstract class Table(val tableName: String) {
    var tableAlias: String? = null
    var rs: ResultSet? = null
    fun <T> column(getter: ResultSet.(String) -> T) = ColumnDelegate(getter)
    override fun toString() = if (tableAlias.isNullOrBlank() || tableAlias == tableName) tableName else "$tableName $tableAlias"
    val columns: List<Column<*>> get() = javaClass.declaredMethods
            .filter { Column::class.java.isAssignableFrom(it.returnType) }
            .map {
                it.isAccessible = true
                it.invoke(this) as Column<*>
            }

}

infix fun <T : Table> T.AS(alias: String): T {
    tableAlias = alias
    return this
}