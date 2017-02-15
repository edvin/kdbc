@file:Suppress("UNCHECKED_CAST", "unused", "HasPlatformType")

package kdbc

import kdbc.ConnectionFactory.Companion.transactionContext
import kdbc.KDBC.Companion.connectionFactory
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

class BatchExpr<T>(val entities: Iterable<T>, val large: Boolean, val op: (BatchExpr<T>).(T) -> Unit, parent: Expr) : Expr(parent)

class SelectExpr(val columns: Iterable<Column<*>>, parent: Expr) : Expr(parent) {
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

    val query: Query<*> get() = if (this is Query<*>) this else parent as? Query<*> ?: parent!!.query

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

    fun <T> BATCH(entities: Iterable<T>, large: Boolean = false, op: (BatchExpr<T>).(T) -> Unit) =
            add(BatchExpr(entities, large, op, this))

    fun SELECT(vararg columns: ColumnOrTable, op: (SelectExpr.() -> Unit)? = null) =
            add(SelectExpr(columns.flatMap { (it as? Table)?.columns ?: listOf(it as Column<*>) }, this), op)

    fun SELECT(columns: Iterable<ColumnOrTable>, op: (SelectExpr.() -> Unit)? = null) =
            add(SelectExpr(columns.flatMap { (it as? Table)?.columns ?: listOf(it as Column<*>) }, this), op)

    fun <T : Table> UPDATE(table: T, op: (SetExpr.() -> Unit)? = null): UpdateExpr {
        val updateExpr = add(UpdateExpr(table, this))
        updateExpr.add(SetExpr(updateExpr), op)
        return updateExpr
    }

    fun SET(op: SetExpr.() -> Unit) = add(SetExpr(this), op)

    fun <T : Table> INSERT(table: T, op: InsertExpr.() -> Unit)
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

    infix fun Any.`=`(param: Any?) = createComparison(this, param, "=")
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

abstract class Insert(connection: Connection? = null, autoclose: Boolean = true) : Query<Any>(connection, autoclose) {
    final override fun TO(rs: ResultSet) = throw UnsupportedOperationException()
}

abstract class Update(connection: Connection? = null, autoclose: Boolean = true) : Query<Any>(connection, autoclose) {
    final override fun TO(rs: ResultSet) = throw UnsupportedOperationException()
}

abstract class Delete(connection: Connection? = null, autoclose: Boolean = true) : Query<Any>(connection, autoclose) {
    final override fun TO(rs: ResultSet) = throw UnsupportedOperationException()
}

fun <Q : Query<*>> Q.connection(connection: Connection): Q {
    this.connection = connection
    return this
}

fun <Q : Query<*>> Q.autoclose(autoclose: Boolean): Q {
    this.autoclose = autoclose
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
 * All queries will use the same connection by default. To create a new connection that will
 * participate in the transaction, nest another `transaction` block inside this.
 *
 * By default, the TransactionType.REQUIRED attribute indicates that this transaction
 * can participate in an already active transaction or create it's own.
 *
 * Changing to TransactionType.REQUIRES_NEW will temporarily suspend any action transactions,
 * and resume them after this block completes.
 *
 * If no connection is specified, the connection retrieved for the first query executed inside the transaction block will be used.
 *
 */
fun transaction(connection: Connection? = null, type: TransactionType = TransactionType.REQUIRED, op: () -> Unit) {
    val context = TransactionContext(type)
    if (connection != null) context.trackConnection(connection)
    context.execute(op)
}

class ConnectionFactory {
    companion object {
        internal val transactionContext = ThreadLocal<TransactionContext>()
        internal val isTransactionActive: Boolean get() = transactionContext.get() != null
    }

    internal var factoryFn: (Query<*>) -> Connection = {
        throw SQLException("No default data source is configured. Use Query.db() or configure `KDBC.dataSourceProvider.\n${it.describe()}")
    }

    internal fun borrow(query: Query<*>): Connection {
        val activeTransaction = transactionContext.get()
        if (activeTransaction?.connection != null) return activeTransaction.connection!!
        val connection = factoryFn(query)
        activeTransaction?.trackConnection(connection)
        return connection
    }
}

class KDBC {
    companion object {
        val connectionFactory = ConnectionFactory()

        fun setConnectionFactory(factoryFn: (Query<*>) -> Connection) {
            connectionFactory.factoryFn = factoryFn
        }

        fun setDataSource(dataSource: DataSource) {
            setConnectionFactory { dataSource.connection }
        }

    }
}

abstract class Query<T>(var connection: Connection? = null, var autoclose: Boolean = true, op: (Query<T>.() -> Unit)? = null) : Expr(null) {
    private var withGeneratedKeys: (ResultSet.(Int) -> Unit)? = null
    val tables = mutableListOf<Table>()
    lateinit var stmt: PreparedStatement
    private var vetoclose: Boolean = false
    private var mapper: (ResultSet) -> T = { throw SQLException("You must provide a mapper to this query by calling TO { resultSet -> T } or override `TO(ResultSet): T`.\n\n${describe()}") }

    init {
        op?.invoke(this)
    }

    /**
     * Convert a result row into the query result object. Instead of extracting
     * data from the supplied ResultSet you should extract the data from
     * the Table instances you used to construct the query.
     */
    open fun TO(rs: ResultSet): T = mapper(rs)

    fun TO(mapper: (ResultSet) -> T) {
        this.mapper = mapper
    }

    fun generatedKeys(op: ResultSet.(Int) -> Unit) {
        withGeneratedKeys = op
    }

    /**
     * Add table and configure alias if more than one table. Also configure alias for the first table if you add a second
     */
    internal fun addTable(table: Table) {
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
                tableAlias = "$tableName${tables.indexOf(this) + 1}"
        }
    }

    override fun render(s: StringBuilder) = renderChildren(s)

    fun first() = firstOrNull()!!
    fun firstOrNull(): T? {
        val rs = requireResultSet()
        try {
            return if (rs.next()) {
                tables.forEach { it.rs = rs }
                TO(rs)
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
            list.add(TO(rs))
        }
        try {
            return list
        } finally {
            checkClose()
        }
    }

    fun sequence(): Sequence<T> {
        val rs = requireResultSet()
        return object : Iterator<T> {
            override fun next(): T {
                tables.forEach { it.rs = rs }
                return TO(rs)
            }

            override fun hasNext(): Boolean {
                val hasNext = rs.next()
                if (!hasNext) {
                    rs.close()
                    checkClose()
                }
                return hasNext
            }
        }.asSequence()
    }

    private fun requireResultSet(): ResultSet {
        vetoclose = true
        try {
            val result = execute()
            if (!result.hasResultSet) {
                checkClose()
                throw SQLException("List was requested but query returned no ResultSet.\n${describe()}")
            }
        } finally {
            vetoclose = false
        }
        return stmt.resultSet
    }

    /**
     * Should we close this connection? Unless spesifically stopped via vetoclose
     * a connection will close if autoclose is set or if this query does not
     * participate in a transaction.
     */
    private fun checkClose() {
        if (!vetoclose && !ConnectionFactory.isTransactionActive && autoclose) logErrors("Closing connection") { connection!!.close() }
    }

    val resultSet: ResultSet get() = stmt.resultSet!!

    operator fun invoke() = execute()

    /**
     * Gather parameters, render the SQL, prepare the statement and execute the query.
     */
    fun execute(): ExecutionResult<T> {
        if (connection == null) {
            connection(connectionFactory.borrow(this))
        } else {
            val activeTransaction = transactionContext.get()
            if (activeTransaction != null && activeTransaction.connection == null)
                activeTransaction.connection = connection
        }
        var hasResultSet: Boolean? = null
        try {
            val keyStrategy = if (withGeneratedKeys != null) PreparedStatement.RETURN_GENERATED_KEYS
            else PreparedStatement.NO_GENERATED_KEYS

            val batch = expressions.first() as? BatchExpr<Any>
            if (batch != null) {
                val wasAutoCommit = connection!!.autoCommit
                connection!!.autoCommit = false

                val iterator = batch.entities.iterator()
                if (!iterator.hasNext()) throw SQLException("Batch expression with no entities.\n${describe()}")
                var first = true

                while (iterator.hasNext()) {
                    batch.op(batch, iterator.next())
                    if (first) {
                        stmt = connection!!.prepareStatement(render(), keyStrategy)
                        first = false
                    }
                    applyParameters()
                    handleGeneratedKeys()
                    stmt.addBatch()
                    batch.expressions.clear()
                }

                val updates = if (batch.large) stmt.executeLargeBatch().toList() else stmt.executeBatch().map { it.toLong() }
                connection!!.commit()
                if (wasAutoCommit) connection!!.autoCommit = true
                return ExecutionResult(this, false, updates)
            } else {
                stmt = connection!!.prepareStatement(render(), keyStrategy)
                applyParameters()
                hasResultSet = stmt.execute()
                handleGeneratedKeys()
                return ExecutionResult(this, hasResultSet, listOf(stmt.updateCount.toLong()))
            }
        } catch (ex: Exception) {
            throw SQLException("${ex.message}\n\n${describe()}", ex)
        } finally {
            if (hasResultSet == false) checkClose()
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
        params.filterNot { it.value is Column<*> }.forEachIndexed { pos, param ->
            applyParameter(param, pos + 1)
        }
    }

    private fun applyParameter(param: Param, pos: Int) {
        val handler: TypeHandler<Any?>? = param.handler ?: if (param.value != null) typeHandlers[param.value.javaClass.kotlin] else null

        if (handler != null) {
            handler.setParam(stmt, pos, param.value)
        } else if (param.type != null) {
            if (param.value == null)
                stmt.setNull(pos, param.type!!)
            else
                stmt.setObject(pos, param.value, param.type!!)
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
                null -> throw SQLException("Parameter #$pos is null, you must provide a handler or sql type.\n${describe()}")
                else -> throw SQLException("Don't know how to handle parameters of type ${param.value.javaClass}.\n${describe()}")
            }
        }
    }

    fun describe(): String {
        val s = StringBuilder()
        s.append("Query    : ${this}\n")
        s.append("SQL      : ${render().replace("\n", "           \n")}\n")
        s.append("Params   : $params")
        val transaction = transactionContext.get()
        if (transaction != null) s.append("\nTX ID    : ${transaction.id}")
        return s.toString()
    }

}

data class ExecutionResult<T>(val query: Query<T>, val hasResultSet: Boolean, val updates: List<Long>) {
    val updatedRows: Long get() = updates.sum()
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
    val id = UUID.randomUUID()
    private val childContexts = mutableListOf<TransactionContext>()
    internal var connection: Connection? = null

    fun trackConnection(connection: Connection): TransactionContext {
        if (connection.autoCommit) connection.autoCommit = false
        this.connection = connection
        return this
    }

    fun trackChildContext(context: TransactionContext) {
        childContexts.add(context)
    }

    fun rollback() {
        connection?.silentlyRollback()
        childContexts.forEach { it.rollback() }
        cleanup()
    }

    fun commit() {
        connection?.silentlyCommit()
        childContexts.forEach { it.commit() }
        cleanup()
    }

    private fun cleanup() {
        connection = null
        childContexts.clear()
    }

    private fun Connection.silentlyCommit() {
        logErrors("Committing connection $this") {
            commit()
            close()
        }
    }

    private fun Connection.silentlyRollback() {
        logErrors("Rolling back connection $this") {
            rollback()
            close()
        }
    }

    fun execute(op: () -> Unit) {
        val activeContext = transactionContext.get()

        if (type == TransactionType.REQUIRED) {
            if (activeContext != null) activeContext.trackChildContext(this)
            else transactionContext.set(this)
        } else if (type == TransactionType.REQUIRES_NEW) {
            transactionContext.set(this)
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
                    if (activeContext == null)
                        commit()
                } else if (type == TransactionType.REQUIRES_NEW) {
                    commit()
                }
            }

            transactionContext.set(activeContext)
        }
    }
}

interface ColumnOrTable

class Column<out T>(val table: Table, val name: String, val ddl: String?, val getter: ResultSet.(String) -> T?, var rs: () -> ResultSet) : ColumnOrTable {
    override fun toString() = fullName
    val fullName: String get() = if (table.tableAlias != null) "${table.tableAlias}.$name" else name
    val alias: String get() = fullName.replace(".", "_")
    val asAlias: String get() = if (table.tableAlias != null) "$fullName $alias" else alias
    val value: T? get() = getter(rs(), alias)
    val v: T get() = getter(rs(), alias)!!
    val isNull: Boolean get() = value == null
    val isNotNull: Boolean get() = !isNull
    operator fun invoke(): T = v
}

class ColumnDelegate<T>(val ddl: String? = null, val getter: ResultSet.(String) -> T) : ReadOnlyProperty<Table, Column<T>> {
    var instance: Column<T>? = null
    override fun getValue(thisRef: Table, property: KProperty<*>): Column<T> {
        if (instance == null) instance = Column(thisRef, property.name, ddl, getter, {
            thisRef.rs ?: throw SQLException("ResultSet was not configured when column value was requested")
        })
        return instance!!
    }
}

class CustomerTable : Table("customer") {
    val id by column(INTEGER_NOT_NULL)
}


abstract class Table(val tableName: String) : ColumnOrTable {
    val INTEGER: ResultSet.(String) -> Int? = { getInt(it) }
    val INTEGER_NOT_NULL: ResultSet.(String) -> Int = { getInt(it) }

    val TEXT: ResultSet.(String) -> String? = { getString(it) }
    val TEXT_NOT_NULL: ResultSet.(String) -> String = { getString(it) }

    var tableAlias: String? = null
    var rs: ResultSet? = null
    protected fun <T> column(getter: ResultSet.(String) -> T) = ColumnDelegate(null, getter)
    protected fun <T> column(ddl: String, getter: ResultSet.(String) -> T) = ColumnDelegate(ddl, getter)
    override fun toString() = if (tableAlias.isNullOrBlank() || tableAlias == tableName) tableName else "$tableName $tableAlias"
    val columns: List<Column<*>> get() = javaClass.declaredMethods
            .filter { Column::class.java.isAssignableFrom(it.returnType) }
            .map {
                it.isAccessible = true
                it.invoke(this) as Column<*>
            }

    // Generate DDL for this table. All columns that have a DDL statement will be included.
    fun ddl(dropIfExists: Boolean): String {
        val s = StringBuilder()
        if (dropIfExists) s.append("DROP TABLE IF EXISTS $tableName;\n")
        s.append("CREATE TABLE $tableName (\n")
        val ddls = columns.filter { it.ddl != null }.iterator()
        while (ddls.hasNext()) {
            val c = ddls.next()
            s.append("\t").append(c.name).append(" ").append(c.ddl)
            if (ddls.hasNext()) s.append(",\n")
        }
        s.append(")")
        return s.toString()
    }

    // Execute create table statement. Creates a query to be able to borrow a connection from the data source factory.
    fun create(dropIfExists: Boolean = false) {
        object : Query<Void>() {
            init {
                add(StringExpr(ddl(dropIfExists), this))
                execute()
            }
        }
    }
}

fun <T : Table> Connection.createTable(tableClass: KClass<T>, dropIfExists: Boolean = false) =
        execute(tableClass.java.newInstance().ddl(dropIfExists))

infix fun <T : Table> T.AS(alias: String): T {
    tableAlias = alias
    return this
}