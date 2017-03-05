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

abstract class Query<T>(var connection: Connection? = null, var autoclose: Boolean = true, op: (Query<T>.() -> Unit)? = null) : Expr(null) {
    private var withGeneratedKeys: (ResultSet.(Int) -> Unit)? = null
    val tables = mutableListOf<Table>()
    lateinit var stmt: PreparedStatement
    private var vetoclose: Boolean = false
    private var mapper: () -> T = { throw SQLException("You must provide a mapper to this query by calling mapper { () -> T } or override `get(): T`.\n\n${describe()}") }

    init {
        op?.invoke(this)
    }

    /**
     * Convert a result row into the query result object. Extract the data from
     * the Table instances you used to construct the query.
     */
    open fun get(): T = mapper()

    fun map(mapper: () -> T) {
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

    fun first(op: (Query<T>.() -> Unit)? = null): T {
        op?.invoke(this)
        return firstOrNull()!!
    }

    fun firstOrNull(op: (Query<T>.() -> Unit)? = null): T? {
        op?.invoke(this)
        val rs = requireResultSet()
        try {
            return if (rs.next()) {
                tables.forEach { it.rs = rs }
                get()
            } else null
        } finally {
            checkClose()
        }
    }

    fun list(op: (Query<T>.() -> Unit)? = null): List<T> {
        op?.invoke(this)
        val rs = requireResultSet()
        val list = mutableListOf<T>()
        while (rs.next()) {
            tables.forEach { it.rs = rs }
            list.add(get())
        }
        try {
            return list
        } finally {
            checkClose()
        }
    }

    fun sequence(op: (Query<T>.() -> Unit)? = null) = iterator(op).asSequence()

    fun iterator(op: (Query<T>.() -> Unit)? = null): Iterator<T> {
        op?.invoke(this)
        val rs = requireResultSet()
        return object : Iterator<T> {
            override fun next(): T {
                tables.forEach { it.rs = rs }
                return get()
            }

            override fun hasNext(): Boolean {
                val hasNext = rs.next()
                if (!hasNext) {
                    rs.close()
                    checkClose()
                }
                return hasNext
            }
        }
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
    fun execute(op: (Query<T>.() -> Unit)? = null): ExecutionResult<T> {
        op?.invoke(this)
        if (connection == null) {
            connection(KDBC.connectionFactory.borrow(this))
        } else {
            val activeTransaction = ConnectionFactory.transactionContext.get()
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
        val transaction = ConnectionFactory.transactionContext.get()
        if (transaction != null) s.append("\nTX ID    : ${transaction.id}")
        return s.toString()
    }

}

abstract class Insert(connection: Connection? = null, autoclose: Boolean = true) : Query<Any>(connection, autoclose)
abstract class Update(connection: Connection? = null, autoclose: Boolean = true) : Query<Any>(connection, autoclose)
abstract class Delete(connection: Connection? = null, autoclose: Boolean = true) : Query<Any>(connection, autoclose)

fun <Q : Query<*>> Q.connection(connection: Connection): Q {
    this.connection = connection
    return this
}

fun <Q : Query<*>> Q.autoclose(autoclose: Boolean): Q {
    this.autoclose = autoclose
    return this
}

data class ExecutionResult<T>(val query: Query<T>, val hasResultSet: Boolean, val updates: List<Long>) {
    val updatedRows: Long get() = updates.sum()
}

inline fun <DomainType> query(connection: Connection? = null, autoclose: Boolean = true, crossinline op: Query<DomainType>.() -> Unit) = object : Query<DomainType>(connection, autoclose) {
    init {
        op(this)
    }
}