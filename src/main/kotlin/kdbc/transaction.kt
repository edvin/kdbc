package kdbc

import java.sql.Connection
import java.util.*

enum class TransactionType { REQUIRED, REQUIRES_NEW }

internal class TransactionContext(val type: TransactionType) {
    val id: UUID = UUID.randomUUID()
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
        val activeContext = ConnectionFactory.transactionContext.get()

        if (type == TransactionType.REQUIRED) {
            if (activeContext != null) activeContext.trackChildContext(this)
            else ConnectionFactory.transactionContext.set(this)
        } else if (type == TransactionType.REQUIRES_NEW) {
            ConnectionFactory.transactionContext.set(this)
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

            ConnectionFactory.transactionContext.set(activeContext)
        }
    }
}

/**
 * Make sure the surrounded code is executed within a transaction.
 *
 * All queries will use the same connection by default. To create a new connection that will
 * participate in the transaction, nest another `transaction` block inside this.
 *
 * By default, the TransactionType.REQUIRED attribute indicates that this transaction
 * can participate in an already active transaction or create it's own.
 *
 * Changing to TransactionType.REQUIRES_NEW will temporarily suspend any active transactions,
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