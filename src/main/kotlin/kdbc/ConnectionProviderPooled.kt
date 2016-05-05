
package kdbc

import com.zaxxer.hikari.HikariDataSource
import java.util.concurrent.atomic.AtomicBoolean

class ConnectionProviderPooled(private val pool: HikariDataSource): ConnectionProvider {

    private val openInd = AtomicBoolean(false)

    override fun get() = pool.connection
    override fun close() = if (openInd.getAndSet(false)) pool.close() else Unit

    companion object {
        fun createPool(url: String, username: String? = null, password: String? = null, minPoolSize: Int? = null, maxPoolSize: Int? = null) =
                HikariDataSource().apply {
                    jdbcUrl = url
                    username?.let { setUsername(it) }
                    password?.let { setPassword(it) }
                    minPoolSize?.let { minimumIdle = it }
                    maxPoolSize?.let { maximumPoolSize = it }
                }.let { ConnectionProviderPooled(it) }

        fun createPool(ds: HikariDataSource)  = ConnectionProviderPooled(ds)
    }
}