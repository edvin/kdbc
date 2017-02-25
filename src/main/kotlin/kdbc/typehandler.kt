package kdbc

import java.sql.PreparedStatement
import kotlin.reflect.KClass

val typeHandlers = mutableMapOf<KClass<*>, TypeHandler<Any?>>()

interface TypeHandler<in T> {
    fun setParam(stmt: PreparedStatement, pos: Int, value: T)
}

