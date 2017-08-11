package no.tornado.kdbc.tests

import kdbc.Table
import org.junit.Test
import java.math.BigDecimal
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*

class TableTests
{
    @Test
    fun defaultGetterTest() {
        with (object : Table() {}) {
            DefaultGetter<Int>()
            DefaultGetter<Long>()
            DefaultGetter<Float>()
            DefaultGetter<Double>()
            DefaultGetter<String>()
            DefaultGetter<BigDecimal>()
            DefaultGetter<Date>()
            DefaultGetter<LocalDate>()
            DefaultGetter<LocalDateTime>()
        }
    }
}