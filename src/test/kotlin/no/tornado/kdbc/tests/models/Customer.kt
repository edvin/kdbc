package no.tornado.kdbc.tests.models

import no.tornado.kdbc.tests.tables.CUSTOMER
import java.util.*

data class Customer(var id: Int? = null, var name: String, var uuid: UUID = UUID.randomUUID()) {
    constructor(t: CUSTOMER) : this(t.id(), t.name()!!, t.uuid()!!)
}


