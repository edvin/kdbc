package no.tornado.kdbc.tests.models

import no.tornado.kdbc.tests.tables.CUSTOMER

data class Customer(var id: Int? = null, var name: String) {
    constructor(t: CUSTOMER) : this(t.id(), t.name())
}


