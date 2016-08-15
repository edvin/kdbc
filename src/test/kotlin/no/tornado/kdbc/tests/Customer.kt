package no.tornado.kdbc.tests

data class Customer(var id: Int? = null, var name: String) {
    constructor(t: CustomerTable) : this(t.id(), t.name())
}

class CustomerTable : kdbc.Table("customer") {
    val id by column { getInt(it) }
    val name by column { getString(it) }
}

class InsertCustomer(customer: no.tornado.kdbc.tests.Customer) : kdbc.Insert() {
    val c = CustomerTable()

    init {
        INSERT(c) {
            c.name TO customer.name
        }
        generatedKeys {
            customer.id = getInt(1)
        }
    }
}

class InsertCustomersInBatch(customers: List<no.tornado.kdbc.tests.Customer>) : kdbc.Insert() {
    val c = no.tornado.kdbc.tests.CustomerTable()

    init {
        // H2 Does not support generated keys in batch, so we can't retrieve them with `generatedKeys { }` here
        BATCH(customers) { customer ->
            INSERT(c) {
                c.name TO customer.name
            }
        }
    }
}

class SelectCustomer() : kdbc.Query<Customer>() {
    val c = no.tornado.kdbc.tests.CustomerTable()

    init {
        SELECT(c.columns)
        FROM(c)
    }

    fun byId(id: Int): no.tornado.kdbc.tests.Customer? = let {
        WHERE { c.id EQ id }
        firstOrNull()
    }

    fun search(name: String): List<no.tornado.kdbc.tests.Customer> = let {
        WHERE { UPPER(c.name) LIKE UPPER("%$name%") }
        list()
    }

    override fun map(rs: java.sql.ResultSet) = no.tornado.kdbc.tests.Customer(c)
}

class UpdateCustomer(customer: no.tornado.kdbc.tests.Customer) : kdbc.Update() {
    val c = no.tornado.kdbc.tests.CustomerTable()

    init {
        UPDATE(c) {
            c.name TO customer.name
        }
        WHERE {
            c.id EQ customer.id
        }
    }
}

class DeleteCustomer(id: Int) : kdbc.Delete() {
    val c = no.tornado.kdbc.tests.CustomerTable()

    init {
        DELETE(c) { c.id EQ id }
    }
}