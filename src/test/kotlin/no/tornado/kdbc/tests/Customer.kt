package no.tornado.kdbc.tests

import kdbc.*

data class Customer(var id: Int? = null, var name: String) {
    constructor(t: CustomerTable) : this(t.id(), t.name())
}

class CustomerTable : Table("customer") {
    val id by column { getInt(it) }
    val name by column { getString(it) }
}

class InsertCustomer(customer: Customer) : Insert() {
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

class InsertCustomersInBatch(customers: List<Customer>) : Insert() {
    val c = CustomerTable()

    init {
        // H2 Does not support generated keys in batch, so we can't retrieve them with `generatedKeys { }` here
        BATCH(customers) { customer ->
            INSERT(c) {
                c.name TO customer.name
            }
        }
    }
}

class SelectCustomer() : Query<Customer>() {
    val c = CustomerTable()

    init {
        SELECT(c.columns)
        FROM(c)
    }

    fun byId(id: Int): Customer? = let {
        WHERE { c.id EQ id }
        firstOrNull()
    }

    fun search(name: String): List<Customer> = let {
        WHERE { UPPER(c.name) LIKE UPPER("%$name%") }
        list()
    }

    override fun map(rs: java.sql.ResultSet) = Customer(c)
}

class UpdateCustomer(customer: Customer) : Update() {
    val c = CustomerTable()

    init {
        UPDATE(c) {
            c.name TO customer.name
        }
        WHERE {
            c.id EQ customer.id
        }
    }
}

class DeleteCustomer(id: Int) : Delete() {
    val c = CustomerTable()

    init {
        DELETE(c) { c.id EQ id }
    }
}