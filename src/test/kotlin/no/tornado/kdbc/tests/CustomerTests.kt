package no.tornado.kdbc.tests

import kdbc.Insert
import kdbc.Query
import kdbc.Update
import no.tornado.kdbc.tests.models.Customer
import no.tornado.kdbc.tests.tables.CUSTOMER

class InsertCustomer(customer: Customer) : Insert() {
    val c = CUSTOMER()

    init {
        insert(c) {
            c.name `=` customer.name
        }
        generatedKeys {
            customer.id = getInt(1)
        }
    }
}

class InsertCustomersInBatch(customers: List<Customer>) : Insert() {
    val c = CUSTOMER()

    init {
        // H2 Does not support generated keys in batch, so we can't retrieve them with `generatedKeys { }` here
        batch(customers) { customer ->
            insert(c) {
                c.name `=` customer.name
            }
        }
    }
}

class SelectCustomer : Query<Customer>() {
    val c = CUSTOMER()

    init {
        select(c)
        from(c)
    }

    override fun get() = Customer(c)

    fun byId(id: Int) = firstOrNull {
        where {
            c.id `=` id
        }
    }

}

class UpdateCustomer(customer: Customer) : Update() {
    val c = CUSTOMER()

    init {
        update(c) {
            c.name `=` customer.name
        }
        where {
            c.id `=` customer.id
        }
    }
}

class DeleteCustomer(id: Int) : Query<Customer>() {
    val c = CUSTOMER()

    init {
        delete(c) {
            c.id `=` id
        }
    }
}