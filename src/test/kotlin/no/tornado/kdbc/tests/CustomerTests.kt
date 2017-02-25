package no.tornado.kdbc.tests

import kdbc.Insert
import kdbc.Query
import kdbc.Update
import no.tornado.kdbc.tests.models.Customer
import no.tornado.kdbc.tests.tables.CUSTOMER

class InsertCustomer(customer: Customer) : Insert() {
    val C = CUSTOMER()

    init {
        insert(C) {
            C.name `=` customer.name
        }
        generatedKeys {
            customer.id = getInt(1)
        }
    }
}

class InsertCustomersInBatch(customers: List<Customer>) : Insert() {
    val C = CUSTOMER()

    init {
        // H2 Does not support generated keys in batch, so we can't retrieve them with `generatedKeys { }` here
        batch(customers) { customer ->
            insert(C) {
                C.name `=` customer.name
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

    override fun rowItem() = Customer(c)

    fun byId(id: Int) = firstOrNull {
        where {
            c.id `=` id
        }
    }

}

class UpdateCustomer(customer: Customer) : Update() {
    val C = CUSTOMER()

    init {
        update(C) {
            C.name `=` customer.name
        }
        where {
            C.id `=` customer.id
        }
    }
}

class DeleteCustomer(id: Int) : Query<Customer>() {
    val C = CUSTOMER()

    init {
        delete(C) {
            C.id `=` id
        }
    }
}