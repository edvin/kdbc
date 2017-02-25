package no.tornado.kdbc.tests

import kdbc.Insert
import kdbc.Query
import kdbc.Update
import no.tornado.kdbc.tests.models.Customer
import no.tornado.kdbc.tests.tables.CUSTOMER

class InsertCustomer(customer: Customer) : Insert() {
    val C = CUSTOMER()

    init {
        INSERT(C) {
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
        BATCH(customers) { customer ->
            INSERT(C) {
                C.name `=` customer.name
            }
        }
    }
}

class SelectCustomer : Query<Customer>() {
    val C = CUSTOMER()

    init {
        SELECT(C)
        FROM(C)
    }

    override fun rowItem() = Customer(C)

    fun byId(id: Int) = firstOrNull {
        WHERE {
            C.id `=` id
        }
    }

}

class UpdateCustomer(customer: Customer) : Update() {
    val C = CUSTOMER()

    init {
        UPDATE(C) {
            C.name `=` customer.name
        }
        WHERE {
            C.id `=` customer.id
        }
    }
}

class DeleteCustomer(id: Int) : Query<Customer>() {
    val C = CUSTOMER()

    init {
        DELETE(C) {
            C.id `=` id
        }
    }
}