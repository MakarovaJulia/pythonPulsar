from pulsar.schema import Record, String, Integer, Float, Boolean


class CartData(Record):
    cart_id = Integer()
    customer_id = Integer()
    product_id = Integer()
    status = Boolean()
    time_created = String()
