from pulsar.schema import Record, String, Integer, Float, Boolean


class OrderData(Record):
    order_id = Integer()
    customer_id = Integer()
    order_date = String()
    cart_id = Integer()
    promo_used = Boolean()
    status = String()