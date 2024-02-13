from pulsar.schema import Record, String, Integer, Float, Boolean


class ProductData(Record):
    product_id = Integer()
    name = String()
    price = Integer()
    rating = Integer()
    views_count = Integer()
    type = String()
    time_created = String()