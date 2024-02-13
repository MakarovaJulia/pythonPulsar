from pulsar.schema import Record, String, Integer, Float, Boolean


class CustomerData(Record):
    customer_id = Integer()
    age = Integer()
    country = String()
    email = String()
    first_name = String()
    last_name = String()
    password = String()
    subscribed_to_news = Boolean()
    time_created = String()