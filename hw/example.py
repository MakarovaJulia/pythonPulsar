from pulsar.schema import Record, String, Integer, Float


class JsonExample(Record):
    user_id = Integer()
    event_time = String()
    bytes = Float()

