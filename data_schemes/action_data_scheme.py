from pulsar.schema import Record, String, Integer, Float, Boolean


class ActionData(Record):
    action_id = Integer()
    customer_id = Integer()
    action_type = String()
    action_time = String()
