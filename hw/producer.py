import random
import time
import pulsar
from random import randrange
from datetime import timedelta, datetime
from example import JsonExample
from pulsar.schema import JsonSchema

client = pulsar.Client('pulsar://localhost:6650')

json_schema = JsonSchema(JsonExample)

producer = client.create_producer(topic='clickhouse-topic', schema=json_schema)


def random_date(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)


d1 = datetime.strptime('1/1/2021 1:30 PM', '%m/%d/%Y %I:%M %p')
d2 = datetime.strptime('1/1/2022 4:50 AM', '%m/%d/%Y %I:%M %p')

try:
    while True:
        message = JsonExample(user_id=random.randint(0, 10),
                              event_time=str(random_date(d1, d2)),
                              bytes=(random.uniform(10000.0, 100000.0)))
        producer.send(message)
        print("Отправлено")
        time.sleep(1)
except KeyboardInterrupt:
    print("Stop")

finally:
    producer.close()
    client.close()
