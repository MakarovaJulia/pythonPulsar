import pulsar
import pg8000
import json

import clickhouse_connect

# conn_client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='')

client = pulsar.Client('pulsar://localhost:6650')

consumer = client.subscribe('test-clickhouse', 'my-test-subscription')

conn = pg8000.connect(
    database='analytics',
    user='default',
    password='',
    host='pulsar://localhost',
    port=8123
)

cursor = conn.cursor()
# client.command('CREATE TABLE pulsar_clickhouse_jdbc_sink (key UInt32, value String, metric Float64) ENGINE
# MergeTree ORDER BY key')

# result = client.query('SELECT max(key), avg(metric) FROM pulsar_clickhouse_jdbc_sink')

# print(result.result_rows)
try:
    while True:
        msg = consumer.receive()
        try:
            print("Received message '{}' id='{}'".format(msg.value(), msg.message_id()))
            consumer.acknowledge(msg)

            insert_data_query = '''INSERT INTO hourly_data (page_id, event_time, count_views) VALUES (%s, %s)'''

            data_json = json.loads(msg.data())
            data = (data_json['page_id'],
                    data_json['event_time'],
                    data_json['count_views'])
            cursor.execute(insert_data_query, data)
            conn.commit()

        except Exception:
            consumer.negative_acknowledge()

finally:
    client.close()
    consumer.close()
    cursor.close()
    conn.close()
