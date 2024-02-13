import pulsar

from schemas import json_customer_schema, json_cart_schema, json_action_schema, json_order_schema, json_product_schema

client = pulsar.Client('pulsar://localhost:6650')

customer_producer = client.create_producer(topic='customer-topic', schema=json_customer_schema)
cart_producer = client.create_producer(topic='cart-topic', schema=json_cart_schema)
action_producer = client.create_producer(topic='action-topic', schema=json_action_schema)
order_producer = client.create_producer(topic='order-topic', schema=json_order_schema)
product_producer = client.create_producer(topic='product', schema=json_product_schema)
